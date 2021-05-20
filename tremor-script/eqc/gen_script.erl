%% Copyright 2020, The Tremor Team
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%

-module(gen_script).

-include_lib("pbt.hrl").

-export([gen/1]).

gen_({'+', A, B}) ->
    ["(", gen_(A), " + ", gen_(B), ")"];
gen_({'-', A, B}) ->
    ["(", gen_(A), " - ", gen_(B), ")"];
gen_({'/', A, B}) ->
    ["(", gen_(A), " / ", gen_(B), ")"];
gen_({'*', A, B}) ->
    ["(", gen_(A), " * ", gen_(B), ")"];
gen_({'%', A, B}) ->
    ["(", gen_(A), " % ", gen_(B), ")"];
gen_({'band', A, B}) ->
    ["(", gen_(A), " & ", gen_(B), ")"];
gen_({'bxor', A, B}) ->
    ["(", gen_(A), " ^ ", gen_(B), ")"];
gen_({'==', A, B}) ->
    ["(", gen_(A), " == ", gen_(B), ")"];
gen_({'!=', A, B}) ->
    ["(", gen_(A), " != ", gen_(B), ")"];
gen_({'>=', A, B}) ->
    ["(", gen_(A), " >= ", gen_(B), ")"];
gen_({'>', A, B}) ->
    ["(", gen_(A), " > ", gen_(B), ")"];
gen_({'<', A, B}) ->
    ["(", gen_(A), " < ", gen_(B), ")"];
gen_({'<=', A, B}) ->
    ["(", gen_(A), " <= ", gen_(B), ")"];
gen_({'and', A, B}) ->
    ["(", gen_(A), " and ", gen_(B), ")"];
gen_({'or', A, B}) ->
    ["(", gen_(A), " or ", gen_(B), ")"];
gen_({'not', A}) -> ["not (", gen_(A), ")"];
gen_({'+', A}) -> ["(+ ", gen_(A), ")"];
gen_({'-', A}) -> ["(- ", gen_(A), ")"];
gen_({'#', String1, String2, Sub}) ->
    ["(", string:trim(gen_(String1), trailing, "\""), "#{",
     gen(Sub), "}",
     string:trim(gen_(String2), leading, "\""), ")"];
gen_({array, Exprs}) ->
    ["[", [[gen_(Ele), ","] || Ele <- Exprs], "]"];
gen_({record, Exprs}) ->
    ["{",
     [[gen_(Key), ":", gen_(Value), ","]
      || {Key, Value} <- maps:to_list(Exprs)],
     "}"];
gen_({'merge', Expr1, Expr2}) ->
    ["(", "merge ", gen_(Expr1), " of ", gen_(Expr2), " end", ")"];
gen_({'let', Path, Expr}) ->
    ["let ", gen_(Path), " = ", gen_(Expr)];
gen_({local, Path}) -> Path;
gen_({emit, A}) -> ["emit (", gen_(A), ")"];
gen_(drop) -> "drop";
gen_(true) -> "true";
gen_(false) -> "false";
gen_(null) -> "null";
gen_(X) when is_number(X) -> io_lib:format("~p", [X]);
gen_(X) when is_binary(X) -> jsx:encode(X).

gen(Expr) -> iolist_to_binary(gen_(Expr)).
