%% Copyright 2018-2019, Wayfair GmbH
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

-module(model).

-include_lib("pbt.hrl").

-export([ast_eval/2]).


resolve(S,path) ->
    "a".

-spec ast_eval(#state{}, {}) -> {#state{}, integer() | float() | boolean() | binary()}.
ast_eval(#state{}=S, {<<"Add">>, A, B}) when is_binary(A) andalso is_binary(B) -> {S, <<A/binary, B/binary>>};
ast_eval(#state{}=S, {<<"Add">>, A, B}) -> {S, A + B};
ast_eval(#state{}=S, {<<"Sub">>, A, B}) -> {S, A - B};
ast_eval(#state{}=S, {<<"Div">>, A, B}) -> {S, A / B};
ast_eval(#state{}=S, {<<"Mul">>, A, B}) -> {S, A * B};
ast_eval(#state{}=S, {<<"Mod">>, A, B}) -> {S, util:mod(A,B)};

ast_eval(#state{}=S, {<<"Eq">>, A, B}) -> {S, A =:= B};
ast_eval(#state{}=S, {<<"NotEq">>, A, B}) -> {S, A =/= B};
ast_eval(#state{}=S, {<<"Gte">>, A, B}) -> {S, A >= B};
ast_eval(#state{}=S, {<<"Gt">>, A, B}) -> {S, A > B};
ast_eval(#state{}=S, {<<"Lt">>, A, B}) -> {S, A < B};
ast_eval(#state{}=S, {<<"Lte">>, A, B}) -> {S, B >= A};
ast_eval(#state{}=S, {<<"And">>, true, true}) -> {S, true};
ast_eval(#state{}=S, {<<"Or">>, true, true}) -> {S, true};
ast_eval(#state{}=S, {<<"Or">>, false, true}) -> {S, true};
ast_eval(#state{}=S, {<<"Or">>, true, false}) -> {S, true};
ast_eval(#state{}=S, {<<"Not">>, A}) when is_boolean(A) -> {S, not A};
ast_eval(#state{}=S, {<<"Plus">>, A}) when is_integer(A) orelse is_float(A) -> {S,A};
ast_eval(#state{}=S, {<<"Minus">>, A}) when is_integer(A) orelse is_float(A) -> {S,-A};
ast_eval(#state{locals=L}=S, {<<"Let">>, Path, Expr}) -> {S1, R} = ast_eval(S,Expr), LL = maps:put(resolve(S1, path), R, L), {S1#state{locals=LL}, R};
ast_eval(#state{}=S, {emit, A}) -> {S1, R} = ast_eval(S, A), {S1,{emit, R}};
ast_eval(#state{}=S, {drop, A}) -> {S1, R} = ast_eval(S, A), {S1,{drop, R}};

ast_eval(#state{}=S,_) -> {S, false}.

