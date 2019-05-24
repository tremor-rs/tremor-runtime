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

-export([ast_eval/1]).

-spec ast_eval({}) -> integer() | float() | boolean() | binary().
ast_eval({<<"Add">>, A, B}) when is_binary(A) andalso is_binary(B) -> <<A/binary, B/binary>>;
ast_eval({<<"Add">>, A, B}) -> A + B;
ast_eval({<<"Sub">>, A, B}) -> A - B;
ast_eval({<<"Div">>, A, B}) -> A / B;
ast_eval({<<"Mul">>, A, B}) -> A * B;
ast_eval({<<"Mod">>, A, B}) -> util:mod(A,B);

ast_eval({<<"Eq">>, A, B}) -> A =:= B;
ast_eval({<<"NotEq">>, A, B}) -> A =/= B;
ast_eval({<<"Gte">>, A, B}) -> A >= B;
ast_eval({<<"Gt">>, A, B}) -> A > B;
ast_eval({<<"Lt">>, A, B}) -> A < B;
ast_eval({<<"Lte">>, A, B}) -> B >= A;
ast_eval({<<"And">>, true, true}) -> true;
ast_eval({<<"Or">>, true, true}) -> true;
ast_eval({<<"Or">>, false, true}) -> true;
ast_eval({<<"Or">>, true, false}) -> true;
ast_eval({<<"Not">>, A}) when is_boolean(A) -> not A;
ast_eval({<<"Plus">>, A}) when is_integer(A) orelse is_float(A) -> A;
ast_eval({<<"Minus">>, A}) when is_integer(A) orelse is_float(A) -> -A;
ast_eval(_) -> false.

