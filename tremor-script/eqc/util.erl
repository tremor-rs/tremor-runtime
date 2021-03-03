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

-module(util).

-export([clamp/2, mod/2, round/2]).

mod(X, Y) when X > 0 -> X rem Y;
mod(X, Y) when X < 0 -> Y + X rem Y;
mod(0, _) -> 0.

round(Number, Precision) ->
    P = math:pow(10, Precision), round(Number * P) / P.

clamp(Number, Precision) when is_float(Number) ->
    list_to_float(float_to_list(round(Number,
				      Precision + 1),
				[{decimals, 10}]));
clamp({K, V}, Precision) -> {K, clamp(V, Precision)};
clamp(#{<<"emit">> := V}, Precision) ->
    #{<<"emit">> => clamp(V, Precision)};
clamp(#{<<"drop">> := V}, Precision) ->
    #{<<"drop">> => clamp(V, Precision)};
clamp(L, Precision) when is_list(L) ->
    [clamp(E, Precision) || E <- L];
clamp(Number, _) -> Number.
