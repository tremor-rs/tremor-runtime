%% Copyright 2018-2020, Wayfair GmbH
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

-module(spec).

-include_lib("pbt.hrl").

-export([gen/1, gen_int/1, gen_float/1, gen_string/1, gen_bool/1, valid/1, id/0]).


valid(Script) ->
    try
        lists:foldr(fun(AAst, {S, _}) ->
                            model:eval(S, AAst)
                    end, {#vars{}, null}, Script),
        true
    catch
        _:_ ->
            false
    end.


id() ->
    ?LET(Path, ?SUCHTHAT(Id, ?LET(Id, list(choose($a, $e)), list_to_binary(Id)), byte_size(Id) > 0), Path).

float() ->
    real().

gen(#state{} = S) ->
    resize(3, ?SIZED(N, gen(S, N))).

gen(#state{} = S, N) ->
    frequency([{10, {emit, spec_inner(S, N)}}, {1, drop}, {80, spec_inner(S, N)}]).

gen_int(#state{} = S) ->
    resize(3, ?SIZED(N, spec_inner_int(S, N))).

gen_float(#state{} = S) ->
    resize(3, ?SIZED(N, spec_inner_float(S, N))).

gen_string(#state{} = S) ->
    resize(3, ?SIZED(N, spec_inner_string(S, N))).

gen_bool(#state{} = S) ->
    resize(3, ?SIZED(N, spec_inner_bool(S, N))).

spec_inner(#state{}=S, N) ->
    frequency([
               {10, spec_inner_float(S, N)},
               {10, spec_inner_int(S, N)},
               {10, spec_inner_string(S, N)},
               {10, spec_inner_bool(S, N)}
          ]).

spec_inner_int(#state{}=S, N) ->
    ?LAZY(frequency([
                     {10, spec_bop_int(S, N)},
                     {5, spec_uop_int(S, N)}
                    ])).

spec_inner_float(#state{}=S, N) ->
    ?LAZY(frequency([
               {10, spec_bop_float(S, N)},
               {5, spec_uop_float(S, N)}
              ])).

spec_inner_string(#state{}=S, N) ->
    ?LAZY(spec_bop_string(S, N)).

spec_inner_bool(#state{}=S, N) ->
    ?LAZY(frequency([
               {10, spec_bop_bool(S, N)},
               {1, spec_uop_bool(S, N)}
              ])).

string() ->
    utf8(10).

small_int() ->
    choose(1,100).

spec_uop_float(S, N) when N =< 1 ->
    {oneof(['+','-']), float_or_float_local(S)};

spec_uop_float(S, N) ->
    {oneof(['+','-']), spec_inner_float(S, N - 1)}.

int_or_int_local(#state{locals = Ls}) ->
    IVs = [{1, {local, K}} || {K, int} <- maps:to_list(Ls)],
    frequency([{max(length(IVs), 1), small_int()} | IVs]).

float_or_float_local(#state{locals = Ls}) ->
    IVs = [{1, {local, K}} || {K, float} <- maps:to_list(Ls)],
    frequency([{max(length(IVs), 1), float()} | IVs]).

bool_or_bool_local(#state{locals = Ls}) ->
    IVs = [{1, {local, K}} || {K, bool} <- maps:to_list(Ls)],
    frequency([{max(length(IVs), 1), bool()} | IVs]).

string_or_string_local(#state{locals = Ls}) ->
    IVs = [{1, {local, K}} || {K, string} <- maps:to_list(Ls)],
    frequency([{max(length(IVs), 1), string()} | IVs]).

spec_uop_int(S, N) when N =< 1 ->
    {oneof(['+','-']), int_or_int_local(S)};

spec_uop_int(S, N) ->
    {oneof(['+','-']), spec_inner_int(S, N - 1)}.

spec_uop_bool(S, N) when N =< 1 ->
    {'not', bool_or_bool_local(S)};

spec_uop_bool(S, N) ->
    {'not', spec_inner_bool(S, N - 1)}.

spec_bop_string(S, N) when N =< 1 ->
    {oneof(['+']), string_or_string_local(S), string_or_string_local(S)};

spec_bop_string(S, N) ->
    N1 = N div 2,
    N2 = N - N1,
    {oneof(['+']), spec_bop_string(S, N1), spec_bop_string(S, N2)}.

spec_bop_bool(S, N) when N =< 1 ->
    {oneof(['and', 'or', '==', '!=']), bool_or_bool_local(S), bool_or_bool_local(S)};

spec_bop_bool(S, N) ->
    N1 = N div 2,
    N2 = N - N1,
    oneof([
           {oneof(['and', 'or']), spec_bop_bool(S, N1), spec_bop_bool(S, N2)},
           {oneof(['==','!=']), spec_inner(S, N1), spec_inner(S, N2)},
           {oneof(['>=','>','<','<=']),
            oneof([spec_inner_int(S, N1), spec_inner_float(S, N1)]),
            oneof([spec_inner_int(S, N2), spec_inner_float(S, N2)])},
           {oneof(['>=','>','<','<=']),
            spec_inner_string(S, N1),
            spec_inner_string(S, N2)}
          ]).


spec_bop_float(S, N) when N =< 1 ->
    oneof([
           {oneof(['+','-','*','/']),
            float_or_float_local(S),
            float_or_float_local(S)},
           {oneof(['+','-','*','/']),
            float_or_float_local(S),
            int_or_int_local(S)},
           {oneof(['+','-','*','/']),
            int_or_int_local(S),
            float_or_float_local(S)}
          ]);

spec_bop_float(S, N) ->
    N1 = N div 2,
    N2 = N - N1,
    oneof([
           {oneof(['+','-','*','/']),
            spec_bop_float(S, N1),
            spec_bop_float(S, N2)},
           {oneof(['+','-','*','/']),
            spec_bop_float(S, N1),
            spec_bop_int(S, N2)},
           {oneof(['+','-','*','/']),
            spec_bop_int(S, N1),
            spec_bop_float(S, N2)}
          ]).
spec_bop_int(S, N) when N =< 1 ->
    {oneof(['+','-','*','/']),
     int_or_int_local(S),
     int_or_int_local(S)};

spec_bop_int(S, N) ->
    N1 = N div 2,
    N2 = N - N1,
    {oneof(['+','-','*','/']),
     spec_bop_int(S, N1),
     spec_bop_int(S, N2)}.
