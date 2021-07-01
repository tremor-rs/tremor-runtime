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

-module(spec).

-include_lib("pbt.hrl").

-export([gen/1, gen_array/1, gen_bool/1, gen_float/1,
	 gen_int/1, gen_record/1, gen_string/1, id/0, valid/1]).

valid(Script) ->
    try lists:foldr(fun (AAst, {S, _}) ->
			    model:eval(S, AAst)
		    end,
		    {#vars{}, null}, Script),
	true
    catch
      %  for testing, A, B are errors and C is stack trace
      %   A:B:C ->
      %   io:format("Invalid: ~p\n~p:~p\n~p", [Script, A, B, C]),
      %   false
      _:_ -> false
    end.

id() ->
    ?LET(Path,
	 (?SUCHTHAT(Id,
		    (?LET(Id, (list(choose($a, $e))),
			  (list_to_binary(Id)))),
		    (byte_size(Id) > 0))),
	 Path).

float() -> real().

gen(#state{} = S) -> resize(3, ?SIZED(N, (gen(S, N)))).

gen(#state{} = S, N) ->
    frequency([{10, {emit, spec_inner(S, N)}}, {1, drop},
	       {80, spec_inner(S, N)}]).

gen_int(#state{} = S) ->
    resize(3, ?SIZED(N, (spec_inner_int(S, N)))).

gen_float(#state{} = S) ->
    resize(3, ?SIZED(N, (spec_inner_float(S, N)))).

gen_string(#state{} = S) ->
    resize(3, ?SIZED(N, (spec_inner_string(S, N)))).

gen_bool(#state{} = S) ->
    resize(3, ?SIZED(N, (spec_inner_bool(S, N)))).

gen_array(#state{} = S) ->
    resize(3, ?SIZED(N, (spec_inner_array(S, N)))).

gen_record(#state{} = S) ->
    resize(3, ?SIZED(N, (spec_inner_record(S, N)))).

spec_inner(#state{} = S, N) ->
    ?LAZY((frequency([{10, spec_inner_float(S, N)},
		      {10, spec_inner_int(S, N)},
		      {10, spec_inner_string(S, N)},
		      {10, spec_inner_bool(S, N)},
		      {10, spec_inner_array(S, N)},
		      {10, spec_inner_record(S, N)}]))).

% FIX ME!! this is simple fix to avoid wrong float comparision in nested structures.
spec_inner_no_float(#state{} = S, N) ->
    ?LAZY((frequency([{10, spec_inner_int(S, N)},
		      {10, spec_inner_string(S, N)},
		      {10, spec_inner_bool(S, N)},
		      {10, spec_inner_array(S, N)},
		      {10, spec_inner_record(S, N)}]))).

spec_inner_int(#state{} = S, N) ->
    ?LAZY((frequency([{10, spec_bop_int(S, N)},
		      {5, spec_uop_int(S, N)}]))).

spec_inner_float(#state{} = S, N) ->
    ?LAZY((frequency([{10, spec_bop_float(S, N)},
		      {5, spec_uop_float(S, N)}]))).

spec_inner_string(#state{} = S, N) ->
    ?LAZY((frequency([{5, spec_bop_string(S, N)},
		      {5, spec_string_interpolation(S, N)}]))).

spec_inner_bool(#state{} = S, N) ->
    ?LAZY((frequency([{10, spec_bop_bool(S, N)},
		      {1, spec_uop_bool(S, N)}]))).

spec_inner_array(S, N) when N =< 1 ->
    array_or_array_local(S);
spec_inner_array(S, N) ->
    {array, list(N - 1, spec_inner_no_float(S, N - 1))}.

literal_record(S, N) when N =< 1 ->
    record_or_record_local(S);
literal_record(S, N) ->
    {record, map(string(), spec_inner_no_float(S, N - 1))}.

spec_inner_record(#state{} = S, N) when N =< 1 ->
    literal_record(S, N);
spec_inner_record(#state{} = S, N) ->
    ?LAZY((frequency([{5, spec_bop_record(S, N - 1)},
		      {10, spec_uop_record(S, N - 1)}]))).

string() ->
    base64:encode(crypto:strong_rand_bytes(rand:uniform(10))).

small_int() -> choose(1, 100).

int_or_int_local(#state{locals = Ls}) ->
    IVs = [{1, {local, K}} || {K, int} <- maps:to_list(Ls)],
    frequency([{max(length(IVs), 1), small_int()} | IVs]).

float_or_float_local(#state{locals = Ls}) ->
    IVs = [{1, {local, K}}
	   || {K, float} <- maps:to_list(Ls)],
    frequency([{max(length(IVs), 1), float()} | IVs]).

bool_or_bool_local(#state{locals = Ls}) ->
    IVs = [{1, {local, K}}
	   || {K, bool} <- maps:to_list(Ls)],
    frequency([{max(length(IVs), 1), bool()} | IVs]).

string_or_string_local(#state{locals = Ls}) ->
    IVs = [{1, {local, K}}
	   || {K, string} <- maps:to_list(Ls)],
    frequency([{max(length(IVs), 1), string()} | IVs]).

array_or_array_local(#state{locals = Ls}) ->
    IVs = [{1, {local, K}}
	   || {K, array} <- maps:to_list(Ls)],
    frequency([{max(length(IVs), 1), {array, []}} | IVs]).

record_or_record_local(#state{locals = Ls} = S) ->
    IVs = [{1, {local, K}}
	   || {K, record} <- maps:to_list(Ls)],
    frequency([{max(length(IVs), 1),
		{record,
		 map(string(),
		     oneof([bool_or_bool_local(S), int_or_int_local(S),
			    string_or_string_local(S)]))}}
	       | IVs]).

spec_uop_int(S, N) when N =< 1 ->
    ?SHRINK({oneof(['+', '-']), int_or_int_local(S)},
	    [int_or_int_local(S)]);
spec_uop_int(S, N) ->
    ?SHRINK({oneof(['+', '-']), spec_inner_int(S, N - 1)},
	    [spec_inner_int(S, N - 1)]).

spec_uop_float(S, N) when N =< 1 ->
    ?SHRINK({oneof(['+', '-']), float_or_float_local(S)},
	    [float_or_float_local(S)]);
spec_uop_float(S, N) ->
    ?SHRINK({oneof(['+', '-']), spec_inner_float(S, N - 1)},
	    [spec_inner_float(S, N - 1)]).

% Operations generated by patch_operation
% {merge, Value}
% {merge, Key, Value}
% {insert, Key, Value}
% {upsert, Key, Value}
% {update, Key, Value}
% {erase, Key}
patch_operation(S, N) ->
    frequency([{1,
		{insert, string(), spec_inner_no_float(S, N - 1)}},
	       {1, {upsert, string(), spec_inner_no_float(S, N - 1)}},
	       {1, {update, string(), spec_inner_no_float(S, N - 1)}},
	       {1, {merge, string(), spec_inner_record(S, N - 1)}},
	       {1, {merge, spec_inner_record(S, N - 1)}},
	       {1, {default, string(), spec_inner_record(S, N - 1)}},
	       {1, {default, spec_inner_record(S, N - 1)}},
	       {1, {erase, string()}}]).

% spec_uop_record function returns {patch, generated_record, patch_operations}
spec_uop_record(S, N) when N =< 1 ->
    ?SHRINK({patch, literal_record(S, N - 1),
	     ?SUCHTHAT(X, (list(1, patch_operation(S, N - 1))),
		       (length(X) >= 1))},
	    [literal_record(S, N - 1)]);
spec_uop_record(S, N) ->
    ?SHRINK({patch, spec_inner_record(S, N - 1),
	     ?SUCHTHAT(X, (list(1, patch_operation(S, N - 1))),
		       (length(X) >= 1))},
	    [spec_inner_record(S, N - 1)]).

spec_uop_bool(S, N) when N =< 1 ->
    ?SHRINK({'not', bool_or_bool_local(S)},
	    [bool_or_bool_local(S)]);
spec_uop_bool(S, N) ->
    ?SHRINK({'not', spec_inner_bool(S, N - 1)},
	    [spec_inner_bool(S, N - 1)]).

spec_string_interpolation(S, N) when N =< 1 ->
    ?SHRINK({'#', string(), string(),
	     oneof([float(), string(), small_int()])},
	    [string(), string(),
	     oneof([float(), string(), small_int()])]);
spec_string_interpolation(S, N) ->
    ?SHRINK({'#', string(), string(), spec_inner(S, N - 1)},
	    [string(), string(), spec_inner(S, N - 1)]).

spec_bop_string(S, N) when N =< 1 ->
    ?SHRINK({oneof(['+']), string_or_string_local(S),
	     string_or_string_local(S)},
	    [string_or_string_local(S), string_or_string_local(S)]);
spec_bop_string(S, N) ->
    N1 = N div 2,
    N2 = N - N1,
    ?SHRINK({oneof(['+']), spec_bop_string(S, N1),
	     spec_bop_string(S, N2)},
	    [spec_bop_string(S, N1), spec_bop_string(S, N2)]).

spec_bop_bool(S, N) when N =< 1 ->
    ?SHRINK({oneof(['and', 'or', '==', '!=']),
	     bool_or_bool_local(S), bool_or_bool_local(S)},
	    [bool_or_bool_local(S), bool_or_bool_local(S)]);
spec_bop_bool(S, N) ->
    N1 = N div 2,
    N2 = N - N1,
    oneof([?SHRINK({oneof(['and', 'or']),
		    spec_bop_bool(S, N1), spec_bop_bool(S, N2)},
		   [spec_bop_bool(S, N1), spec_bop_bool(S, N2)]),
	   ?SHRINK({oneof(['==', '!=']), spec_inner(S, N1),
		    spec_inner(S, N2)},
		   [spec_inner(S, N1), spec_inner(S, N2)]),
	   ?SHRINK({oneof(['>=', '>', '<', '<=']),
		    oneof([spec_inner_int(S, N1), spec_inner_float(S, N1)]),
		    oneof([spec_inner_int(S, N2),
			   spec_inner_float(S, N2)])},
		   [oneof([spec_inner_int(S, N1),
			   spec_inner_float(S, N1)]),
		    oneof([spec_inner_int(S, N2),
			   spec_inner_float(S, N2)])]),
	   ?SHRINK({oneof(['>=', '>', '<', '<=']),
		    spec_inner_string(S, N1), spec_inner_string(S, N2)},
		   [spec_inner_string(S, N1), spec_inner_string(S, N2)])]).

spec_bop_float(S, N) when N =< 1 ->
    oneof([?SHRINK({oneof(['+', '-', '*', '/']),
		    float_or_float_local(S), float_or_float_local(S)},
		   [float_or_float_local(S), float_or_float_local(S)]),
	   ?SHRINK({oneof(['+', '-', '*', '/']),
		    float_or_float_local(S), int_or_int_local(S)},
		   [float_or_float_local(S), int_or_int_local(S)]),
	   ?SHRINK({oneof(['+', '-', '*', '/']),
		    int_or_int_local(S), float_or_float_local(S)},
		   [int_or_int_local(S), float_or_float_local(S)]),
	   ?SHRINK({oneof(['/']), int_or_int_local(S),
		    int_or_int_local(S)},
		   [int_or_int_local(S), int_or_int_local(S)])]);
spec_bop_float(S, N) ->
    N1 = N div 2,
    N2 = N - N1,
    oneof([?SHRINK({oneof(['+', '-', '*', '/']),
		    spec_bop_float(S, N1), spec_bop_float(S, N2)},
		   [spec_bop_float(S, N1), spec_bop_float(S, N2)]),
	   ?SHRINK({oneof(['+', '-', '*', '/']),
		    spec_bop_float(S, N1), spec_bop_int(S, N2)},
		   [spec_bop_float(S, N1), spec_bop_int(S, N2)]),
	   ?SHRINK({oneof(['+', '-', '*', '/']),
		    spec_bop_int(S, N1), spec_bop_float(S, N2)},
		   [spec_bop_int(S, N1), spec_bop_float(S, N2)]),
	   ?SHRINK({oneof(['/']), spec_bop_int(S, N1),
		    spec_bop_int(S, N2)},
		   [spec_bop_int(S, N1), spec_bop_int(S, N2)])]).

spec_bop_int(S, N) when N =< 1 ->
    ?SHRINK({oneof(['+', '-', '*', 'band', 'bxor']),
	     int_or_int_local(S), int_or_int_local(S)},
	    [int_or_int_local(S), int_or_int_local(S)]);
spec_bop_int(S, N) ->
    N1 = N div 2,
    N2 = N - N1,
    % FIX ME!! LHS should be spec_inner_int
    ?SHRINK({oneof(['+', '-', '*', 'band', 'bxor']),
	     spec_bop_int(S, N1), spec_bop_int(S, N2)},
	    [spec_bop_int(S, N1), spec_bop_int(S, N2)]).

spec_bop_record(S, N) when N =< 1 ->
    ?SHRINK({oneof([merge]), literal_record(S, N - 1),
	     literal_record(S, N - 1)},
	    [literal_record(S, N - 1), literal_record(S, N - 1)]);
spec_bop_record(S, N) ->
    ?SHRINK({oneof([merge]), spec_inner_record(S, N - 1),
	     spec_inner_record(S, N - 1)},
	    [spec_inner_record(S, N - 1),
	     spec_inner_record(S, N - 1)]).
