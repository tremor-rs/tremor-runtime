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

-module(model).

-include_lib("pbt.hrl").

-export([eval/1, eval/2]).

resolve(#state{locals = L}, {local, _} = K) ->
    maps:get(K, L).

combine_values(Key, null, Acc) -> maps:remove(Key, Acc);
combine_values(Key, SpecVal = #{}, Acc) ->
    case maps:get(Key, Acc) of
      TargetVal = #{} ->
	  maps:fold(fun combine_values/3, TargetVal, SpecVal);
      _ -> maps:put(Key, SpecVal, Acc)
    end;
combine_values(Key, SpecVal, Acc) ->
    maps:put(Key, SpecVal, Acc).

% Operations covered by the folowing patch_operation are
% {merge, Value}
% {merge, Key, Value}
% {insert, Key, Value}
% {upsert, Key, Value}
% {update, Key, Value}
% {erase, Key}

patch_operation({insert, Key, Value}, Acc) ->
    maps:put(Key, Value, Acc);
patch_operation({merge, Key, Value}, Acc) ->
    maps:fold(fun combine_values/3, #{Key => Value}, Acc);
patch_operation({upsert, Key, Value}, Acc) ->
    % does what we expect from upsert
    maps:put(Key, Value, Acc);
patch_operation({erase, Key}, Acc) ->
    maps:remove(Key, Acc).

-spec ast_eval(#vars{}, {}) -> {#vars{},
				integer() | float() | boolean() | binary()}.

ast_eval(#vars{} = S, {patch, Expr, PatchOperation}) ->
    {_, ExprUpdate} = ast_eval(S, Expr),
    UpdatdPatchOperation = lists:map(fun ({erase, Key}) ->
					     {_, UpdatedKey} = ast_eval(S, Key),
					     {erase, UpdatedKey};
					 ({merge, Key}) ->
					     {_, UpdatedKey} = ast_eval(S, Key),
					     {merge, UpdatedKey};
					 ({insert, Key, Value}) ->
					     {_, UpdatedKey} = ast_eval(S, Key),
					     {_, UpdatedValue} = ast_eval(S,
									  Value),
					     {insert, UpdatedKey, UpdatedValue};
					 ({upsert, Key, Value}) ->
					     {_, UpdatedKey} = ast_eval(S, Key),
					     {_, UpdatedValue} = ast_eval(S,
									  Value),
					     {upsert, UpdatedKey, UpdatedValue};
                     ({update, Key, Value}) ->
					     {_, UpdatedKey} = ast_eval(S, Key),
					     {_, UpdatedValue} = ast_eval(S,
									  Value),
					     {update, UpdatedKey, UpdatedValue};
					 ({merge, Key, Value}) ->
					     {_, UpdatedKey} = ast_eval(S, Key),
					     {_, UpdatedValue} = ast_eval(S,
									  Value),
					     {merge, UpdatedKey, UpdatedValue};
					 (X) -> X
				     end,
				     PatchOperation),
    {S,
     lists:foldl(fun patch_operation/2, ExprUpdate,
		 UpdatdPatchOperation)};
ast_eval(#vars{} = S, {merge, Expr1, Expr2}) ->
    {_, Expr1Update} = ast_eval(S, Expr1),
    {_, Expr2Update} = ast_eval(S, Expr2),
    {S,
     maps:fold(fun combine_values/3, Expr1Update,
	       Expr2Update)};
ast_eval(#vars{} = S, {record, Expr}) ->
    Expr1 = [{ast_eval(S, Key), ast_eval(S, Value)}
	     || {Key, Value} <- maps:to_list(Expr)],
    Expr2 = [{Key, Value}
	     || {{_, Key}, {_, Value}} <- Expr1],
    {S, maps:from_list(Expr2)};
ast_eval(#vars{} = S, {array, Expr}) ->
    Expr1 = [ast_eval(S, X) || X <- Expr],
    Expr2 = [Elem || {_, Elem} <- Expr1],
    {S, Expr2};
ast_eval(#vars{} = S, {'#', A, B, Expr}) ->
    {S1, Expr1} = ast_eval(S, Expr),
    case Expr1 of
      Expr1 when is_binary(Expr1) ->
	  {S1, <<A/binary, Expr1/binary, B/binary>>};
      Expr1 ->
	  {S1,
	   <<A/binary, (jsx:encode(util:unfloat(Expr1)))/binary,
	     B/binary>>}
    end;
ast_eval(#vars{} = S, {'+', A, B})
    when is_binary(A) andalso is_binary(B) ->
    {S, <<A/binary, B/binary>>};
ast_eval(#vars{} = S, {'+', A, B}) ->
    {S1, A1} = ast_eval(S, A),
    {S2, B1} = ast_eval(S1, B),
    case {A1, B1} of
      {A1, B1} when is_binary(A1) andalso is_binary(B1) ->
	  {S, <<A1/binary, B1/binary>>};
      {A1, B1} -> {S2, A1 + B1}
    end;
ast_eval(#vars{} = S, {'-', A, B}) ->
    {S1, A1} = ast_eval(S, A),
    {S2, B1} = ast_eval(S1, B),
    {S2, A1 - B1};
ast_eval(#vars{} = S, {'/', A, B}) ->
    {S1, A1} = ast_eval(S, A),
    {S2, B1} = ast_eval(S1, B),
    {S2, A1 / B1};
ast_eval(#vars{} = S, {'*', A, B}) ->
    {S1, A1} = ast_eval(S, A),
    {S2, B1} = ast_eval(S1, B),
    {S2, A1 * B1};
ast_eval(#vars{} = S, {'%', A, B}) ->
    {S1, A1} = ast_eval(S, A),
    {S2, B1} = ast_eval(S1, B),
    {S2, util:mod(A1, B1)};
ast_eval(#vars{} = S, {'==', A, B}) ->
    {S1, A1} = ast_eval(S, A),
    {S2, B1} = ast_eval(S1, B),
    {S2, A1 == B1};
ast_eval(#vars{} = S, {'!=', A, B}) ->
    {S1, A1} = ast_eval(S, A),
    {S2, B1} = ast_eval(S1, B),
    {S2, A1 /= B1};
ast_eval(#vars{} = S, {'>=', A, B}) ->
    {S1, A1} = ast_eval(S, A),
    {S2, B1} = ast_eval(S1, B),
    {S2, A1 >= B1};
ast_eval(#vars{} = S, {'>', A, B}) ->
    {S1, A1} = ast_eval(S, A),
    {S2, B1} = ast_eval(S1, B),
    {S2, A1 > B1};
ast_eval(#vars{} = S, {'<', A, B}) ->
    {S1, A1} = ast_eval(S, A),
    {S2, B1} = ast_eval(S1, B),
    {S2, A1 < B1};
ast_eval(#vars{} = S, {'<=', A, B}) ->
    {S1, A1} = ast_eval(S, A),
    {S2, B1} = ast_eval(S1, B),
    {S2, A1 =< B1};
ast_eval(#vars{} = S, {'and', A, B}) ->
    {S1, A1} = ast_eval(S, A),
    {S2, B1} = ast_eval(S1, B),
    {S2, A1 andalso B1};
ast_eval(#vars{} = S, {'or', A, B}) ->
    {S1, A1} = ast_eval(S, A),
    {S2, B1} = ast_eval(S1, B),
    {S2, A1 orelse B1};
ast_eval(#vars{} = S, {'not', A}) ->
    {S1, A1} = ast_eval(S, A), {S1, not A1};
ast_eval(#vars{} = S, {'band', A, B}) ->
    {S1, A1} = ast_eval(S, A),
    {S2, B1} = ast_eval(S1, B),
    {S2, A1 band B1};
ast_eval(#vars{} = S, {'bxor', A, B}) ->
    {S1, A1} = ast_eval(S, A),
    {S2, B1} = ast_eval(S1, B),
    {S2, A1 bxor B1};
ast_eval(#vars{} = S, {'+', A}) ->
    {S1, A1} = ast_eval(S, A), {S1, A1};
ast_eval(#vars{} = S, {'-', A}) ->
    {S1, A1} = ast_eval(S, A), {S1, -A1};
ast_eval(#vars{locals = L} = S, {'let', Path, Expr}) ->
    {S1, R} = ast_eval(S, Expr),
    LL = maps:put(Path, R, L),
    {S1#vars{locals = LL}, R};
ast_eval(S, true) -> {S, true};
ast_eval(S, false) -> {S, false};
ast_eval(S, null) -> {S, null};
ast_eval(S, N) when is_number(N) -> {S, N};
ast_eval(S, B) when is_binary(B) -> {S, B};
ast_eval(S, {local, _} = L) -> {S, resolve(S, L)};
ast_eval(#vars{} = S, {emit, A}) ->
    {S1, R} = ast_eval(S, A), {S1, {emit, R}};
ast_eval(#vars{} = S, drop) -> {S, drop}.

%% We run the model specification through a simple implementation of
%% tremor-script implemented in Erlang natively.
eval(Spec) -> eval(#vars{}, Spec).

eval(#vars{} = SNOT, Spec) ->
    case ast_eval(SNOT, Spec) of
      {S, {emit, X}} ->
	  {S, #{<<"emit">> => util:clamp(X, 13)}};
      {S, drop} -> {S, #{<<"drop">> => null}};
      {S, X} -> {S, #{<<"emit">> => util:clamp(X, 13)}}
    end.
