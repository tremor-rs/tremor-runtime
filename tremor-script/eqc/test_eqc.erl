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

-module(test_eqc).

-include_lib("pbt.hrl").
-compile([export_all]).


-spec initial_state() -> eqc_statem:symbol_state().
initial_state() ->
  #state{
    locals = #{ <<"a">> => 1, <<"b">> => 2, <<"c">> => 3, <<"d">> => 4, <<"e">> => 5 },
    globals = #{ <<"a">> => 5, <<"b">> => 4, <<"c">> => 3, <<"d">> => 2, <<"e">> => 1 },
    event = #{ <<"foo">> => <<"bar">> }
  }.


command_precondition_common(_State, _Cmd) ->
    true.

precondition_common(_State, _Call) ->
    true.

id() ->
    ?SUCHTHAT(Id, ?LET(Id, list(choose($a, $e)), list_to_binary(Id)), byte_size(Id) > 0).

enqueue_args(#state{} = S) ->
    [spec(S)].

enqueue_pre(#state{} = S) ->
    true.

enqueue(Ast) ->
    tremor_script_eval(Ast).

enqueue_post(#state{} = S, [Ast], RustResult) ->
    {S1, ModelResult} = model_eval(S, Ast),
    ModelResult =:= RustResult.

enqueue_next(#state{} = S, Eval, [Ast]) ->
    {S1, ModelResult} = model_eval(S, Ast),
    S.

spec(S) ->
    frequency([{1, {emit, spec_inner(S)}}, {1, {drop, spec_inner(S)}}, {8, spec_inner(S)}]).

spec_inner(#state{locals = _, globals = _G, event = _E}=S) ->
    oneof([spec_bop_real(),spec_bop_int(),spec_bop_bool(), spec_uop_bool(), spec_uop_int(), spec_uop_real(), spec_bop_string(), spec_let_expr(S)]).

spec_let_expr(S) ->
    {<<"Let">>,id(),?LAZY(spec_inner(S))}.

spec_uop_real() ->
    {oneof([<<"Plus">>,<<"Minus">>]), ?SUCHTHAT(X, real(), X =/= 0.0)}.

spec_uop_int() ->
    {oneof([<<"Plus">>,<<"Minus">>]), choose(1,100)}.

spec_uop_bool() ->
    {<<"Not">>, bool()}.

spec_bop_string() ->
    {oneof([<<"Add">>]), utf8(10), utf8(10)}.

spec_bop_bool() ->
    {oneof([<<"And">>, <<"Or">>,<<"Eq">>,<<"NotEq">>]), bool(), oneof([true,false])}.

spec_bop_real() ->
    {oneof([<<"Add">>,<<"Sub">>,<<"Mul">>,<<"Div">>,<<"Eq">>,<<"NotEq">>,<<"Gte">>,<<"Gt">>,<<"Lt">>,<<"Lte">>]), real(),?SUCHTHAT(X, real(), X =/= 0.0)}.

spec_bop_int() ->
    {oneof([<<"Add">>,<<"Sub">>,<<"Mul">>,<<"Div">>, <<"Mod">>,<<"Eq">>,<<"NotEq">>,<<"Gte">>,<<"Gt">>,<<"Lt">>,<<"Lte">>]), choose(1,100),choose(50,150)}.

%% Given a model specification, we produce a tremor-script AST
%% and send it via an Erlang NIF to Rust as a serialized JSON
%% document.
%%
%% In Rust, we load the AST from the serialized AST and evaluate
%% returning a JSON result back to Erlang/EQC as a serialized JSON
%% document encoded in an erlang binary ( preserves UTF-8 encoding )
%%
%% The result is decoded into Erlang Term format and returned
%%
tremor_script_eval(Spec) ->
    Testcase = gen_ast:gen_ast(Spec),
    JsonAst = jsx:encode(Testcase),
    RustResult = ts:eval(jsx:encode(Testcase)),
    ErlangResult = jsx:decode(RustResult),
    case ErlangResult of
        [{ <<"Emit">> , RustValue }] -> { emit, util:clamp(RustValue, 13) };
        [{ <<"Drop">>, RustValue }] -> { drop, util:clamp(RustValue, 13) };
        Error -> { error, Error}
    end.

%% We run the model specification through a simple implementation of
%% tremor-script implemented in Erlang natively.
model_eval(#state{}=SNOT, Spec) ->
    case model:ast_eval(SNOT,Spec) of
      {S,{emit, X}} -> {S, {emit, util:clamp(X,13)}};
      {S,{drop, X}} -> {S, {drop, util:clamp(X,13)}};
      {S,Y} -> {S, {emit, util:clamp(Y,13)}}
    end.

-spec prop_simple_expr() -> eqc:property().
prop_simple_expr() ->
    S = initial_state(),
    ?FORALL(Params, spec(S), 
            begin 
                RustResult = tremor_script_eval(Params),
                { #state{}, ModelResult } = model_eval(S, Params),
                ?WHENFAIL(
                   io:format("SIMPLE EXPR MODEL FAILED! ~p ~p ~p", 
                             [Params, ModelResult, RustResult]), 
                             %% The real ( rust ) and model simulation ( erlang ) results should be equivalent
                             ModelResult =:= RustResult
                  )
            end).

-spec prop_simple_expr_with_state() -> eqc:property().
prop_simple_expr_with_state() ->
    ?FORALL(Cmds, commands(?MODULE),
        begin
            {History, State, Result} = run_commands(Cmds, []),
            Success =
                case Result of
                    ok ->
                        true;
                    _ ->
                        io:format("[~p] Res1: ~p~n", [?LINE, Result]),
                        false
               end,
            pretty_commands(?MODULE, Cmds, {History, State, Result}, Success)
        end
    ).
