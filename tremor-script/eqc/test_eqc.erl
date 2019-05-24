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
    event = #{ <<"foo">> => <<"bar">> }
}.


spec() ->
    oneof([spec_bop_real(),spec_bop_int(),spec_bop_bool(), spec_uop_bool(), spec_uop_int(), spec_uop_real(), spec_bop_string()]).

spec_uop_real() ->
    {oneof([<<"Plus">>,<<"Minus">>]), ?SUCHTHAT(X, real(), X =/= 0.0)}.

spec_uop_int() ->
    {oneof([<<"Plus">>,<<"Minus">>]), choose(1,100)}.

spec_uop_bool() ->
    {<<"Not">>, bool()}.

spec_bop_string() ->
    {oneof([<<"Add">>]), utf8(10), utf8(10)}.
%%      {<<"Add">>, oneof([<<"snot">>, <<"cookie">>]), oneof([<<"badger">>, <<"monster">>])}.

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
    %% io:format("~p~n", [JsonAst]),
    RustResult = ts:eval(jsx:encode(Testcase)),
    ErlangResult = jsx:decode(RustResult),
    [{ _ , RustValue }] = ErlangResult,
    util:clamp(RustValue,13).

%% We run the model specification through a simple implementation of
%% tremor-script implemented in Erlang natively.
model_eval(Spec) ->
    util:clamp(model:ast_eval(Spec),13).

-spec prop_simple_expr() -> eqc:property().
prop_simple_expr() ->
    ?FORALL(Params, spec(), 
            begin 
                RustResult = tremor_script_eval(Params),
                ModelResult = model_eval(Params),
                ?WHENFAIL(
                   io:format("SIMPLE EXPR MODEL FAILED! ~p ~p ~p", 
                             [Params, ModelResult, RustResult]), 
                             %% The real ( rust ) and model simulation ( erlang ) results should be equivalent
                             ModelResult =:= RustResult
                  )
            end).

