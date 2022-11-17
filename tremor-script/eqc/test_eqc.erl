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

%% * [x] 'not true' would crash since it caused reached a unreachable bit of code
%% * [x] missing comparison for strings
%% * [x] different behaviour of == in %{}, case and top level

-module(test_eqc).

-include_lib("pbt.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-compile([export_all]).


-spec initial_state() -> eqc_statem:symbol_state().
initial_state() ->
  #state{
     locals = #{},
     %% globals = #{},
     %% event = #{},
     script = []
  }.


%% Ensure that we only execute AST's that are

command_precondition_common(_S, _Ast) ->
    true.

precondition_common(_State, _Call) ->
    true.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% expr
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

expr_args(#state{} = S) ->
    [S#state.script, spec:gen(S)].

expr_pre(#state{}) ->
    true.

%% We don't allow emit or drop
expr_pre(_S, [_Script, {emit, _}]) ->
    false;
expr_pre(_S, [_Script, drop]) ->
    false;
expr_pre(S, [Script, _Expr]) ->
    S#state.script == Script. %% for better shrinking

expr(Script, Ast) ->
    remote_eval(tremor_script_eval, [Script, Ast]).

expr_post(#state{}, [Script, Ast], RustResult) ->
    expected([Ast | Script], RustResult).

expr_next(#state{} = S, _Eval, [Script, Ast]) ->
    %% Since tremor script has no state we need to keep
    %% track of the commands exectuted so far
    case (catch model_eval([Ast | Script])) of
        {'EXIT', _} ->
            %% Do not add crashing operation to the script
            %% but we test that Rust does also crash in postcondition
            S;
        _ ->
            S#state{script = [Ast | Script]}
    end.

expr_features(#state{locals = Locals}, [_Script, _Ast], _Res) ->
    [{nr_locals_in_script, maps:size(Locals)}].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% let local of type
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

let_local_args(#state{} = S) ->
    [S#state.script, spec:id(), oneof([{int, spec:gen_int(S)},
                                       {bool, spec:gen_bool(S)}
                                       %{float, spec:gen_float(S)},
                                       %{string, spec:gen_string(S)}
                                      ])].

let_local_pre(S, [Script, _, {_Type, _Expr}]) ->
    S#state.script == Script.

let_local_adapt(S, [_Script, Id, TypedExpr]) ->
    [S#state.script, Id, TypedExpr].

let_local(Script, Id, {_Type, Expr}) ->
    remote_eval(tremor_script_eval, [Script, {'let', {local, Id}, Expr}]).

let_local_next(#state{} = S, _Eval, [Script, Id, {Type, Expr}]) ->
    Ast = {'let', {local, Id}, Expr},
    %% Since tremor script has no state we need to keep
    %% track of the commands exectuted so far
    case model_eval([Expr | Script]) of
        {'EXIT', _} ->
            S;
        _ ->
            S#state{
              script = [Ast | Script],
              locals = maps:put(Id, Type, S#state.locals)
             }
    end.

let_local_post(#state{}, [Script, Id, {_Type, Expr}], RustResult) ->
    expected([{'let', {local, Id}, Expr} | Script], RustResult).

expected(Script, RustResult) ->
    ModelResult = model_eval(Script),
    ValidExit =
        is_exit(RustResult) andalso is_exit(ModelResult),
    case ValidExit orelse ModelResult =:= RustResult of
        true -> true;
        false ->
            io:format("~ts", [asts_to_script(Script)]),
            %% io:format("model: ~p =/= tremor: ~p\n", [ModelResult, RustResult]),
            {{model, ModelResult}, '!=', {rust, RustResult}}
    end.


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

asts_to_script(Script) ->
    lists:foldr(fun(Ast, Result) ->
                        Line = gen_script:pp(Ast),
                        <<Result/binary, Line/binary, ";\n">>
                end, <<>>, Script).

tremor_script_eval(Spec) ->
    tremor_script_eval([], Spec).

tremor_script_eval(Init, Spec) ->
    Testcase = asts_to_script([Spec | Init]),
    RustResult = ts:eval(Testcase),
    ErlangResult = jsx:decode(RustResult),
    util:clamp(ErlangResult, 13).

is_exit({badrpc, {'EXIT', _}}) ->
    true;
is_exit({'EXIT', _}) ->
    true;
is_exit(_) ->
    false.

-spec prop_simple_expr() -> eqc:property().
prop_simple_expr() ->
    ?FORALL(Params, spec:gen(initial_state()),
            begin
                RustResult = remote_eval(tremor_script_eval, [Params]),
                ModelResult = model_eval([Params]),
                ?WHENFAIL(
                   io:format("SIMPLE EXPR MODEL FAILED!\n AST: ~p\n Script: ~s\n Expected Result: ~p\n Actual Result: ~p\n",
                             [Params, gen_script:pp(Params), ModelResult, RustResult]),
                             %% The real ( rust ) and model simulation ( erlang ) results should be equivalent
                             (is_exit(RustResult) andalso is_exit(ModelResult)) orelse ModelResult =:= RustResult
                  )
            end).

-spec prop_simple_expr_with_state() -> eqc:property().
prop_simple_expr_with_state() ->
    ?FORALL(Cmds, commands(?MODULE),
        begin
            {History, State, Result} = run_commands(Cmds, []),
            pretty_commands(?MODULE, Cmds, {History, State, Result},
                            ?WHENFAIL(io:format("[~p] Res1: ~p~n", [?LINE, Result]), Result == ok))
        end
    ).


%%====================================================================
%% Remote helpers
%%====================================================================

-ifdef(REMOTE_EVAL).
maybe_client() ->
    case ct_slave:start(eqc_client) of
        {ok, Client} ->
            rpc:call(Client, code, add_paths, [code:get_path()]),
            {ok, Client};
        {error, already_started, Client} ->
            {ok, Client};
        E ->
            io:format(user, ">>>> E: ~p~n", [E]),
            E
    end.
-else.
maybe_client() ->
    {ok, node()}.

-endif.

remote_eval(Fn, Args) ->
    {ok, Client} = maybe_client(),
    rpc:call(Client, ?MODULE, Fn, Args).

model_eval(Script) ->
    try {_, ModelResult} =
             lists:foldr(fun (AAst, {Vars, _}) -> model:eval(Vars, AAst) end,
                         {#vars{}, null}, Script),
         ModelResult
    catch _:Reason ->
            {'EXIT', Reason}
    end.
