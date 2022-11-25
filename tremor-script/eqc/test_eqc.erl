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

%% We build a script in the state by adding 1 command at a time.
%% The command is either a let binding for a new variable with a value
%% or an expression that potentially uses those variables.
%%
%% In case the script results in an overflow, we do not add the last
%% operation to the script. So we do test that overflows result in
%% an error on the Rust side, but we don't work with script that have
%% an overflow somewhere before the final operation.

%% Define what tests to skip due to unfinished modeling of semantics
%% Remove element from the list to notice what is missing.
skip(Script) ->
    skip([badkey, negative, float_arith], Script).

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
expr_pre(S, [Script, Expr]) ->
    %% for better shrinking
    S#state.script == Script andalso
        not skip([Expr | Script]).

expr_adapt(S, [_Script, Expr]) ->
    [S#state.script, Expr].

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
    [
        S#state.script,
        spec:id(),
        oneof([
            {int, spec:gen_int(S)},
            {bool, spec:gen_bool(S)}
            %{float, spec:gen_float(S)},
            %{string, spec:gen_string(S)}
        ])
    ].

let_local_pre(S, [Script, _, {_Type, Expr}]) ->
    S#state.script == Script andalso
        not skip([Expr | Script]).

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
        case ModelResult of
            {'EXIT', float_arith} ->
                io:format("Model exit with float arithmatic\n"),
                true;
            _ ->
                is_exit(RustResult)
        end,
    case ValidExit orelse ModelResult =:= RustResult of
        true ->
            true;
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
    lists:foldr(
        fun(Ast, Result) ->
            Line = gen_script:pp(Ast),
            <<Result/binary, Line/binary, ";\n">>
        end,
        <<>>,
        Script
    ).

tremor_script_eval(Spec) ->
    tremor_script_eval([], Spec).

tremor_script_eval(Init, Spec) ->
    Testcase = asts_to_script([Spec | Init]),
    {ok, RustResult} = ts:eval(Testcase),
    ErlangResult = jsx:decode(RustResult),
    util:clamp(ErlangResult, 13).

is_exit({badrpc, {'EXIT', _}}) ->
    true;
is_exit({'EXIT', _}) ->
    true;
is_exit(_) ->
    false.

skip(Issues, Script) ->
    OnlyPositive = lists:member(negative, Issues),
    case model_eval(Script) of
        {'EXIT', _} when OnlyPositive ->
            true;
        {'EXIT', {badkey, _}} ->
            lists:member(badkey, Issues);
        {'EXIT', float_arith} ->
            lists:member(float_arith, Issues);
        _ ->
            false
    end.

-spec prop_simple_expr() -> eqc:property().
prop_simple_expr() ->
    in_parallel(
        ?FORALL(
            Params,
            ?SUCHTHAT(Ast, spec:gen(initial_state()), not skip([Ast])),
            begin
                RustResult = remote_eval(tremor_script_eval, [Params]),
                ModelResult = model_eval([Params]),
                ?WHENFAIL(
                    io:format(
                        "SIMPLE EXPR MODEL FAILED!\n AST: ~p\n Script: ~s\n Expected Result: ~p\n Actual Result: ~p\n",
                        [Params, gen_script:pp(Params), ModelResult, RustResult]
                    ),
                    %% The real ( rust ) and model simulation ( erlang ) results should be equivalent
                    (is_exit(RustResult) andalso is_exit(ModelResult)) orelse
                        ModelResult =:= RustResult
                )
            end
        )
    ).

-spec prop_simple_expr_with_state() -> eqc:property().
prop_simple_expr_with_state() ->
    in_parallel(
        ?FORALL(
            Cmds,
            commands(?MODULE),
            begin
                {History, State, Result} = run_commands(Cmds, []),
                measure(
                    length,
                    commands_length(Cmds),
                    aggregate(
                        call_features(History),
                        pretty_commands(?MODULE, Cmds, {History, State, Result}, Result == ok)
                    )
                )
            end
        )
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
    try
        {_, ModelResult} =
            lists:foldr(
                fun(AAst, {Vars, _}) -> model:eval(Vars, AAst) end,
                {#vars{}, null},
                Script
            ),
        ModelResult
    catch
        _:Reason ->
            {'EXIT', Reason}
    end.
