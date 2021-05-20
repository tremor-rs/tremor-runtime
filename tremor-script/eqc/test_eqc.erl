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
expr_pre(_S, [Script, Ast]) ->
    spec:valid([Ast | Script]).

expr(Script, Ast) ->
    remote_eval(tremor_script_eval, [Script, Ast]).

exprenqueue_post(#state{}, [Script, Ast], RustResult) ->
    {_, ModelResult} = lists:foldr(fun(AAst, {S, _}) ->
                                           model:eval(S, AAst)
                                   end, {#vars{}, null}, [Ast | Script]),
    case ModelResult =:= RustResult of
        true -> true;
        false ->
            io:format("~ts", [asts_to_script([Ast | Script])]),
            io:format("model: ~p =/= tremor: ~p\n", [ModelResult, RustResult]),
            false
        end.

expr_next(#state{} = S, _Eval, [Script, Ast]) ->
    %% Since tremor script has no state we need to keep
    %% track of the commands exectuted so far
    S#state{script = [Ast | Script]}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% let_int
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

let_local_int_args(#state{} = S) ->
    [S#state.script, spec:id(), spec:gen_int(S)].

let_local_int_pre(#state{}) ->
    true.

let_local_int_pre(_S, [Script, _, Ast]) ->
    spec:valid([Ast | Script]).

let_local_int(Script, Id, Expr) ->
    remote_eval(tremor_script_eval, [Script, {'let', {local, Id}, Expr}]).

let_local_int_post(#state{}, [Script, Id, Expr], RustResult) ->
    Ast = {'let', {local, Id}, Expr},
    {_, ModelResult} = lists:foldr(fun(AAst, {S, _}) ->
                                           model:eval(S, AAst)
                                   end, {#vars{}, null}, [Ast | Script]),
    case ModelResult =:= RustResult of
        true -> true;
        false ->
            io:format("\nAst:\n~p\n", [lists:reverse([Ast | Script])]),
            io:format("Script:\n~ts\n", [asts_to_script([Ast | Script])]),
            io:format("model: ~p =/= tremor: ~p\n", [ModelResult, RustResult]),
            false
        end.

let_local_int_next(#state{} = S, _Eval, [Script, Id, Expr]) ->
    Ast = {'let', {local, Id}, Expr},
    %% Since tremor script has no state we need to keep
    %% track of the commands exectuted so far
    S#state{
      script = [Ast | Script],
      locals = maps:put(Id, int, S#state.locals)
     }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% let_float
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

let_local_float_args(#state{} = S) ->
    [S#state.script, spec:id(), spec:gen_float(S)].

let_local_float_pre(#state{}) ->
    true.

let_local_float_pre(_S, [Script, _, Ast]) ->
    spec:valid([Ast | Script]).

let_local_float(Script, Id, Expr) ->
    remote_eval(tremor_script_eval, [Script, {'let', {local, Id}, Expr}]).

let_local_float_post(#state{}, [Script, Id, Expr], RustResult) ->
    Ast = {'let', {local, Id}, Expr},
    {_, ModelResult} = lists:foldr(fun(AAst, {S, _}) ->
                                           model:eval(S, AAst)
                                   end, {#vars{}, null}, [Ast | Script]),
    case ModelResult =:= RustResult of
        true -> true;
        false ->
            io:format("\nAst:\n~p\n", [lists:reverse([Ast | Script])]),
            io:format("Script:\n~ts\n", [asts_to_script([Ast | Script])]),
            io:format("model: ~p =/= tremor: ~p\n", [ModelResult, RustResult]),
            false
        end.

let_local_float_next(#state{} = S, _Eval, [Script, Id, Expr]) ->
    Ast = {'let', {local, Id}, Expr},
    %% Since tremor script has no state we need to keep
    %% track of the commands exectuted so far
    S#state{
      script = [Ast | Script],
      locals = maps:put(Id, float, S#state.locals)
     }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% let_bool
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

let_local_bool_args(#state{} = S) ->
    [S#state.script, spec:id(), spec:gen_bool(S)].

let_local_bool_pre(#state{}) ->
    true.

let_local_bool_pre(_S, [Script, _, Ast]) ->
    spec:valid([Ast | Script]).

let_local_bool(Script, Id, Expr) ->
    remote_eval(tremor_script_eval, [Script, {'let', {local, Id}, Expr}]).

let_local_bool_post(#state{}, [Script, Id, Expr], RustResult) ->
    Ast = {'let', {local, Id}, Expr},
    {_, ModelResult} = lists:foldr(fun(AAst, {S, _}) ->
                                           model:eval(S, AAst)
                                   end, {#vars{}, null}, [Ast | Script]),
    case ModelResult =:= RustResult of
        true -> true;
        false ->
            io:format("\nAst:\n~p\n", [lists:reverse([Ast | Script])]),
            io:format("Script:\n~ts\n", [asts_to_script([Ast | Script])]),
            io:format("model: ~p =/= tremor: ~p\n", [ModelResult, RustResult]),
            false
        end.

let_local_bool_next(#state{} = S, _Eval, [Script, Id, Expr]) ->
    Ast = {'let', {local, Id}, Expr},
    %% Since tremor script has no state we need to keep
    %% track of the commands exectuted so far
    S#state{
      script = [Ast | Script],
      locals = maps:put(Id, bool, S#state.locals)
     }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% let_string
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

let_local_string_args(#state{} = S) ->
    [S#state.script, spec:id(), spec:gen_string(S)].

let_local_string_pre(#state{}) ->
    true.

let_local_string_pre(_S, [Script, _, Ast]) ->
    spec:valid([Ast | Script]).

let_local_string(Script, Id, Expr) ->
    remote_eval(tremor_script_eval, [Script, {'let', {local, Id}, Expr}]).

let_local_string_post(#state{}, [Script, Id, Expr], RustResult) ->
    Ast = {'let', {local, Id}, Expr},
    {_, ModelResult} = lists:foldr(fun(AAst, {S, _}) ->
                                           model:eval(S, AAst)
                                   end, {#vars{}, null}, [Ast | Script]),
    case ModelResult =:= RustResult of
        true -> true;
        false ->
            io:format("\nAst:\n~p\n", [lists:reverse([Ast | Script])]),
            io:format("Script:\n~ts\n", [asts_to_script([Ast | Script])]),
            io:format("model: ~p =/= tremor: ~p\n", [ModelResult, RustResult]),
            false
        end.

let_local_string_next(#state{} = S, _Eval, [Script, Id, Expr]) ->
    Ast = {'let', {local, Id}, Expr},
    %% Since tremor script has no state we need to keep
    %% track of the commands exectuted so far
    S#state{
      script = [Ast | Script],
      locals = maps:put(Id, string, S#state.locals)
     }.
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
                        Line = gen_script:gen(Ast),
                        <<Result/binary, Line/binary, ";\n">>
                end, <<>>, Script).

tremor_script_eval(Spec) ->
    tremor_script_eval([], Spec).

tremor_script_eval(Init, Spec) ->
    Testcase = asts_to_script([Spec | Init]),
    RustResult = ts:eval(Testcase),
    ErlangResult = jsx:decode(RustResult),
    util:clamp(ErlangResult, 13).

-spec prop_simple_expr() -> eqc:property().
prop_simple_expr() ->
    ?FORALL(Params, ?SUCHTHAT(Ast, spec:gen(initial_state()), spec:valid([Ast])),
            begin
                RustResult = remote_eval(tremor_script_eval, [Params]),
                { _, ModelResult } = model:eval(Params),
                ?WHENFAIL(
                   io:format("SIMPLE EXPR MODEL FAILED!\n AST: ~p\n Script: ~s\n Expected Result: ~p\n Actual Result: ~p\n",
                             [Params, gen_script:gen(Params), ModelResult, RustResult]),
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

