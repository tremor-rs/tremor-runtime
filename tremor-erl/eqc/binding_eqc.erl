%% Bugs:
%%
%% * [ ] Allow submitting empty ids (workaround by not allowing empty submits for now)
%% * [ ] Deleting non existing binding returns 400 when no bindings exists
%% * [ ] Deleting non existing binding returns 500 when bindings exists
%% * [x] Publishing an binding doesn't return the artefact but only it's config.
%% * [ ] Non ANSI id's can get translated to unicode leading to input and output id differing
%% * [ ] Publishing a binding with 'bad' content closes the connection
-module(binding_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_component.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state,{
               connection,
               %% All created VMs in this run
               bindings = []
              }).


-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{
       connection = tremor_api:new()
      }.

command_precondition_common(_S, _Command) ->
    true.

precondition_common(_S, _Call) ->
    true.

id() ->
    ?SUCHTHAT(Id, ?LET(Id, list(choose($a, $z)), list_to_binary(Id) ), byte_size(Id) > 0).

binding() ->
    ?LET(Id, id(), binding_with_id([Id])).

binding_with_id(Ids) ->
    ?LET(Id,
         elements(Ids),
         #{
           id => Id,
           links => #{
                      <<"/onramp/test/{instance}/out">> =>  [ <<"/pipeline/test/{instance}/in">> ]
                     }
          }).

%% -----------------------------------------------------------------------------
%% Grouped operator: list_binding
%% When listing bindings we require the return values to be the same as the ones
%% we locally collected.
%% -----------------------------------------------------------------------------

list_binding_args(#state{connection = C}) ->
    [C].

list_binding_pre(#state{}) ->
    true.

list_binding(C) ->
    {ok, Bindings} = tremor_binding:list(C),
    Bindings.

list_binding_post(#state{bindings = Bindings}, _Args, Result) ->
    lists:sort(Result) == lists:sort(Bindings).

list_binding_next(S, _Result, _Args) ->
    S.

%% -----------------------------------------------------------------------------
%% Grouped operator: publish_binding_args
%% Publish a new binding with a key we know doesn't exist. We syould receive
%% a return with the same Id as the one submitted.
%%
%% We add the Id to the offramps we keep track off.
%% -----------------------------------------------------------------------------

publish_binding_args(#state{connection = C}) ->
    [C, binding()].

publish_binding_pre(#state{}) ->
    true.

publish_binding_pre(#state{bindings = Bindings}, [_C, #{id := Id}]) ->
    not lists:member(Id, Bindings).

publish_binding(C, Binding) ->
    tremor_binding:publish(Binding, C).

publish_binding_post(#state{}, [_C, #{id := Id}], {ok, #{<<"id">> := Id}}) ->
    true;
publish_binding_post(_, _, _) ->
    false.

publish_binding_next(S = #state{bindings = Bindings}, _Result, [_C, #{id := Id}]) ->
    S#state{bindings = [Id | Bindings]}.

%% -----------------------------------------------------------------------------
%% Grouped operator: publish_existing_binding_args
%% Publish an binding with an Id we know exists, this should result in a 409
%% (conflict) error.
%% -----------------------------------------------------------------------------

publish_existing_binding_args(#state{connection = C, bindings = Bindings}) ->
    [C, binding_with_id(Bindings)].

publish_existing_binding_pre(#state{bindings = []}) ->
    false;
publish_existing_binding_pre(#state{}) ->
    true.

publish_existing_binding_pre(#state{bindings = Bindings}, [_C, #{id := Id}]) ->
    lists:member(Id, Bindings).

publish_existing_binding(C, Binding) ->
    tremor_binding:publish(Binding, C).

publish_existing_binding_post(#state{}, [_, _Submitted], {error, 409}) ->
    true;

publish_existing_binding_post(_, _, _) ->
    false.

publish_existing_binding_next(S, _, _) ->
    S.

%% -----------------------------------------------------------------------------
%% Grouped operator: unpublish_nonexisting_binding_args
%% Unpublish an binding that was not published first, this should return a 404
%% (not found) error.
%% -----------------------------------------------------------------------------

unpublish_nonexisting_binding_args(#state{connection = C}) ->
    [C, id()].

unpublish_nonexisting_binding_pre(#state{}) ->
    true.

unpublish_nonexisting_binding_pre(#state{bindings = Bindings}, [_C, Id]) ->
    not lists:member(Id, Bindings).

unpublish_nonexisting_binding(C, Id) ->
    tremor_binding:unpublish(Id, C).

unpublish_nonexisting_binding_post(#state{}, [_, _Submitted], {error, 404}) ->
    true;

unpublish_nonexisting_binding_post(_, _, _) ->
    false.

unpublish_nonexisting_next(S, _, _) ->
    S.

%% -----------------------------------------------------------------------------
%% Grouped operator: unpublish_binding_args
%% Unpublishes an offramp that we know exists. This should return the
%% artefact with the matchign id.
%%
%% We remove the Id from our state.
%% -----------------------------------------------------------------------------

unpublish_binding_args(#state{connection = C, bindings = Bindings}) ->
    [C, elements(Bindings)].

unpublish_binding_pre(#state{bindings = []}) ->
    false;
unpublish_binding_pre(#state{}) ->
    true.

unpublish_binding_pre(#state{bindings = Bindings}, [_C, Id]) ->
    lists:member(Id, Bindings).

unpublish_binding(C, Id) ->
    tremor_binding:unpublish(Id, C).

unpublish_binding_post(#state{}, [_, Id], {ok, #{<<"id">> := Id}}) ->
    true;

unpublish_binding_post(_, _, _) ->
    false.

unpublish_binding_next(S = #state{bindings = Bindings}, _Result, [_C, Deleted]) ->
    S#state{bindings = [Id || Id <- Bindings, Id =/= Deleted]}.

%% -----------------------------------------------------------------------------
%% Final property
%% -----------------------------------------------------------------------------

-spec prop_binding() -> eqc:property().
prop_binding() ->
    ?SETUP(fun() ->
                   %% setup mocking here
                   tremor_api:start(),
                   cleanup(),
                   fun () -> cleanup() end %% Teardown function
           end,
           ?FORALL(Cmds, commands(?MODULE),
                   begin
                       {H, S, Res} = run_commands(Cmds, []),
                       Res1 = cleanup(),
                       Success =
                           case {Res, Res1} of
                               {ok, ok} ->
                                   true;
                               {_, ok} ->
                                   false;
                               {ok, _} ->
                                   io:format("[~p] Res1: ~p~n", [?LINE, Res1]),
                                   false;
                               _ ->
                                   io:format("[~p] Res1: ~p~n", [?LINE, Res1]),
                                   false
                           end,
                       pretty_commands(?MODULE, Cmds, {H, S, Res}, Success)
                   end)).


%% -----------------------------------------------------------------------------
%% Cleanup = removes all bindings
%% -----------------------------------------------------------------------------

cleanup() ->
    C = tremor_api:new(),
    %% We do it twice to ensure failed vm's are deleted proppelry.
    {ok, Bindings} = tremor_binding:list(C),
    [tremor_binding:unpublish(Binding, C) || Binding <- Bindings],
    {ok, []} = tremor_binding:list(C),
    ok.



