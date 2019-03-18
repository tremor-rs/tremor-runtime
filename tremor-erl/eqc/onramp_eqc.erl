%% Bugs:
%%
%% * [ ] Allow submitting empty ids (workaround by not allowing empty submits for now)
%% * [ ] Deleting non existing onramp returns 400 when no onramps exists
%% * [ ] Deleting non existing onramp returns 500 when onramps exists
%% * [x] Publishing an onramp doesn't return the artefact but only it's config.
%% * [ ] Non ANSI id's can get translated to unicode leading to input and output id differing

-module(onramp_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_component.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state,{
               connection,
               %% All created VMs in this run
               onramps = []
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

onramp() ->
    ?LET(Id, id(), onramp_with_id([Id])).

onramp_with_id(Ids) ->
    ?LET(Id,
         elements(Ids),
         #{
           id => Id,
           type => <<"file">>,
           config => #{
                       source => <<"file.txt">>
                      }
          }).

%% -----------------------------------------------------------------------------
%% Grouped operator: list_onramp
%% When listing onramps we require the return values to be the same as the ones
%% we locally collected.
%% -----------------------------------------------------------------------------

list_onramp_args(#state{connection = C}) ->
    [C].

list_onramp_pre(#state{}) ->
    true.

list_onramp(C) ->
    {ok, Onramps} = tremor_onramp:list(C),
    Onramps.

list_onramp_post(#state{onramps = Onramps}, _Args, Result) ->
    lists:sort(Result) == lists:sort(Onramps).

list_onramp_next(S, _Result, _Args) ->
    S.

%% -----------------------------------------------------------------------------
%% Grouped operator: publish_onramp_args
%% Publish a new onramp with a key we know doesn't exist. We syould receive
%% a return with the same Id as the one submitted.
%%
%% We add the Id to the offramps we keep track off.
%% -----------------------------------------------------------------------------

publish_onramp_args(#state{connection = C}) ->
    [C, onramp()].

publish_onramp_pre(#state{}) ->
    true.

publish_onramp_pre(#state{onramps = Onramps}, [_C, #{id := Id}]) ->
    not lists:member(Id, Onramps).

publish_onramp(C, Onramp) ->
    tremor_onramp:publish(Onramp, C).

publish_onramp_post(#state{}, [_C, #{id := Id}], {ok, #{<<"id">> := Id}}) ->
    true;
publish_onramp_post(_, _, _) ->
    false.

publish_onramp_next(S = #state{onramps = Onramps}, _Result, [_C, #{id := Id}]) ->
    S#state{onramps = [Id | Onramps]}.

%% -----------------------------------------------------------------------------
%% Grouped operator: publish_existing_onramp_args
%% Publish an onramp with an Id we know exists, this should result in a 409
%% (conflict) error.
%% -----------------------------------------------------------------------------

publish_existing_onramp_args(#state{connection = C, onramps = Onramps}) ->
    [C, onramp_with_id(Onramps)].

publish_existing_onramp_pre(#state{onramps = []}) ->
    false;
publish_existing_onramp_pre(#state{}) ->
    true.

publish_existing_onramp_pre(#state{onramps = Onramps}, [_C, #{id := Id}]) ->
    lists:member(Id, Onramps).

publish_existing_onramp(C, Onramp) ->
    tremor_onramp:publish(Onramp, C).

publish_existing_onramp_post(#state{}, [_, _Submitted], {error, 409}) ->
    true;

publish_existing_onramp_post(_, _, _) ->
    false.

publish_existing_onramp_next(S, _, _) ->
    S.

%% -----------------------------------------------------------------------------
%% Grouped operator: unpublish_nonexisting_onramp_args
%% Unpublish an onramp that was not published first, this should return a 404
%% (not found) error.
%% -----------------------------------------------------------------------------

unpublish_nonexisting_onramp_args(#state{connection = C}) ->
    [C, id()].

unpublish_nonexisting_onramp_pre(#state{}) ->
    true.

unpublish_nonexisting_onramp_pre(#state{onramps = Onramps}, [_C, Id]) ->
    not lists:member(Id, Onramps).

unpublish_nonexisting_onramp(C, Id) ->
    tremor_onramp:unpublish(Id, C).

unpublish_nonexisting_onramp_post(#state{}, [_, _Submitted], {error, 404}) ->
    true;

unpublish_nonexisting_onramp_post(_, _, _) ->
    false.

unpublish_nonexisting_next(S, _, _) ->
    S.

%% -----------------------------------------------------------------------------
%% Grouped operator: unpublish_onramp_args
%% Unpublishes an offramp that we know exists. This should return the
%% artefact with the matchign id.
%%
%% We remove the Id from our state.
%% -----------------------------------------------------------------------------

unpublish_onramp_args(#state{connection = C, onramps = Onramps}) ->
    [C, elements(Onramps)].

unpublish_onramp_pre(#state{onramps = []}) ->
    false;
unpublish_onramp_pre(#state{}) ->
    true.

unpublish_onramp_pre(#state{onramps = Onramps}, [_C, Id]) ->
    lists:member(Id, Onramps).

unpublish_onramp(C, Id) ->
    tremor_onramp:unpublish(Id, C).

unpublish_onramp_post(#state{}, [_, Id], {ok, #{<<"id">> := Id}}) ->
    true;

unpublish_onramp_post(_, _, _) ->
    false.

unpublish_onramp_next(S = #state{onramps = Onramps}, _Result, [_C, Deleted]) ->
    S#state{onramps = [Id || Id <- Onramps, Id =/= Deleted]}.

%% -----------------------------------------------------------------------------
%% Final property
%% -----------------------------------------------------------------------------

-spec prop_onramp() -> eqc:property().
prop_onramp() ->
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
%% Cleanup = removes all onramps
%% -----------------------------------------------------------------------------

cleanup() ->
    C = tremor_api:new(),
    %% We do it twice to ensure failed vm's are deleted proppelry.
    {ok, Onramps} = tremor_onramp:list(C),
    [tremor_onramp:unpublish(Onramp, C) || Onramp <- Onramps],
    {ok, []} = tremor_onramp:list(C),
    ok.



