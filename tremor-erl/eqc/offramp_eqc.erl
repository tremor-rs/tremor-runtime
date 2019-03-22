%% Bugs:
%%
%% * [ ] Allow submitting empty ids (workaround by not allowing empty submits for now)
%% * [ ] Deleting non existing offramp returns 400 when no offramps exists
%% * [ ] Deleting non existing offramp returns 500 when offramps exists
%% * [x] Publishing an offramp doesn't return the artefact but only it's config.
%% * [ ] Non ANSI id's can get translated to unicode leading to input and output id differing

-module(offramp_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_component.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(SYS_OFFRAMPS, [<<"system::stderr">>,<<"system::stdout">>]).

-record(state,{
               connection,
               %% All created VMs in this run
               offramps = []
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

offramp() ->
    ?LET(Id, id(), offramp_with_id([Id])).

offramp_with_id(Ids) ->
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
%% Grouped operator: list_offramp
%% When listing offramps we require the return values to be the same as the ones
%% we locally collected.
%% -----------------------------------------------------------------------------

list_offramp_args(#state{connection = C}) ->
    [C].

list_offramp_pre(#state{}) ->
    true.

list_offramp(C) ->
    {ok, Offramps} = tremor_offramp:list(C),
    Offramps.

list_offramp_post(#state{offramps = Offramps}, _Args, Result) ->
    lists:sort(Result) == lists:sort(Offramps ++ ?SYS_OFFRAMPS).

list_offramp_next(S, _Result, _Args) ->
    S.

%% -----------------------------------------------------------------------------
%% Grouped operator: publish_offramp_args
%% Publish a new offramp with a key we know doesn't exist. We syould receive
%% a return with the same Id as the one submitted.
%%
%% We add the Id to the offramps we keep track off.
%% -----------------------------------------------------------------------------

publish_offramp_args(#state{connection = C}) ->
    [C, offramp()].

publish_offramp_pre(#state{}) ->
    true.

publish_offramp_pre(#state{offramps = Offramps}, [_C, #{id := Id}]) ->
    not lists:member(Id, Offramps).

publish_offramp(C, Offramp) ->
    tremor_offramp:publish(Offramp, C).

publish_offramp_post(#state{}, [_C, #{id := Id}], {ok, #{<<"id">> := Id}}) ->
    true;
publish_offramp_post(_, _, _) ->
    false.

publish_offramp_next(S = #state{offramps = Offramps}, _Result, [_C, #{id := Id}]) ->
    S#state{offramps = [Id | Offramps]}.

%% -----------------------------------------------------------------------------
%% Grouped operator: publish_existing_offramp_args
%% Publish an offramp with an Id we know exists, this should result in a 409
%% (conflict) error.
%% -----------------------------------------------------------------------------

publish_existing_offramp_args(#state{connection = C, offramps = Offramps}) ->
    [C, offramp_with_id(Offramps)].

publish_existing_offramp_pre(#state{offramps = []}) ->
    false;
publish_existing_offramp_pre(#state{}) ->
    true.

publish_existing_offramp_pre(#state{offramps = Offramps}, [_C, #{id := Id}]) ->
    lists:member(Id, Offramps).

publish_existing_offramp(C, Offramp) ->
    tremor_offramp:publish(Offramp, C).

publish_existing_offramp_post(#state{}, [_, _Submitted], {error, 409}) ->
    true;

publish_existing_offramp_post(_, _, _) ->
    false.

publish_existing_offramp_next(S, _, _) ->
    S.

%% -----------------------------------------------------------------------------
%% Grouped operator: unpublish_nonexisting_offramp_args
%% Unpublish an offramp that was not published first, this should return a 404
%% (not found) error.
%% -----------------------------------------------------------------------------

unpublish_nonexisting_offramp_args(#state{connection = C}) ->
    [C, id()].

unpublish_nonexisting_offramp_pre(#state{}) ->
    true.

unpublish_nonexisting_offramp_pre(#state{offramps = Offramps}, [_C, Id]) ->
    not lists:member(Id, Offramps).

unpublish_nonexisting_offramp(C, Id) ->
    tremor_offramp:unpublish(Id, C).

unpublish_nonexisting_offramp_post(#state{}, [_, _Submitted], {error, 404}) ->
    true;

unpublish_nonexisting_offramp_post(_, _, _) ->
    false.

unpublish_nonexisting_next(S, _, _) ->
    S.

%% -----------------------------------------------------------------------------
%% Grouped operator: unpublish_offramp_args
%% Unpublishes an offramp that we know exists. This should return the
%% artefact with the matchign id.
%%
%% We remove the Id from our state.
%% -----------------------------------------------------------------------------

unpublish_offramp_args(#state{connection = C, offramps = Offramps}) ->
    [C, elements(Offramps)].

unpublish_offramp_pre(#state{offramps = []}) ->
    false;
unpublish_offramp_pre(#state{}) ->
    true.

unpublish_offramp_pre(#state{offramps = Offramps}, [_C, Id]) ->
    lists:member(Id, Offramps).

unpublish_offramp(C, Id) ->
    tremor_offramp:unpublish(Id, C).

unpublish_offramp_post(#state{}, [_, Id], {ok, #{<<"id">> := Id}}) ->
    true;

unpublish_offramp_post(_, _, _) ->
    false.

unpublish_offramp_next(S = #state{offramps = Offramps}, _Result, [_C, Deleted]) ->
    S#state{offramps = [Id || Id <- Offramps, Id =/= Deleted]}.

%% -----------------------------------------------------------------------------
%% Final property
%% -----------------------------------------------------------------------------

-spec prop_offramp() -> eqc:property().
prop_offramp() ->
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
%% Cleanup = removes all offramps
%% -----------------------------------------------------------------------------

cleanup() ->
    C = tremor_api:new(),
    %% We do it twice to ensure failed vm's are deleted proppelry.
    {ok, Offramps} = tremor_offramp:list(C),
    [tremor_offramp:unpublish(Offramp, C) || Offramp <- Offramps -- ?SYS_OFFRAMPS],
    {ok, Rest} = tremor_offramp:list(C),
    ?SYS_OFFRAMPS = lists:sort(Rest),
    ok.



