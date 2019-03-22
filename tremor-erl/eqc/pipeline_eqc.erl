%% Bugs:
%%
%% * [ ] Allow submitting empty ids (workaround by not allowing empty submits for now)
%% * [ ] Deleting non existing pipeline returns 400 when no pipelines exists
%% * [ ] Deleting non existing pipeline returns 500 when pipelines exists
%% * [x] Publishing an pipeline doesn't return the artefact but only it's config.
%% * [ ] Non ANSI id's can get translated to unicode leading to input and output id differing

-module(pipeline_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_component.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(SYS_PIPES, [<<"system::metrics">>]).

-record(state,{
               connection,
               %% All created VMs in this run
               pipelines = []
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

pipeline() ->
    ?LET(Id, id(), pipeline_with_id([Id])).

pipeline_with_id(Ids) ->
    ?LET(Id,
         elements(Ids),
         #{
           id => Id,
           interface => #{
                          inputs => [<<"in">>],
                          outputs => [<<"out">>]
                         },
           nodes => [],
           links => #{
                      in => [<<"out">>]
                     }
          }).

%% -----------------------------------------------------------------------------
%% Grouped operator: list_pipeline
%% When listing pipelines we require the return values to be the same as the ones
%% we locally collected.
%% -----------------------------------------------------------------------------

list_pipeline_args(#state{connection = C}) ->
    [C].

list_pipeline_pre(#state{}) ->
    true.

list_pipeline(C) ->
    {ok, Pipelines} = tremor_pipeline:list(C),
    Pipelines.

list_pipeline_post(#state{pipelines = Pipelines}, _Args, Result) ->
    lists:sort(Result) == lists:sort(Pipelines ++ ?SYS_PIPES).

list_pipeline_next(S, _Result, _Args) ->
    S.

%% -----------------------------------------------------------------------------
%% Grouped operator: publish_pipeline_args
%% Publish a new pipeline with a key we know doesn't exist. We syould receive
%% a return with the same Id as the one submitted.
%%
%% We add the Id to the offramps we keep track off.
%% -----------------------------------------------------------------------------

publish_pipeline_args(#state{connection = C}) ->
    [C, pipeline()].

publish_pipeline_pre(#state{}) ->
    true.

publish_pipeline_pre(#state{pipelines = Pipelines}, [_C, #{id := Id}]) ->
    not lists:member(Id, Pipelines).

publish_pipeline(C, Pipeline) ->
    tremor_pipeline:publish(Pipeline, C).

publish_pipeline_post(#state{}, [_C, #{id := Id}], {ok, #{<<"id">> := Id}}) ->
    true;
publish_pipeline_post(_, _, _) ->
    false.

publish_pipeline_next(S = #state{pipelines = Pipelines}, _Result, [_C, #{id := Id}]) ->
    S#state{pipelines = [Id | Pipelines]}.

%% -----------------------------------------------------------------------------
%% Grouped operator: publish_existing_pipeline_args
%% Publish an pipeline with an Id we know exists, this should result in a 409
%% (conflict) error.
%% -----------------------------------------------------------------------------

publish_existing_pipeline_args(#state{connection = C, pipelines = Pipelines}) ->
    [C, pipeline_with_id(Pipelines)].

publish_existing_pipeline_pre(#state{pipelines = []}) ->
    false;
publish_existing_pipeline_pre(#state{}) ->
    true.

publish_existing_pipeline_pre(#state{pipelines = Pipelines}, [_C, #{id := Id}]) ->
    lists:member(Id, Pipelines).

publish_existing_pipeline(C, Pipeline) ->
    tremor_pipeline:publish(Pipeline, C).

publish_existing_pipeline_post(#state{}, [_, _Submitted], {error, 409}) ->
    true;

publish_existing_pipeline_post(_, _, _) ->
    false.

publish_existing_pipeline_next(S, _, _) ->
    S.

%% -----------------------------------------------------------------------------
%% Grouped operator: unpublish_nonexisting_pipeline_args
%% Unpublish an pipeline that was not published first, this should return a 404
%% (not found) error.
%% -----------------------------------------------------------------------------

unpublish_nonexisting_pipeline_args(#state{connection = C}) ->
    [C, id()].

unpublish_nonexisting_pipeline_pre(#state{}) ->
    true.

unpublish_nonexisting_pipeline_pre(#state{pipelines = Pipelines}, [_C, Id]) ->
    not lists:member(Id, Pipelines).

unpublish_nonexisting_pipeline(C, Id) ->
    tremor_pipeline:unpublish(Id, C).

unpublish_nonexisting_pipeline_post(#state{}, [_, _Submitted], {error, 404}) ->
    true;

unpublish_nonexisting_pipeline_post(_, _, _) ->
    false.

unpublish_nonexisting_next(S, _, _) ->
    S.

%% -----------------------------------------------------------------------------
%% Grouped operator: unpublish_pipeline_args
%% Unpublishes an offramp that we know exists. This should return the
%% artefact with the matchign id.
%%
%% We remove the Id from our state.
%% -----------------------------------------------------------------------------

unpublish_pipeline_args(#state{connection = C, pipelines = Pipelines}) ->
    [C, elements(Pipelines)].

unpublish_pipeline_pre(#state{pipelines = []}) ->
    false;
unpublish_pipeline_pre(#state{}) ->
    true.

unpublish_pipeline_pre(#state{pipelines = Pipelines}, [_C, Id]) ->
    lists:member(Id, Pipelines).

unpublish_pipeline(C, Id) ->
    tremor_pipeline:unpublish(Id, C).

unpublish_pipeline_post(#state{}, [_, Id], {ok, #{<<"id">> := Id}}) ->
    true;

unpublish_pipeline_post(_, _, _) ->
    false.

unpublish_pipeline_next(S = #state{pipelines = Pipelines}, _Result, [_C, Deleted]) ->
    S#state{pipelines = [Id || Id <- Pipelines, Id =/= Deleted]}.

%% -----------------------------------------------------------------------------
%% Final property
%% -----------------------------------------------------------------------------

-spec prop_pipeline() -> eqc:property().
prop_pipeline() ->
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
%% Cleanup = removes all pipelines
%% -----------------------------------------------------------------------------

cleanup() ->
    C = tremor_api:new(),
    %% We do it twice to ensure failed vm's are deleted proppelry.
    {ok, Pipelines} = tremor_pipeline:list(C),
    [tremor_pipeline:unpublish(Pipeline, C) || Pipeline <- Pipelines -- ?SYS_PIPES],
    {ok, Rest} = tremor_pipeline:list(C),
    ?SYS_PIPES = lists:sort(Rest),
    ok.



