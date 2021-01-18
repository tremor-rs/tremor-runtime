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

-define(SYS_PIPES, [<<"system::metrics">>, <<"system::passthrough">>]).

-record(state,{
               connection,
               root,
               schema,
               %% All created VMs in this run
               pipelines = []
              }).


-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    {ok, Root} = jsg_jsonschema:read_schema("../static/openapi.json"),
    {ok, Schema} = jsg_jsonref:deref(["components", "schemas", "pipeline"], Root),
    #state{
       connection = tremor_api:new(),
       root = Root,
       schema = Schema
      }.

command_precondition_common(_S, _Command) ->
    true.

precondition_common(_S, _Call) ->
    true.

id() ->
    ?SUCHTHAT(Id, ?LET(Id, list(choose($a, $z)), list_to_binary(Id) ), byte_size(Id) > 0).

%% TODO: chose a simple sample pipeline as constant
%% got to find out a way to do this properly
pipeline(_State) ->
    ?LET(Id, 
        id(),
        << <<"#!config id = \"">>/binary, Id/binary, <<"\"\nselect event from in into out;">>/binary >>
    ).

pipeline_id(Pipeline) ->
    case binary:match(Pipeline, <<"#!config id = \"">>) of
        {Start, Length} -> 
            Idx = Start + Length,
            case binary:match(Pipeline, <<"\"\n">>, [{scope, {Idx, byte_size(Pipeline) - Idx}}]) of
                {SStart, _} ->
                    binary:part(Pipeline, {Idx, SStart - Idx});
                nomatch -> none
            end;
        nomatch -> none
    end.
pipeline_ids(Pipelines) -> [ pipeline_id(P) || P <- Pipelines].

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
    lists:sort(Result) == lists:sort(pipeline_ids(Pipelines) ++ ?SYS_PIPES).

list_pipeline_next(S, _Result, _Args) ->
    S.

%% -----------------------------------------------------------------------------
%% Grouped operator: publish_pipeline_args
%% Publish a new pipeline with a key we know doesn't exist. We syould receive
%% a return with the same Id as the one submitted.
%%
%% We add the Id to the pipelines we keep track off.
%% -----------------------------------------------------------------------------

publish_pipeline_args(S = #state{connection = C}) ->
    [C, pipeline(S)].

publish_pipeline_pre(#state{}) ->
    true.

publish_pipeline_pre(#state{pipelines = Pipelines}, [_C, Pipeline]) ->
    not lists:member(pipeline_id(Pipeline), pipeline_ids(Pipelines)).

publish_pipeline(C, Pipeline) ->
    tremor_pipeline:publish(Pipeline, C).

publish_pipeline_post(_State,
                      [_C, Pipeline],
                      {ok, Resp}) ->
    case {pipeline_id(Pipeline), pipeline_id(Resp)} of
        %% assert that we have the same id published as we have locally
        {Id, Id} -> true;
        _ -> false
    end;
publish_pipeline_post(_, _, _) ->
    false.

publish_pipeline_next(S = #state{pipelines = Pipelines}, _Result, [_C, Pipeline]) ->
    S#state{pipelines = [Pipeline | Pipelines]}.

%% -----------------------------------------------------------------------------
%% Grouped operator: publish_existing_pipeline_args
%% Publish an pipeline with an Id we know exists, this should result in a 409
%% (conflict) error.
%% -----------------------------------------------------------------------------

publish_existing_pipeline_args(#state{connection = C, pipelines = Pipelines}) ->
    [C, elements(Pipelines)].

publish_existing_pipeline_pre(#state{pipelines = []}) ->
    false;
publish_existing_pipeline_pre(#state{}) ->
    true.

publish_existing_pipeline_pre(#state{pipelines = Pipelines}, [_C, Pipeline]) ->
    lists:member(Pipeline, Pipelines).

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
    not lists:member(Id, pipeline_ids(Pipelines)).

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
%% Unpublishes an pipeline that we know exists. This should return the
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

unpublish_pipeline_pre(#state{pipelines = Pipelines}, [_C, Pipeline]) ->
    lists:member(Pipeline, Pipelines).

unpublish_pipeline(C, Pipeline) ->
    tremor_pipeline:unpublish(pipeline_id(Pipeline), C).

unpublish_pipeline_post(_State,
                        [_, Pipeline],
                        {ok, Resp}) ->
    case {pipeline_id(Pipeline), pipeline_id(Resp)} of
        {Id, Id} -> true;
        _ -> false
    end;

unpublish_pipeline_post(_, _, _) ->
    false.

unpublish_pipeline_next(S = #state{pipelines = Pipelines}, _Result, [_C, Deleted]) ->
    S#state{pipelines = [P || P <- Pipelines, P =/= Deleted]}.

%% -----------------------------------------------------------------------------
%% Grouped operator: find_nonexisting_pipeline_args
%% Find an pipeline that was not published first, this should return a 404
%% (not found) error.
%% -----------------------------------------------------------------------------

find_nonexisting_pipeline_args(#state{connection = C}) ->
    [C, id()].

find_nonexisting_pipeline_pre(#state{}) ->
    true.

find_nonexisting_pipeline_pre(#state{pipelines = Pipelines}, [_C, Id]) ->
    not lists:member(Id, pipeline_ids(Pipelines)).

find_nonexisting_pipeline(C, Id) ->
    tremor_pipeline:find(Id, C).

find_nonexisting_pipeline_post(#state{}, [_, _Submitted], {error, 404}) ->
    true;

find_nonexisting_pipeline_post(_, _, _) ->
    false.

find_nonexisting_next(S, _, _) ->
    S.

%% -----------------------------------------------------------------------------
%% Grouped operator: find_pipeline_args
%% Findes an pipeline that we know exists. This should return the
%% artefact with the matchign id along with it's instances.
%% -----------------------------------------------------------------------------

find_pipeline_args(#state{connection = C, pipelines = Pipelines}) ->
    [C, elements(Pipelines)].

find_pipeline_pre(#state{pipelines = []}) ->
    false;
find_pipeline_pre(#state{}) ->
    true.

find_pipeline_pre(#state{pipelines = Pipelines}, [_C, Existing]) ->
    lists:member(Existing, Pipelines).

find_pipeline(C, Pipeline) ->
    tremor_pipeline:find(pipeline_id(Pipeline), C).

find_pipeline_post(_State, [_, Existing], {ok, Resp}) ->
    case {pipeline_id(Existing), pipeline_id(Resp)} of
        {Id, Id} -> true;
        _ -> false
    end;
find_pipeline_post(_, _, Resp) ->
    io:format("Error: bad body ~w~n", [Resp]),
    false.

find_pipeline_next(S, _Result, [_C, _Pipeline]) ->
    S.

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
    {ok, Pipelines} = tremor_pipeline:list(C),
    [ tremor_pipeline:unpublish(Pipeline, C) || Pipeline <- Pipelines -- ?SYS_PIPES],
    {ok, Rest} = tremor_pipeline:list(C),
    ?SYS_PIPES = lists:sort(Rest),
    ok.



