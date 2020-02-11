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
               root,
               schema,
               %% All created VMs in this run
               onramps = []
              }).


-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    {ok, Root} = jsg_jsonschema:read_schema("../static/openapi.json"),
    {ok, Schema} = jsg_jsonref:deref(["components", "schemas", "onramp"], Root),
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

onramp(#state{root = Root, schema = Schema}) ->
    ?LET(V, jsongen:json(Schema, [{root, Root}, {depth, 3}]), tremor_http:decode(list_to_binary(jsg_json:encode(V)))).

%%    ?LET(Id, id(), onramp_with_id([Id])).

onramp_with_id(State = #state{onramps = Onramps}) ->
    ?LET(Id,
         elements(Onramps),
         ?LET(Onramp, onramp(State),
              maps:put(<<"id">>, Id, Onramp))).

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

publish_onramp_args(S = #state{connection = C}) ->
    [C, onramp(S)].

publish_onramp_pre(#state{}) ->
    true.

publish_onramp_pre(#state{onramps = Onramps}, [_C, #{<<"id">> := Id}]) ->
    not lists:member(Id, Onramps).

publish_onramp(C, Onramp) ->
    tremor_onramp:publish(Onramp, C).

publish_onramp_post(#state{schema = Schema, root = Root},
                    [_C, Resp = #{<<"id">> := Id}], 
                    {ok, #{<<"id">> := Id}}) ->
    jesse_validator:validate(Schema, Root, jsone:encode(Resp));
publish_onramp_post(_, _, _) ->
    false.

publish_onramp_next(S = #state{onramps = Onramps}, _Result, [_C, #{<<"id">> := Id}]) ->
    S#state{onramps = [Id | Onramps]}.

%% -----------------------------------------------------------------------------
%% Grouped operator: publish_existing_onramp_args
%% Publish an onramp with an Id we know exists, this should result in a 409
%% (conflict) error.
%% -----------------------------------------------------------------------------

publish_existing_onramp_args(S = #state{connection = C}) ->
    [C, onramp_with_id(S)].

publish_existing_onramp_pre(#state{onramps = []}) ->
    false;
publish_existing_onramp_pre(#state{}) ->
    true.

publish_existing_onramp_pre(#state{onramps = Onramps}, [_C, #{<<"id">> := Id}]) ->
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

unpublish_onramp_post(#state{root = Root, schema = Schema}, [_, Id], {ok, Resp = #{<<"id">> := Id}}) ->
    jesse_validator:validate(Schema, Root, jsone:encode(Resp));

unpublish_onramp_post(_, _, _) ->
    false.

unpublish_onramp_next(S = #state{onramps = Onramps}, _Result, [_C, Deleted]) ->
    S#state{onramps = [Id || Id <- Onramps, Id =/= Deleted]}.


%% -----------------------------------------------------------------------------
%% Grouped operator: find_nonexisting_onramp_args
%% Find an onramp that was not published first, this should return a 404
%% (not found) error.
%% -----------------------------------------------------------------------------

find_nonexisting_onramp_args(#state{connection = C}) ->
    [C, id()].

find_nonexisting_onramp_pre(#state{}) ->
    true.

find_nonexisting_onramp_pre(#state{onramps = Onramps}, [_C, Id]) ->
    not lists:member(Id, Onramps).

find_nonexisting_onramp(C, Id) ->
    tremor_onramp:find(Id, C).

find_nonexisting_onramp_post(#state{}, [_, _Submitted], {error, 404}) ->
    true;

find_nonexisting_onramp_post(_, _, _) ->
    false.

find_nonexisting_next(S, _, _) ->
    S.

%% -----------------------------------------------------------------------------
%% Grouped operator: find_onramp_args
%% Findes an offramp that we know exists. This should return the
%% artefact with the matchign id along with it's instances.
%% -----------------------------------------------------------------------------

find_onramp_args(#state{connection = C, onramps = Onramps}) ->
    [C, elements(Onramps)].

find_onramp_pre(#state{onramps = []}) ->
    false;
find_onramp_pre(#state{}) ->
    true.

find_onramp_pre(#state{onramps = Onramps}, [_C, Id]) ->
    lists:member(Id, Onramps).

find_onramp(C, Id) ->
    tremor_onramp:find(Id, C).

find_onramp_post(#state{root = Root}, [_, Id], {ok, Resp = #{<<"artefact">> := #{<<"id">> := Id}}}) ->
    {ok, S} = jsg_jsonref:deref(["components", "schemas", "onramp_state"], Root),
    jesse_validator:validate(S, Root, jsone:encode(Resp));

find_onramp_post(_, _, _) ->
    io:format("Error: bad body"),
    false.

find_onramp_next(S, _Result, [_C, _Deleted]) ->
    S.

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



