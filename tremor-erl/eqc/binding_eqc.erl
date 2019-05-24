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
               root,
               schema,
               %% All created VMs in this run
               bindings = []
              }).


-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    {ok, Root} = jsg_jsonschema:read_schema("../static/openapi.json"),
    {ok, Schema} = jsg_jsonref:deref(["components", "schemas", "binding"], Root),
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

binding(#state{root = Root, schema = Schema}) ->
    ?LET(V, jsongen:json(Schema, [{root, Root}, {depth, 3}]), tremor_http:decode(list_to_binary(jsg_json:encode(V)))).

binding_with_id(State = #state{bindings = Bindings}) ->
    ?LET(Id,
         elements(Bindings),
         ?LET(Binding, binding(State),
              maps:put(<<"id">>, Id, Binding))).


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

publish_binding_args(S = #state{connection = C}) ->
    [C, binding(S)].

publish_binding_pre(#state{}) ->
    true.

publish_binding_pre(#state{bindings = Bindings}, [_C, #{<<"id">> := Id}]) ->
    not lists:member(Id, Bindings).

publish_binding(C, Binding) ->
    tremor_binding:publish(Binding, C).

publish_binding_post(#state{root = Root, schema = Schema},
                     [_C, #{<<"id">> := Id}],
                     {ok, Resp = #{<<"id">> := Id}}) ->
    jesse_validator:validate(Schema, Root, jsone:encode(Resp));

publish_binding_post(_, _, _) ->
    false.

publish_binding_next(S = #state{bindings = Bindings}, _Result, [_C, #{<<"id">> := Id}]) ->
    S#state{bindings = [Id | Bindings]}.

%% -----------------------------------------------------------------------------
%% Grouped operator: publish_existing_binding_args
%% Publish an binding with an Id we know exists, this should result in a 409
%% (conflict) error.
%% -----------------------------------------------------------------------------

publish_existing_binding_args(S = #state{connection = C}) ->
    [C, binding_with_id(S)].

publish_existing_binding_pre(#state{bindings = []}) ->
    false;
publish_existing_binding_pre(#state{}) ->
    true.

publish_existing_binding_prex(#state{bindings = Bindings}, [_C, #{<<"id">> := Id}]) ->
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

unpublish_binding_post(#state{root = Root, schema = Schema},
                       [_, Id], {ok, Resp = #{<<"id">> := Id}}) ->
    jesse_validator:validate(Schema, Root, jsone:encode(Resp));

unpublish_binding_post(_, _, _) ->
    false.

unpublish_binding_next(S = #state{bindings = Bindings}, _Result, [_C, Deleted]) ->
    S#state{bindings = [Id || Id <- Bindings, Id =/= Deleted]}.


%% -----------------------------------------------------------------------------
%% Grouped operator: find_nonexisting_binding_args
%% Find an binding that was not published first, this should return a 404
%% (not found) error.
%% -----------------------------------------------------------------------------

find_nonexisting_binding_args(#state{connection = C}) ->
    [C, id()].

find_nonexisting_binding_pre(#state{}) ->
    true.

find_nonexisting_binding_pre(#state{bindings = Bindings}, [_C, Id]) ->
    not lists:member(Id, Bindings).

find_nonexisting_binding(C, Id) ->
    tremor_binding:find(Id, C).

find_nonexisting_binding_post(#state{}, [_, _Submitted], {error, 404}) ->
    true;

find_nonexisting_binding_post(_, _, _) ->
    false.

find_nonexisting_next(S, _, _) ->
    S.

%% -----------------------------------------------------------------------------
%% Grouped operator: find_binding_args
%% Findes an binding that we know exists. This should return the
%% artefact with the matchign id along with it's instances.
%% -----------------------------------------------------------------------------

find_binding_args(#state{connection = C, bindings = Bindings}) ->
    [C, elements(Bindings)].

find_binding_pre(#state{bindings = []}) ->
    false;
find_binding_pre(#state{}) ->
    true.

find_binding_pre(#state{bindings = Bindings}, [_C, Id]) ->
    lists:member(Id, Bindings).

find_binding(C, Id) ->
    tremor_binding:find(Id, C).

find_binding_post(#state{root = Root}, [_, Id], {ok, Resp = #{<<"artefact">> := #{<<"id">> := Id}}}) ->
    {ok, S} = jsg_jsonref:deref(["components", "schemas", "binding_state"], Root),
    jesse_validator:validate(S, Root, jsone:encode(Resp));

find_binding_post(_, _, _) ->
    io:format("Error: bad body"),
    false.

find_binding_next(S = #state{bindings = Bindings}, _Result, [_C, Deleted]) ->
    S.
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



