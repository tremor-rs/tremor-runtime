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
               root,
               schema,
               %% All created VMs in this run
               offramps = []
              }).


-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    {ok, Root} = jsg_jsonschema:read_schema("../static/openapi.json"),
    {ok, Schema} = jsg_jsonref:deref(["components", "schemas", "offramp"], Root),
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

offramp(#state{root = Root, schema = Schema}) ->
    ?LET(V, jsongen:json(Schema, [{root, Root}, {depth, 3}]), offramp_prepare(V)).

offramp_prepare(Offramp) ->
    Prepared = tremor_http:decode(list_to_binary(jsg_json:encode(Offramp))),
    offramp_add_valid_config(Prepared).

%% add a valid config - this is to cumbersome to express in openapi.yaml
offramp_add_valid_config(#{<<"type">> := <<"rest">>} = Offramp) ->
    Offramp#{ <<"config">> => #{ <<"endpoint">> => <<"http://127.0.0.1:65535">> }};
offramp_add_valid_config(#{<<"type">> := <<"elastic">>} = Offramp) ->
    Offramp#{<<"config">> => #{ <<"endpoints">> => [<<"http://127.0.0.1:8888">>]}};
offramp_add_valid_config(#{<<"type">> := <<"kafka">>} = Offramp) ->
    Offramp#{<<"config">> => #{ <<"brokers">> => [<<"http://127.0.0.1:8888">>], <<"topic">> => <<"test">> }};
offramp_add_valid_config(#{<<"type">> := <<"ws">>} = Offramp) ->
    Offramp#{<<"config">> => #{ <<"url">> => <<"http://127.0.0.1:8888">>}};
offramp_add_valid_config(#{<<"type">> := <<"udp">>} = Offramp) ->
    Offramp#{<<"config">> => #{ <<"host">> => <<"0.0.0.0">>, <<"port">> => <<"0">>, <<"dst_host">> => <<"example.org">>, <<"dst_port">> => <<"9999">> }};
offramp_add_valid_config(#{<<"type">> := <<"postgres">>} = Offramp) ->
    Offramp#{<<"config">> => #{ <<"host">> => <<"localhost">>, <<"port">> => <<"9990">>, <<"user">> => <<"root">>, <<"password">> => <<"abcdef">>, <<"dbname">> => <<"db">>, <<"table">> => <<"my_table">> }};
offramp_add_valid_config(#{<<"type">> := <<"file">>} = Offramp) ->
    Offramp#{<<"config">> => #{ <<"file">> => <<"/tmp/file.json">>}};
offramp_add_valid_config(#{<<"type">> := <<"blackhole">>} = Offramp) ->
    Offramp#{<<"config">> => #{ <<"warmup_secs">> => <<"1">>, <<"stop_after_secs">> => <<"2">>, <<"significant_figures">> => <<"1">>}};
offramp_add_valid_config(#{<<"type">> := <<"tcp">>} = Offramp) ->
    Offramp#{<<"config">> => #{ <<"host">> => <<"localhost">>, <<"port">> => <<"65535">>}};
offramp_add_valid_config(#{<<"type">> := <<"newrelic">>} = Offramp) ->
    Offramp#{<<"config">> => #{ <<"license_key">> => <<"keystring">>, <<"compress_logs">> => <<"true">>, <<"region">> => <<"europe">>}};
offramp_add_valid_config(Offramp) -> 
    Offramp.


offramp_with_id(State = #state{offramps = Offramps}) ->
    ?LET(Id,
         elements(Offramps),
         ?LET(Artefact, offramp(State),
              maps:put(<<"id">>, Id, Artefact))).

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

publish_offramp_args(S = #state{connection = C}) ->
    [C, offramp(S)].

publish_offramp_pre(#state{}) ->
    true.

publish_offramp_pre(#state{offramps = Offramps}, [_C, #{<<"id">> := Id}]) ->
    not lists:member(Id, Offramps).

publish_offramp(C, Offramp) ->
    tremor_offramp:publish(Offramp, C).

publish_offramp_post(#state{schema = Schema, root = Root},
                     [_C, #{<<"id">> := Id}],
                     {ok, Resp =  #{<<"id">> := Id}}) ->
    jesse_validator:validate(Schema, Root, jsone:encode(Resp));

publish_offramp_post(_, _, _) ->
    false.

publish_offramp_next(S = #state{offramps = Offramps}, _Result, [_C, #{<<"id">> := Id}]) ->
    S#state{offramps = [Id | Offramps]}.

%% -----------------------------------------------------------------------------
%% Grouped operator: publish_existing_offramp_args
%% Publish an offramp with an Id we know exists, this should result in a 409
%% (conflict) error.
%% -----------------------------------------------------------------------------

publish_existing_offramp_args(S = #state{connection = C}) ->
    [C, offramp_with_id(S)].

publish_existing_offramp_pre(#state{offramps = []}) ->
    false;
publish_existing_offramp_pre(#state{}) ->
    true.

publish_existing_offramp_pre(#state{offramps = Offramps}, [_C, #{<<"id">> := Id}]) ->
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

unpublish_offramp_post(#state{schema = Schema, root = Root}, [_, Id], {ok, Resp = #{<<"id">> := Id}}) ->
    jesse_validator:validate(Schema, Root, jsone:encode(Resp));

unpublish_offramp_post(_, _, _) ->
    false.

unpublish_offramp_next(S = #state{offramps = Offramps}, _Result, [_C, Deleted]) ->
    S#state{offramps = [Id || Id <- Offramps, Id =/= Deleted]}.

%% -----------------------------------------------------------------------------
%% Grouped operator: find_nonexisting_offramp_args
%% Find an offramp that was not published first, this should return a 404
%% (not found) error.
%% -----------------------------------------------------------------------------

find_nonexisting_offramp_args(#state{connection = C}) ->
    [C, id()].

find_nonexisting_offramp_pre(#state{}) ->
    true.

find_nonexisting_offramp_pre(#state{offramps = Offramps}, [_C, Id]) ->
    not lists:member(Id, Offramps).

find_nonexisting_offramp(C, Id) ->
    tremor_offramp:find(Id, C).

find_nonexisting_offramp_post(#state{}, [_, _Submitted], {error, 404}) ->
    true;

find_nonexisting_offramp_post(_, _, _) ->
    false.

find_nonexisting_next(S, _, _) ->
    S.

%% -----------------------------------------------------------------------------
%% Grouped operator: find_offramp_args
%% Findes an offramp that we know exists. This should return the
%% artefact with the matchign id along with it's instances.
%% -----------------------------------------------------------------------------

find_offramp_args(#state{connection = C, offramps = Offramps}) ->
    [C, elements(Offramps)].

find_offramp_pre(#state{offramps = []}) ->
    false;
find_offramp_pre(#state{}) ->
    true.

find_offramp_pre(#state{offramps = Offramps}, [_C, Id]) ->
    lists:member(Id, Offramps).

find_offramp(C, Id) ->
    tremor_offramp:find(Id, C).

find_offramp_post(#state{root = Root}, [_, Id], {ok, Resp = #{<<"artefact">> := #{<<"id">> := Id}}}) ->
    {ok, S} = jsg_jsonref:deref(["components", "schemas", "offramp_state"], Root),
    jesse_validator:validate(S, Root, jsone:encode(Resp));

find_offramp_post(_, _, _) ->
    io:format("Error: bad body"),
    false.

find_offramp_next(S, _Result, [_C, _Offramp]) ->
    S.

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



