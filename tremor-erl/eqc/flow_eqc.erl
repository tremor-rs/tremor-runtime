%% Copyright 2022, The Tremor Team
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%% 
-module(flow_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_component.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state, {
    connection,
    root,
    %% All the flows we that are deployed
    flows
}).

-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    {ok, Root} = jsg_jsonschema:read_schema("../static/openapi.json"),
    Client = tremor_api:new(),
    {ok, Flows} = tremor_flow:list(Client),
    #state{
        connection = Client,
        root = Root,
        flows = Flows
    }.

command_precondition_common(_S, _Command) ->
    true.

precondition_common(_S, _Call) ->
    true.

%% -----------------------------------------------------------------------------

get_runtime_status_args(#state{connection = C}) ->
    [C].

get_runtime_status_pre(#state{}) ->
    true.

get_runtime_status(C) ->
    tremor_status:get(C).

get_runtime_status_post(#state{flows = Flows, root = Root}, _Args, {ok, Resp = #{}}) ->
    get_runtime_status_post_(Flows, Root, Resp);
get_runtime_status_post(#state{flows = Flows, root = Root}, _Args, {error, 503, Resp = #{}}) ->
    get_runtime_status_post_(Flows, Root, Resp);
get_runtime_status_post(#state{}, _Args, _) ->
    false.

get_runtime_status_post_(Flows, Root, Resp = #{<<"num_flows">> := NumFlows, <<"flows">> := FlowStatus, <<"all_running">> := AllRunning}) ->
    {ok, Schema} = jsg_jsonref:deref(["components", "schemas", "runtime_status"], Root),
    Validated = jesse_validator:validate(Schema, Root, jsone:encode(Resp)),
    ExpectedNumFlows = length(Flows),
    ExpectedFlowStatus = lists:foldl(
        fun({Status, _Count}, Map) -> maps:put(Status, maps:get(Status, Map, 0) + 1, Map) end, 
        #{}, 
        [ {Status, 1} || #{<<"status">> := Status} <- Flows]
    ),
    ExpectedAllRunning = length([{} || #{<<"status">> := <<"running">>} <- Flows]) == ExpectedNumFlows,
    Validated and (FlowStatus == ExpectedFlowStatus) and (NumFlows == ExpectedNumFlows) == (AllRunning == ExpectedAllRunning).

get_runtime_status_next(S, _, _) ->
    S.

%% -----------------------------------------------------------------------------

list_flows_args(#state{connection = C}) ->
    [C].

list_flows_pre(#state{}) ->
    true.

list_flows(C) ->
    tremor_flow:list(C).

list_flows_post(#state{flows = Flows, root = Root}, _Args, {ok, Resp}) ->
    {ok, Schema} = jsg_jsonref:deref(["components", "schemas", "flows"], Root),
    % verify schema and check against current state
    jesse_validator:validate(Schema, Root, jsone:encode(Resp)) and (lists:sort(Resp) == lists:sort(Flows)).

list_flows_next(S, _Result, _Args) ->
    S.

%% -----------------------------------------------------------------------------

get_flow_args(#state{connection = C, flows = Flows}) ->
    [C, elements(Flows)].

get_flow_pre(#state{flows = []}) ->
    false;
get_flow_pre(#state{}) ->
    true.

get_flow_pre(#state{flows = Flows}, [_C, Flow]) ->
    lists:member(Flow, Flows).

get_flow(C, _Flow = #{<<"alias">> := Alias}) ->
    tremor_flow:get(Alias, C).

get_flow_post(#state{root = Root}, [_, Flow = #{<<"alias">> := Alias}], {ok, Resp = #{<<"alias">> := Alias}}) ->
    {ok, Schema} = jsg_jsonref:deref(["components", "schemas", "flow"], Root),
    jesse_validator:validate(Schema, Root, jsone:encode(Resp)) and (Flow == Resp);
    
get_flow_post(_, _, _) ->
    io:format("Error: unexpected GET flow body"),
    false.

get_flow_next(S, _, _) ->
    S.

%% -----------------------------------------------------------------------------

pause_flow_args(#state{connection = C, flows = Flows}) ->
    [C, elements(Flows)].

pause_flow_pre(#state{flows = []}) ->
    false;
pause_flow_pre(#state{}) ->
    true.

pause_flow_pre(#state{flows = Flows}, [_C, Flow]) ->
    #{<<"status">> := Status} = Flow,
    (Status == <<"running">>) and lists:member(Flow, Flows).

pause_flow(C, _Flow = #{<<"alias">> := Alias}) ->
    tremor_flow:pause(Alias, C).

pause_flow_post(#state{root = Root}, [_C, _Flow = #{<<"alias">> := Alias}], {ok, Resp = #{<<"alias">> := Alias, <<"status">> := <<"paused">>}}) ->
    {ok, Schema} = jsg_jsonref:deref(["components", "schemas", "flow"], Root),
    jesse_validator:validate(Schema, Root, jsone:encode(Resp));

pause_flow_post(_, _, _) ->
    false.

pause_flow_next(S = #state{flows = Flows}, _Res, [_C, Paused]) ->
    % manually set the status
    Flows2 = [maps:put(<<"status">>, <<"paused">>, Paused) | lists:delete(Paused, Flows)],
    S#state{flows = Flows2}.

%% -----------------------------------------------------------------------------

resume_flow_args(#state{connection = C, flows = Flows}) ->
    [C, elements(Flows)].

resume_flow_pre(#state{flows = []}) ->
    false;
resume_flow_pre(#state{}) ->
    true.

resume_flow_pre(#state{flows = Flows}, [_c, Flow]) ->
    #{<<"status">> := Status} = Flow,
    ((Status == <<"paused">>) or (Status == <<"running">>)) and lists:member(Flow, Flows).

resume_flow(C, _Flow = #{<<"alias">> := Alias}) ->
    tremor_flow:resume(Alias, C).

resume_flow_post(#state{root = Root}, [_C, _Flow = #{<<"alias">> := Alias}], {ok, Resp = #{<<"alias">> := Alias, <<"status">> := <<"running">>}}) ->
    {ok, Schema} = jsg_jsonref:deref(["components", "schemas", "flow"], Root),
    jesse_validator:validate(Schema, Root, jsone:encode(Resp));
resume_flow_post(_, _, _) ->
    false.

resume_flow_next(S = #state{flows = Flows}, _Res, [_C, Resumed]) ->
    % manually set the status
    Flows2 = [maps:put(<<"status">>, <<"running">>, Resumed) | lists:delete(Resumed, Flows)],
    S#state{flows = Flows2}.


%% -----------------------------------------------------------------------------
%% Flow property
%% -----------------------------------------------------------------------------

-spec prop_flow() -> eqc:property().
prop_flow() ->
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
%% Cleanup = in case we need it
%% -----------------------------------------------------------------------------

cleanup() ->
    % no need to cleanup anything
    ok.
