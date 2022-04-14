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
-module(flow_connectors_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_component.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state, {
    connection,
    root,
    connectors
}).

-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    {ok, Root} = jsg_jsonschema:read_schema("../static/openapi.json"),
    Client = tremor_api:new(),
    {ok, Flows} = tremor_flow:list(Client),
    FlowConnectors = [{Alias, tremor_flow:list_connectors(Alias, Client)} || #{<<"alias">> := Alias} <- Flows],
    FlowConnectorsMap = maps:from_list([{FlowAlias, Connectors} || {FlowAlias, {ok, Connectors}} <- FlowConnectors]),
    #state{
        connection = Client,
        root = Root,
        connectors = FlowConnectorsMap
    }.

command_precondition_common(_S, _Command) ->
    true.

precondition_common(_S, _Call) ->
    true.

%% -----------------------------------------------------------------------------
%% FIXME

list_flow_connectors_args(#state{connection = C, connectors = ConnectorsMap}) ->
    [C, eqc_gen:elements(maps:to_list(ConnectorsMap))].

list_flow_connectors_pre(#state{connectors = ConnectorsMap = #{}}) when map_size(ConnectorsMap) == 0 ->
    false;
list_flow_connectors_pre(#state{}) ->
    true.

list_flow_connectors(C, {Flow, _Connectors}) ->
    tremor_flow:list_connectors(Flow, C).

list_flow_connectors_post(#state{connectors = Connectors, root = Root}, [_C, {Flow, _Connector}], {ok, Resp}) ->
    FlowConnectors = maps:get(Flow, Connectors),
    {ok, Schema} = jsg_jsonref:deref(["components", "schemas", "connectors"], Root),
    jesse_validator:validate(Schema, Root, jsone:encode(Resp)) and (lists:sort(Resp) == lists:sort(FlowConnectors)).

list_flows_next(S, _, _) ->
    S.

%% -----------------------------------------------------------------------------

get_flow_connector_args(#state{connection = C, connectors = Connectors}) ->
    % filter out flows without connectors
    ConnectorsList = [{FlowAlias, FlowConnectors} || {FlowAlias, FlowConnectors} <- maps:to_list(Connectors), length(FlowConnectors) > 0],
    Gen = case ConnectorsList of
        [] -> noconnector; % this happens when there is no flow with a connector
        ConnectorsList ->
            ?LET({Alias, FlowConnectors}, elements(ConnectorsList), elements([{Alias, Connector} || Connector <- FlowConnectors]))
    end,
    [C, Gen].

get_flow_connector_pre(#state{connectors = Connectors = #{}}) when map_size(Connectors) == 0 ->
    false;
get_flow_connector_pre(#state{}) ->
    true.

get_flow_connector_pre(#state{}, [_C, noconnector]) ->
    false;
get_flow_connector_pre(#state{connectors = Connectors}, [_C, {Flow, Connector}]) ->
    FlowConnectors = maps:get(Flow, Connectors, []),
    lists:member(Connector, FlowConnectors).

get_flow_connector(C, {Flow, _Connector = #{<<"alias">> := Alias}}) ->
    tremor_flow:get_connector(Flow, Alias, C).

get_flow_connector_post(#state{root = Root}, [_C, {_Flow, Connector = #{<<"alias">> := Alias}}], {ok, Resp = #{<<"alias">> := Alias}}) ->
    {ok, Schema} = jsg_jsonref:deref(["components", "schemas", "connector"], Root),
    jesse_validator:validate(Schema, Root, jsone:encode(Resp)) and (Connector == Resp);

get_flow_connector_post(_, _, Res) ->
    io:format("Error: unexpected GET flow connector body response ~p", [Res]),
    false.

get_flow_connector_next(S, _, _) ->
    S.

%% -----------------------------------------------------------------------------

pause_flow_connector_args(#state{connection = C, connectors = Connectors}) ->
    % filter out flows without connectors
    ConnectorsList = [{FlowAlias, FlowConnectors} || {FlowAlias, FlowConnectors} <- maps:to_list(Connectors), length(FlowConnectors) > 0],
    Gen = case ConnectorsList of
        [] -> noconnector; % this happens when there is no flow with a connector
        ConnectorsList ->
            ?LET({Alias, FlowConnectors}, elements(ConnectorsList), elements([{Alias, Connector} || Connector <- FlowConnectors]))
    end,
    [C, Gen].

pause_flow_connector_pre(#state{connectors = Connectors = #{}}) when map_size(Connectors) == 0 ->
    false;
pause_flow_connector_pre(#state{}) ->
    true.

pause_flow_connector_pre(#state{}, [_c, noconnector]) ->
    false;
pause_flow_connector_pre(#state{connectors = ConnectorsMap}, [_c, {Flow, Connector}]) ->
    FlowConnectors = maps:get(Flow, ConnectorsMap, []),
    lists:member(Connector, FlowConnectors).

pause_flow_connector(C, {Flow, _Connector = #{<<"alias">> := Alias}}) ->
    tremor_flow:pause_connector(Flow, Alias, C).

pause_flow_connector_post(#state{root = Root}, [_C, {_Flow, _Connector = #{<<"alias">> := Alias}}], {ok, Resp = #{<<"alias">> := Alias, <<"status">> := <<"paused">> }}) ->
    {ok, Schema} = jsg_jsonref:deref(["components", "schemas", "connector"], Root),
    jesse_validator:validate(Schema, Root, jsone:encode(Resp));
pause_flow_connector_post(#state{root = Root}, [_C, {_Flow, _Connector = #{<<"status">> := Status}}], {error, 400, ErrorResponse}) when (Status /= <<"running">>) and (Status /= <<"paused">>) ->
    {ok, Schema} = jsg_jsonref:deref(["components", "schemas", "error"], Root),
    jesse_validator:validate(Schema, Root, jsone:encode(ErrorResponse));

pause_flow_connector_post(_, _, Res) ->
    io:format("Error: unexpected pause flow connector body response ~p~n", [Res]),
    false.

pause_flow_connector_next(S = #state{}, _Res, [_C, {_Flow, _Paused = #{<<"status">> := Status} }]) when (Status /= <<"paused">>) and (Status /= <<"running">>) ->
    S;
pause_flow_connector_next(S = #state{connectors = ConnectorsMap}, _Res, [_C, {Flow, Paused}]) ->
    FlowConnectors = maps:get(Flow, ConnectorsMap, []),
    FlowConnectors2 = [maps:put(<<"status">>, <<"paused">>, Paused) | lists:delete(Paused, FlowConnectors)],
    S#state{connectors = maps:put(Flow, FlowConnectors2, ConnectorsMap)}.

%% -----------------------------------------------------------------------------

resume_flow_connector_args(#state{connection = C, connectors = Connectors}) ->
    % filter out flows without connectors
    ConnectorsList = [{FlowAlias, FlowConnectors} || {FlowAlias, FlowConnectors} <- maps:to_list(Connectors), length(FlowConnectors) > 0],
    Gen = case ConnectorsList of
        [] -> noconnector; % this happens when there is no flow with a connector
        ConnectorsList ->
            ?LET({Alias, FlowConnectors}, elements(ConnectorsList), elements([{Alias, Connector} || Connector <- FlowConnectors]))
    end,
    [C, Gen].

resume_flow_connector_pre(#state{connectors = Connectors}) when map_size(Connectors) == 0 ->
    false;
resume_flow_connector_pre(#state{}) ->
    true.

resume_flow_connector_pre(#state{}, [_c, noconnector]) ->
    false;
resume_flow_connector_pre(#state{connectors = ConnectorsMap}, [_c, {Flow, Connector}]) ->
    FlowConnectors = maps:get(Flow, ConnectorsMap, []),
    lists:member(Connector, FlowConnectors).

resume_flow_connector(C, {Flow, _Connector = #{<<"alias">> := Alias}}) ->
    tremor_flow:resume_connector(Flow, Alias, C).

resume_flow_connector_post(#state{root = Root}, [_C, {_Flow, _Connector = #{<<"alias">> := Alias}}], {ok, Resp = #{<<"alias">> := Alias, <<"status">> := <<"running">> }}) ->
    {ok, Schema} = jsg_jsonref:deref(["components", "schemas", "connector"], Root),
    jesse_validator:validate(Schema, Root, jsone:encode(Resp));

resume_flow_connector_post(#state{root = Root}, [_C, {_Flow, _Connector = #{<<"status">> := Status}}], {error, 400, ErrorResponse}) when (Status /= <<"paused">>) and (Status /= <<"running">>) ->
    {ok, Schema} = jsg_jsonref:deref(["components", "schemas", "error"], Root),
    jesse_validator:validate(Schema, Root, jsone:encode(ErrorResponse));
resume_flow_connector_post(_, _, Res) ->
    io:format("Error: unexpected resume flow connector body response ~p", [Res]),
    false.

resume_flow_connector_next(S = #state{}, _Res, [_C, {_Flow, _Resumed = #{<<"status">> := Status} }]) when (Status /= <<"paused">>) and (Status /= <<"running">>) ->
    S;
resume_flow_connector_next(S = #state{connectors = ConnectorsMap}, _Res, [_C, {Flow, Resumed}]) ->
    FlowConnectors = maps:get(Flow, ConnectorsMap, []),
    FlowConnectors2 = [maps:put(<<"status">>, <<"running">>, Resumed) | lists:delete(Resumed, FlowConnectors)],
    S#state{connectors = maps:put(Flow, FlowConnectors2, ConnectorsMap)}.

%% -----------------------------------------------------------------------------
%% Flow Connectors property
%% -----------------------------------------------------------------------------

-spec prop_flow_connectors() -> eqc:property().
prop_flow_connectors() ->
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