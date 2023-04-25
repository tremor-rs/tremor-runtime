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

-module(tremor_flow).

-define(API_VERSION, "v1").
-define(ENDPOINT, "flows").
-define(APP, "default").
-export([ 
    list/1, 
    get/2, 
    pause/2, 
    resume/2, 
    list_connectors/2, 
    get_connector/3, 
    pause_connector/3, 
    resume_connector/3
]).

%%------------------------------------------------------------------------
%% @doc
%% List all currently deployed flows
%% @end
%%------------------------------------------------------------------------
-spec list(tremor_api:connection()) -> {ok, JSON::binary()}.
list(Conn) ->
    tremor_http:get([?API_VERSION, $/, ?ENDPOINT], Conn).

%%------------------------------------------------------------------------
%% @doc
%% Get a flow identified by Alias
%% @end
%%------------------------------------------------------------------------
-spec get(Alias :: binary(), tremor_api:connection()) -> {ok, JSON::binary()}.
get(Alias, Conn) ->
    tremor_http:get([?API_VERSION, $/, ?ENDPOINT, $/, ?APP, $/, Alias], Conn).


%%------------------------------------------------------------------------
%% @doc
%% Pause a flow
%% @end
%%------------------------------------------------------------------------
-spec pause(Alias :: binary(), tremor_api:connection()) -> {ok, JSON::binary()}.
pause(Alias, Conn) ->
    tremor_http:patch([?API_VERSION, $/, ?ENDPOINT, $/, ?APP, $/, Alias], #{ status => <<"paused">> }, Conn).

%%------------------------------------------------------------------------
%% @doc
%% Resume a flow
%% @end
%%------------------------------------------------------------------------
-spec resume(Alias :: binary(), tremor_api:connection()) -> {ok, JSON::binary()}.
resume(Alias, Conn) ->
    tremor_http:patch([?API_VERSION, $/, ?ENDPOINT, $/, ?APP, $/, Alias], #{ status => <<"running">> }, Conn).


%%------------------------------------------------------------------------
%% @doc
%% Get all the connectors created within the flow identified by `FlowAlias`
%% @end
%%------------------------------------------------------------------------
-spec list_connectors(FlowAlias :: binary(), tremor_api:connection()) -> {ok, JSON::binary()}.
list_connectors(FlowAlias, Conn) ->
    tremor_http:get([?API_VERSION, $/, ?ENDPOINT, $/, ?APP, $/, FlowAlias , $/, <<"connectors">>], Conn).

%%------------------------------------------------------------------------
%% @doc
%% Get the connector identified by `ConnectorAlias` 
%% created within the flow identified by `FlowAlias`
%% @end
%%------------------------------------------------------------------------
-spec get_connector(FlowAlias :: binary(), ConnectorAlias :: binary(), tremor_api:connection()) -> {ok, JSON::binary()}.
get_connector(FlowAlias, ConnectorAlias, Conn) ->
    tremor_http:get([?API_VERSION, $/, ?ENDPOINT, $/, ?APP, $/, FlowAlias, $/, <<"connectors">>, $/, ConnectorAlias], Conn).

%%------------------------------------------------------------------------
%% @doc
%% Pause the connector identified by `ConnectorAlias`
%% created within the flow identified by `FlowAlias`.
%% @end
%%------------------------------------------------------------------------
-spec pause_connector(FlowAlias :: binary(), ConnectorAlias :: binary(), tremor_api:connection()) -> {ok, JSON::binary()}.
pause_connector(FlowAlias, ConnectorAlias, Conn) ->
    tremor_http:patch([?API_VERSION, $/, ?ENDPOINT, $/, ?APP, $/, FlowAlias, $/, <<"connectors">>, $/, ConnectorAlias], #{status => <<"paused">>}, Conn).

%%------------------------------------------------------------------------
%% @doc
%% Resume the connector identified by `ConnectorAlias`
%% created within the flow identified by `FlowAlias`.
%% @end
%%------------------------------------------------------------------------
-spec resume_connector(FlowAlias :: binary(), ConnectorAlias :: binary(), tremor_api:connection()) -> {ok, JSON::binary()}.
resume_connector(FlowAlias, ConnectorAlias, Conn) ->
    tremor_http:patch([?API_VERSION, $/, ?ENDPOINT, $/, ?APP, $/, FlowAlias, $/, <<"connectors">>, $/, ConnectorAlias], #{status => <<"running">>}, Conn).
