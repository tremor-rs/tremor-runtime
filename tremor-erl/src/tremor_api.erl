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
-module(tremor_api).

%% API exports
-export([start/0, new/0, new/1]).


%%====================================================================
%% API functions
%%====================================================================


start() ->
    application:ensure_all_started(tremor_api).


-spec new(Options :: [tremor_http:con_options()])  ->
                 tremor_http:connection().

new(Options) ->
    tremor_http:new(Options).

new() ->
    new([]).

%%====================================================================
%% Internal functions
%%====================================================================
