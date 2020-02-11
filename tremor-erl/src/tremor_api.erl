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
