-module(tremor_binding).

-export([
         example/0,
         list/1,
         find/2,
         publish/2,
         unpublish/2
        ]).

-xref_ignore([
              example/0,
              list/3,
              find/2,
              publish/2,
              unpublish/2
             ]).

-define(ENDPOINT, "binding").



%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec list(tremor_api:connection()) ->
                  {ok, JSON :: binary()}.

list(C) ->
    tremor_http:get(?ENDPOINT, C).


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec find(ID :: binary(), tremor_api:connection()) ->
                  {ok, JSON :: binary()}.

find(UUID, C) ->
    tremor_http:get([?ENDPOINT, $/, UUID], C).



%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec publish(Spec :: map(), tremor_api:connection()) ->
                     {error, duplicate} |
                     {error, bad_content} |
                     ok.

publish(Spec, C) ->
    tremor_http:post([?ENDPOINT], Spec, C).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec unpublish(ID :: map(), tremor_api:connection()) ->
                     {error, duplicate} |
                     {error, bad_content} |
                     ok.

unpublish(Id, C) ->
    tremor_http:delete([?ENDPOINT, $/, Id], [], C).

example() ->
    #{
      id => <<"test">>,
      type => <<"file">>,
      config => #{
                  source => <<"file.txt">>
                 }
     }.
