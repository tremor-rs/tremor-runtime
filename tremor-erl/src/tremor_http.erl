%% taken from: https://gitlab.com/Project-FiFo/FiFo/fifo_api/blob/master/src/fifo_api_http.erl
-module(tremor_http).

-export([new/1, get/2, get/3, post/3, put/3, url/2,
        delete/2, delete/3, connect/1, close/1, decode/1]).

-export([take_last/1, full_list/1]).

-export_type([connection/0, connection_options/0]).

-record(connection, {
          endpoint = "localhost" :: string(),
          port = 9898 :: pos_integer()
%%          prefix = "api" :: string(),
%%          version = "1" :: string(),
%%          token :: binary() | undefined
         }).

-type connection_options() ::
        {endpoint, string()} |
        {port, pos_integer()}.
%%        {prefix, string()} |
%%        {token, binary()} |
%%        {version, string()}.

-type connection() :: #connection{}.



-ifdef(USE_MSGPACK).
-define(ENCODING,  <<"application/x-msgpack">>).
encode(E) ->
    msgpack:pack(E, [{map_format, jsx}]).
decode(E) ->
    {ok, R} = msgpack:unpack(E, [{map_format, jsx}]),
    R.
-else.
-define(ENCODING,  <<"application/json">>).
encode(E) ->
    jsx:encode(E).
decode(E) ->
    jsx:decode(E, [return_maps]).
-endif.




new(Options) ->
    new(Options, #connection{}).

connect(#connection{endpoint = Endpoint, port = Port}) ->
    {ok, ConnPid} = gun:open(Endpoint, Port),
    {ok, _} = gun:await_up(ConnPid),
    ConnPid.

close(ConnPid) ->
    gun:close(ConnPid).

get(Path, C) ->
    get(Path, [], C).

delete(Path, C) ->
    delete(Path, [], C).

get(Path, Opts, C) ->
    get_(url(Path, C), Opts, C).

get_(URL, C) ->
    get_(URL, [], C).

get_(URL, Opts, C) ->
    ConnPid = connect(C),
    ReqHeaders = [{<<"accept">>, ?ENCODING} | Opts],
    StreamRef = gun:get(ConnPid, URL, ReqHeaders),
    case gun:await(ConnPid, StreamRef) of
        {response, fin, Code, _Hdrs} when Code >= 400 ->
            gun:close(ConnPid),
            {error, Code};
        {response, nofin, Code, _Hdrs} when Code >= 400 ->
            case  gun:await_body(ConnPid, StreamRef) of
                {ok, Body1} ->
                    _Body2 = decode(Body1),
                    gun:close(ConnPid),
                    {error, Code};
                E1 ->
                    gun:close(ConnPid),
                    E1
            end;
        {response, nofin, _Code, _Hdrs} ->
            case  gun:await_body(ConnPid, StreamRef) of
                {ok, Body1} ->
                    Body2 = decode(Body1),
                    gun:close(ConnPid),
                    {ok, Body2};
                E1 ->
                    gun:close(ConnPid),
                    E1
            end;
        E ->
            gun:close(ConnPid),
            E
    end.

delete(Path, Opts, C) ->
    ConnPid = connect(C),
    URL = url(Path, C),
    ReqHeaders = [{<<"accept">>, ?ENCODING} | Opts],
    StreamRef = gun:delete(ConnPid, URL, ReqHeaders),
    case gun:await(ConnPid, StreamRef) of
        {response, fin, Code, _Hdrs} when Code >= 400 ->
            gun:close(ConnPid),
            {error, Code};
        {response, fin, _Status, _Hdrs} ->
            gun:close(ConnPid),
            ok;
        {response, nofin, Code, _Hdrs} when Code >= 400 ->
            case  gun:await_body(ConnPid, StreamRef) of
                {ok, Body1} ->
                    _Body2 = decode(Body1),
                    gun:close(ConnPid),
                    {error, Code};
                E1 ->
                    gun:close(ConnPid),
                    E1
            end;        {response, nofin, _Status, _Hdrs} ->
            case gun:await_body(ConnPid, StreamRef) of
                {ok, Body1} ->
                    Body2 = decode(Body1),
                    gun:close(ConnPid),
                    {ok, Body2};
                E1 ->
                    gun:close(ConnPid),
                    E1
            end;
        E ->
            gun:close(ConnPid),
            E
    end.

post(Path, Body, C) ->
    ConnPid = connect(C),
    URL = url(Path, C),
    ReqHeaders = [{<<"accept">>, ?ENCODING},
                  {<<"content-type">>, ?ENCODING}],
    ReqBody = encode(Body),
    StreamRef = gun:post(ConnPid, URL, ReqHeaders),
    gun:data(ConnPid, StreamRef, fin, ReqBody),
    case gun:await(ConnPid, StreamRef) of
        {response, fin, Code, _Hdrs} when Code >= 400 ->
            gun:close(ConnPid),
            {error, Code};
        {response, fin, 200, _Hdrs}  ->
            gun:close(ConnPid),
            ok;
        {response, fin, 201, _Hdrs}  ->
            gun:close(ConnPid),
            ok;
        {response, _, 204, _Hdrs}  ->
            gun:close(ConnPid),
            ok;
        {response, nofin, Code, _Hdrs} when Code >= 400 ->
            case gun:await_body(ConnPid, StreamRef) of
                {ok, Body1} ->
                    _ = decode(Body1),
                    gun:close(ConnPid),
                    {error, Code};
                E1 ->
                    gun:close(ConnPid),
                    E1
            end;
        {response, nofin, Code, _Hdrs} when Code == 200 orelse Code == 201 ->
            case gun:await_body(ConnPid, StreamRef) of
                {ok, Body1} ->
                    Body2 = decode(Body1),
                    gun:close(ConnPid),
                    {ok, Body2};
                E1 ->
                    gun:close(ConnPid),
                    E1
            end;
        {response, fin, 303, H} ->
            Location = proplists:get_value(<<"location">>, H),
            gun:close(ConnPid),
            get_(Location, C);
        Error ->
            gun:close(ConnPid),
            Error
    end.

put(Path, Body, C) ->
    ConnPid = connect(C),
    URL = url(Path, C),
    ReqHeaders = [{<<"accept">>, ?ENCODING},
                  {<<"content-type">>, ?ENCODING}],
    ReqBody = encode(Body),
    StreamRef = gun:put(ConnPid, URL, ReqHeaders),
    gun:data(ConnPid, StreamRef, fin, ReqBody),
    case gun:await(ConnPid, StreamRef) of
        {response, fin, Code, _Hdrs} when Code >= 400 ->
            gun:close(ConnPid),
            {error, Code};
        {response, fin, 200, _Hdrs}  ->
            gun:close(ConnPid),
            ok;
        {response, _, 204, _Hdrs}  ->
            gun:close(ConnPid),
            ok;
        {response, nofin, 200, _Hdrs} ->
            case gun:await_body(ConnPid, StreamRef) of
                {ok, Body1} ->
                    Body2 = decode(Body1),
                    gun:close(ConnPid),
                    {ok, Body2};
                E1 ->
                    gun:close(ConnPid),
                    E1
            end;
        {response, fin, 303, H} ->
            Location = proplists:get_value(<<"location">>, H),
            gun:close(ConnPid),
            get_(Location, C);
        Error ->
            gun:close(ConnPid),
            Error
    end.

url([$/ | Path], C) ->
    url(Path, C);

url(Path,
    #connection{}) ->
    [$/, Path].

take_last(L) ->
    take_last(L, []).
take_last([E], R) ->
    {lists:reverse(R), E};
take_last([E | R], L) ->
    take_last(R, [E | L]).

full_list(L) ->
    list_to_binary(string:join([to_l(E) || E <- L], ",")).


to_l(E) when is_list(E) ->
    E;
to_l(E) when is_binary(E) ->
    binary_to_list(E).


new([], C) ->
    C;

new([{endpoint, Endpoint} | R], C) when is_list(Endpoint) ->
    new(R, C#connection{endpoint = Endpoint});

new([_ | R], C) ->
    new(R, C).
