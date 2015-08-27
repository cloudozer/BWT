-module(http).

-export([get/1, start_link/0, start_reg_link/0, get_async/1, get_async/2]).
-export([loop/0,get_fastq/0, get_fmindex/1, get_refseq/1]).

start_link() ->
  Pid = spawn_link(fun loop/0),
  true = is_pid(Pid),
  {ok, Pid}.

start_reg_link() ->
  {ok, Pid} = start_link(),
  true = register(?MODULE, Pid),
  {ok, Pid}.

get_async(Pid, Url) ->
  Pid ! {get_async, Url, self()}.

get_async(Url) ->
  get_async(?MODULE, Url).

get(Url) ->
  {_Headers, Body} = do_get(Url),
  {ok, Body}.

%% Private

loop() ->
  receive
    {get_async, Url, Pid} ->
      Fun = fun()->
        {_Headers, Body} = do_get(Url),

        Pid ! {http_response, Body}
      end,
      spawn_link(Fun);
    E ->
      throw(E)
  end.

do_get(Url) ->
  {ok, {_Scheme, _UserInfo, Host, Port, Path, Query}} = http_uri:parse(Url),
  {ok, Sock} = gen_tcp:connect(Host, Port, [binary,{active,false}]),
  ok = gen_tcp:send(Sock, "GET " ++ Path++Query ++ " HTTP/1.1\r\nHost: " ++ Host ++ "\r\n\r\n"),
  {ok, Headers, Body} = do_recv(Sock),
  ok = gen_tcp:close(Sock),
  {Headers, Body}.

%% HTTP/1.1 200 OK\r\nContent-Length: 3301\r\nAccept-Ranges: bytes\r\nServer: TwistedWeb/15.3.0\r\nLast-Modified: Thu, 06 Aug 2015 12:04:51 GMT\r\nDate: Fri, 14 Aug 2015 12:47:43 GMT\r\nContent-Type: application/json\r\n\r\n

do_recv(Sock) ->
  recv_headers(Sock, <<"">>).

recv_headers(Sock, Headers) ->
  case binary:split(Headers, <<"\r\n\r\n">>) of
    [Headers1, Body] ->
      OK = <<"200 OK">>,
      [<<_HttpVer:9/binary, OK/binary >> | Headers2] = binary:split(Headers1, <<"\r\n">>, [global]),
      Headers3 = lists:map(fun(B)->[K,V]=binary:split(B, <<": ">>), {K,V} end, Headers2),
      ContentLength = binary_to_integer(proplists:get_value(<<"Content-Length">>, Headers3)),
      {ok, Body1} = recv_body(Sock, [Body], ContentLength-size(Body)),
      {ok, <<"">>, Body1};
    [Headers] ->
      case gen_tcp:recv(Sock, 0) of
        {ok, B} ->
          recv_headers(Sock, << Headers/binary, B/binary >>);
      {error, closed} ->
        throw({recv_headers, {error, closed}})
      end
  end.

recv_body(_Sock, Bs, 0) ->
  {ok, list_to_binary(lists:reverse(Bs))};
recv_body(Sock, Bs, ReadMore) when ReadMore > 0 ->
  {ok, B} = gen_tcp:recv(Sock, 0),
  recv_body(Sock, [B | Bs], ReadMore-size(B)).

% gets next fastq file somewhere
get_fastq() ->
  [batch1,batch2,batch3,batch4,batch5].
  

get_fmindex(Chunk) -> Chunk.


get_refseq(Chunks) -> Chunks.

