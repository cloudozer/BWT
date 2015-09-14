-module(http).

-export([get/1, get/2, get_headers/1, get_async/1, get_async/2]).
-export([get_fastq/0, get_fmindex/1, get_refseq/1]).
-export([range/2]).

get_async(Url) ->
  get_async(Url, []).

get_async(Url, Headers) ->
  Pid = self(),
  spawn_link(fun() ->
    {ok, Headers1, Body} = get(Url, Headers),
    Pid ! {got_async, Headers1, Body}
  end).

get(Url) ->
  get(Url, []).

get(Url, Headers) ->
  {ok, Sock} = do_request(Url, Headers),
  {ok, Headers1, Body} = do_recv(Sock),
  ok = gen_tcp:close(Sock),
  {ok, Headers1, Body}.

get_headers(Url) ->
  {ok, Sock} = do_request(Url, []),
  {ok, Headers} = do_recv_headers(Sock),
  ok = gen_tcp:close(Sock),
  {ok, Headers}.

%% Private

do_request(Url, Headers) ->
  {ok, {_Scheme, _UserInfo, Host, Port, Path, Query}} = http_uri:parse(Url),
  {ok, Sock} = gen_tcp:connect(Host, Port, [binary,{active,false}]),
  Headers1 = lists:foldr(fun({N, V}, Acc) when is_list(N), is_list(V) -> "\r\n" ++ N ++ ": " ++ V ++ Acc end, "", Headers),
  ok = gen_tcp:send(Sock, "GET " ++ Path++Query ++ " HTTP/1.1\r\nHost: " ++ Host ++ Headers1 ++ "\r\n\r\n"),
  {ok, Sock}.

%% HTTP/1.1 200 OK\r\nContent-Length: 3301\r\nAccept-Ranges: bytes\r\nServer: TwistedWeb/15.3.0\r\nLast-Modified: Thu, 06 Aug 2015 12:04:51 GMT\r\nDate: Fri, 14 Aug 2015 12:47:43 GMT\r\nContent-Type: application/json\r\n\r\n

do_recv(Sock) ->
  {Headers, Sock, Binaries = [Body]} = recv_headers(Sock, <<"">>),
  ContentLength = binary_to_integer(proplists:get_value(<<"Content-Length">>, Headers)),
  Size = ContentLength-size(Body),

  {ok, Body1} = recv_body(Sock, Binaries, Size),
  {ok, Headers, Body1}.

do_recv_headers(Sock) ->
  {Headers, Sock, _Binaries} = recv_headers(Sock, <<"">>),
  {ok, Headers}.

recv_headers(Sock, Headers) ->
  case binary:split(Headers, <<"\r\n\r\n">>) of
    [Headers1, Body] ->

      [<<_HttpVer:9/binary, StatusCodeBin:3/binary, _StatusMsg/binary >> | Headers2] = binary:split(Headers1, <<"\r\n">>, [global]),
      StatusCode = binary_to_integer(StatusCodeBin),
      %% if not 2**
      if ( StatusCode div 100 ) =/= 2 ->
        throw({http_get, StatusCode});
        true -> ok
      end,
      Headers3 = lists:map(fun(B)->[K,V]=binary:split(B, <<": ">>), {K,V} end, Headers2),
      {Headers3, Sock, [Body]};
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


range(Start, End) ->
  io_lib:format("bytes=~b-~b", [Start, End]).

