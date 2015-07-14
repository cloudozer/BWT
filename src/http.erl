-module(http).

-export([get/1, start_link/0, start_reg_link/0, get_async/1, get_async/2]).
-export([loop/0,get_fastq/0, get_fmindex/1]).

start_link() ->
  inets:start(),
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
  inets:start(), %% TODO: do something
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

%% do_get(Url) ->
%%   {ok, {_Scheme, _UserInfo, Host, Port, Path, _Query}} = http_uri:parse(Url),
%%   {ok, Sock} = gen_tcp:connect(Host, Port, [binary,{active,false}]),
%%   ok = gen_tcp:send(Sock, "GET " ++ Path ++ " HTTP/1.1\n\rHost: " ++ Host ++ "\r\n\r\n"),
%%   {ok, Bin} = do_recv(Sock, []),
%%   ok = gen_tcp:close(Sock),
%%   [Headers, Body] = binary:split(Bin, <<"\r\n\r\n">>),
%%   {Headers, Body}.

do_get(Url) ->
  {ok, {{_Version, 200, _ReasonPhrase}, Headers, Body}} =
    httpc:request(get, {Url, []}, [], [{body_format, binary}]),
  {Headers, Body}.


%% do_recv(Sock, Bs) ->
%%   case gen_tcp:recv(Sock, 0) of
%%     {ok, B} ->
%%       do_recv(Sock, [B | Bs]);
%%     {error, closed} ->
%%       {ok, list_to_binary(lists:reverse(Bs))}
%%   end.



% gets next fastq file somewhere
get_fastq() ->
  [batch1,batch2,batch3,batch4,batch5].
  

get_fmindex(Chunk) -> Chunk.


