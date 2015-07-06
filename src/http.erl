-module(http).

-export([get/1, start_link/0, start_reg_link/0, get_async/1, get_async/2]).
-export([loop/0]).

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
  inets:start(),
  {ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body}} =
    httpc:request(get, {Url, []}, [], [{body_format, binary}]),
  {ok, Body}.

%% Private

loop() ->
  receive
    {get_async, Url, Pid} ->
      Fun = fun()->
        {ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body}} =
          httpc:request(get, {Url, []}, [], [{body_format, binary}]),
        Pid ! {http_response, Body}
      end,
      spawn_link(Fun);
    E ->
      throw(E)
  end.



