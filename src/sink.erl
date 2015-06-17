-module(sink).
-behaviour(gen_server).
-export([init/1]).
-export([start_link/0]).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {}, []).

init(_Args) ->
  {ok, {}}.
