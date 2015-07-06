-module(log).

-export([info/1, info/2]).

info(Msg) ->
  info(Msg, []).

info(Fmt, Args) ->
  io:format(Fmt ++ "~n", Args).