#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ebin deps/lager/ebin deps/goldrush/ebin -setcookie secret_gc -name master@localhost -attached

main([]) ->
  master:test_cluster(),
  receive stop -> ok end.
