#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ebin deps/lager/ebin deps/goldrush/ebin -setcookie secret_gc -name master@localhost -attached

main([]) ->
  process_flag(trap_exit, true),
  master:test_cluster(),
  receive 
    stop -> ok;
    Error -> lager:error("Error: ~p", [Error])
  end.
