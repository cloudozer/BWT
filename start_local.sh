#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ebin deps/lager/ebin deps/goldrush/ebin -setcookie secret_gc -name master@localhost -attached

main([]) ->
  master:test(),
  receive stop -> ok end;

main([SeqFileName, Chromosome, WorkersNumStr]) ->
  main([SeqFileName, Chromosome, WorkersNumStr, "false"]);

main([SeqFileName, Chromosome, WorkersNumStr, DebugStr]) ->
  WorkersNum = list_to_integer(WorkersNumStr),
  Debug = list_to_atom(DebugStr),
  master:test(SeqFileName, Chromosome, WorkersNum, Debug),
  receive stop -> ok end.
