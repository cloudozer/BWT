#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ebin deps/lager/ebin deps/goldrush/ebin -setcookie secret_gc -name master@localhost -attached

main([]) ->
  master:test(),
  receive stop -> ok end;

main([SeqFileName, Chromosome, WorkersNumStr]) ->
  WorkersNum = list_to_integer(WorkersNumStr),
  master:test(SeqFileName, Chromosome, WorkersNum),
  receive stop -> ok end.
