#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ebin apps/master/ebin deps/lager/ebin deps/goldrush/ebin -name master@127.0.0.1 -attached

main([]) ->
  io:format("Usage: start_local.sh SeqFileName Chromosome WorkersNumber Debug~n");

main([SeqFileName, Chromosome, WorkersNumStr]) ->
  main([SeqFileName, Chromosome, WorkersNumStr, "false"]);

main([SeqFileName, Chromosome, WorkersNumStr, DebugStr]) ->
  WorkersNum = list_to_integer(WorkersNumStr),
  Debug = list_to_atom(DebugStr),
  master:test(SeqFileName, Chromosome, WorkersNum, Debug),
  receive
    {stop, Secs} ->
      io:format("It's all over. ~.1f sec.~n", [Secs])
  end.
