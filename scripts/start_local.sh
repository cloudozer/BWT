#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ebin apps/source/ebin apps/sink/ebin deps/lager/ebin deps/goldrush/ebin -name client@127.0.0.1 -attached

main([]) ->
  io:format("Usage: start_local.sh SeqFileName Chromosome Debug?~n");

main([SeqFileName, Chromosome]) ->
  main([SeqFileName, Chromosome, "false"]);

main([SeqFileName, Chromosome, DebugStr]) ->
  Debug = list_to_atom(DebugStr),
  source:test(SeqFileName, Chromosome, Debug),
  receive
    {stop, Secs} ->
      io:format("It's all over. ~.1f sec.~n", [Secs])
  end.
