#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ebin deps/lager/ebin deps/goldrush/ebin -setcookie secret_gc -name client@localhost -attached

main([MasterIpStr, WorkersNumStr]) ->
  lager:start(),
  MasterPid = {master, list_to_atom("master@" ++ MasterIpStr)},
  WorkersNum = list_to_integer(WorkersNumStr),
  ok = gen_server:call(MasterPid , {run, "bwt_files/SRR770176_1.fastq", "GL000193.1", WorkersNum}),
  receive_cigars().

receive_cigars() ->
  receive
    {cigar, SeqName, Chromosome, Pos, CigarValue, CigarRate, SeqValue} ->
      io:format("~s      ~s      ~b      ~s      ~b      ~s~n", [SeqName, Chromosome, Pos, CigarValue, CigarRate, SeqValue]),
      receive_cigars();
    _ -> throw(not_a_cigar)
  end.