#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ebin deps/lager/ebin deps/goldrush/ebin -setcookie secret_gc -name client@localhost -attached

main([MasterIpStr, WorkersNumStr]) ->
  main(["GL000193.1", MasterIpStr, WorkersNumStr]);

main([Chromosome, MasterIpStr, WorkersNumStr]) ->
  MasterPid = rpc:call(list_to_atom("master@" ++ MasterIpStr), erlang, whereis,[master]),
  WorkersNum = list_to_integer(WorkersNumStr),
  process_flag(trap_exit, true),
  true = link(MasterPid),
  ok = gen_server:call(MasterPid , {run, "bwt_files/SRR770176_1.fastq", Chromosome, WorkersNum}),
  receive_cigars().

receive_cigars() ->
  receive
    {cigar, SeqName, Chromosome, Pos, CigarValue, CigarRate, RefSeq} ->
      io:format("~s      ~s      ~b      ~s      ~b      ~s~n", [SeqName, Chromosome, Pos, CigarValue, CigarRate, RefSeq]),
      receive_cigars();

    Error -> io:format("Error: ~p~n", [Error])
  end.