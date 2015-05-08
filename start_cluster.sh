#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ebin deps/lager/ebin deps/goldrush/ebin -setcookie secret_gc -name client@localhost -attached

main([]) ->
  io:format("Usage: start_cluster.sh Chromosome, FastqFile, MasterIp, WorkersNumber~n");

main([MasterIpStr, WorkersNumStr]) ->
  main(["GL000193.1", "bwt_files/SRR770176_1.fastq", MasterIpStr, WorkersNumStr]);

main([Chromosome, FastqFile, MasterIpStr, WorkersNumStr]) ->
  MasterPid = rpc:call(list_to_atom("master@" ++ MasterIpStr), erlang, whereis, [master]),
  WorkersNum = list_to_integer(WorkersNumStr),
  process_flag(trap_exit, true),
  true = link(MasterPid),
  ok = gen_server:call(MasterPid , {run, FastqFile, Chromosome, WorkersNum}),
  receive_cigars().

receive_cigars() ->
  receive
    {cigar, SeqName, Chromosome, Pos, CigarValue, CigarRate, RefSeq} ->
      io:format("~s      ~s      ~b      ~s      ~b      ~s~n", [SeqName, Chromosome, Pos, CigarValue, CigarRate, RefSeq]),
      receive_cigars();
    {stop, Secs} ->
      io:format("It's all over. ~.1f sec.~n", [Secs]);
    Error -> io:format("Error: ~p~n", [Error])
  end.