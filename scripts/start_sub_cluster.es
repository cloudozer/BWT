#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ebin apps/source/ebin apps/sink/ebin deps/jsx/ebin -attached -setcookie secret -rsh ssh

main([]) ->
  io:format("Usage: start_local.sh SeqFileName Chromosome HttpStorage Boxes~n");

main([SeqFileName, ChromosomeList, HttpStorage, Boxes]) ->
  main([SeqFileName, ChromosomeList, HttpStorage, Boxes, "[]"]);

main([SeqFileName, ChromosomeListRaw, HttpStorage, BoxesRaw, OptsStr]) ->
  ChromosomeList = string_to_term(ChromosomeListRaw),
  Boxes = list_to_term(BoxesRaw),
  Opts = list_to_term(OptsStr),
  VM = proplists:get_value(vm, Opts, beam),
  start_subcluster(SeqFileName, ChromosomeList, HttpStorage, VM, Boxes),
  receive
    {stop, Secs} ->
      io:format("It's all over. ~.1f sec.~n", [Secs])
  end.

string_to_term(String) ->
  lists:foldl(fun(<<>>, A) -> A; (B, A) -> [binary_to_list(B) | A] end, [],
    binary:split(list_to_binary(String), [<<",">>, <<"[">>, <<"]">>], [global])).

list_to_term(String) ->
  {ok, T, _} = erl_scan:string(String ++ "."),
  case erl_parse:parse_term(T) of
    {ok, Term} ->
      Term;
    {error, Error} ->
      Error
  end.

start_subcluster(SeqFileName, ChromosomeList, HttpStorage, VM, Boxes = [{_, Host, _}|_]) ->

  Name = list_to_atom("launcher@" ++ atom_to_list(Host)),
  {ok,_} = net_kernel:start([Name, longnames]),

  % TODO: refactor it
  IndexUrl = HttpStorage ++ "/fm_indices/index.json",
  {ok, _, FilesInfoJson} = http:get(IndexUrl),
  FilesInfo = jsx:decode(FilesInfoJson),

  case schedule:chunks_to_box(ChromosomeList,FilesInfo,Boxes) of
    not_enough_memory ->
      io:format("Not enough memory~n"),
      throw(not_enough_memory);

    Schedule ->
      io:format("Schedule: ~p~n", [Schedule]),
      ok = subcluster:start(Boxes, Schedule, SeqFileName, HttpStorage, VM, Host),

      receive_cigars(SeqFileName,ChromosomeList)
  end.

receive_cigars(SeqFileName,ChromosomeList) ->
  receive
    {cigar, SeqName, Chromosome, Pos, CigarValue, CigarRate, RefSeq} ->
      io:format("~s      ~s      ~b      ~s      ~b      ~s~n", [SeqName, Chromosome, Pos, CigarValue, CigarRate, RefSeq]),
      receive_cigars(SeqFileName,ChromosomeList);
    {source_sink_done, Sec, WorkersNum, ReadsNum} ->
      {{Year, Month, Day}, {Hour, Min, Sec1}} = erlang:localtime(),
      StatTemplate = "~nReads: ~p~nReference seq: ~p~nChromosomes: ~p~nReads aligned: ~p~nAlignment completion time: ~.1f sec~nWorkers: ~p~nDate/time: ~4.10.0B-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B~n~n",
      io:format(StatTemplate, [SeqFileName, "human_g1k_v37_decoy.fasta", ChromosomeList, ReadsNum, Sec, WorkersNum, Year, Month, Day, Hour, Min, Sec1]),
      halt(0)
  end.
