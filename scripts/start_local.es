#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ebin apps/source/ebin apps/sink/ebin deps/jsx/ebin -name client@127.0.0.1 -attached

main([]) ->
  io:format("Usage: start_local.sh SeqFileName Chromosome HttpStorage Boxes~n");

main([SeqFileName, ChromosomeList, HttpStorage, Boxes]) ->
  main([SeqFileName, ChromosomeList, HttpStorage, Boxes, "[]"]);

main([SeqFileName, ChromosomeListRaw, HttpStorage, BoxesRaw, OptsStr]) ->
  ChromosomeList = string_to_term(ChromosomeListRaw),
  Boxes = string_to_term(BoxesRaw),
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

start_subcluster(SeqFileName, ChromosomeList, HttpStorage, VM, Boxes) ->

  %% Start local navel
  navel_sup:start_link(launcher),
  true = register(launcher, self()),

  %% Start lingd daemon
  {ok, {LingdNode, LingdPid}} = lingd:start_link(VM),
  LingdRef = {LingdNode, LingdPid},

  {ok,SourceHost} = lingd:create(LingdRef, source, [{memory, 128}]),
  ok = navel:call(source,lingd,connect,[]),
  navel:call_no_return(source, erlang, spawn, [rs,start_cluster,[Boxes,ChromosomeList,SeqFileName,HttpStorage,LingdRef]]),

  receive xz -> ok end,

  throw(enough),

  % TODO: refactor it
  % box RAM in Gb
  Box_mem = 24,

  %% TODO: add availability test
  IndexUrl = HttpStorage ++ "/fm_indices/index.json",
  {ok, LsJson} = http:get(IndexUrl),
  FilesList = jsx:decode(LsJson),

  case schedule:chunks_to_box(ChromosomeList, FilesList, length(Boxes), Box_mem) of
    not_enough_memory ->
      io:format("Not enough memory~n"),
      throw(no_memory);

    Schedule ->





      Schedule1 = lists:zip(Boxes, Schedule),
      io:format("Schedule: ~p~n", [Schedule1]),


      %% Start sink app
      {ok,SinkHost} = lingd:create(LingdRef, sink, [{memory, 128}]),
      {_Box,Sink,Schedule2} = navel:call(sink, sk, start_sink, [Schedule1,{source,source}]),
      io:format("Sink started. Sink pid: ~p~nSchedule:~p~n",[Sink,Schedule2]),

      %% Connect the Sink to the Source
%%       ok = navel:call(sink, navel, connect, [SourceHost]),

      %% Create just one alq
      Schedule3 = alq:start_alq(Schedule2,Sink,LingdRef), % {Box_id,Alq,Chunk_files}
      io:format("Alq started. New Schedule: ~p~n",[Schedule3]),

      {Alqs, SFs} = sf:start_SF(Schedule3,LingdRef,HttpStorage),
      io:format("SFs started. Alqs & SFs: ~p~n~p~n",[Alqs,SFs]),

      SeqFileNameUrl = HttpStorage ++ "/" ++ SeqFileName,
      {ok,Reads} = http:get(SeqFileNameUrl),

      {ok,SourceHost} = lingd:create(LingdRef, source, [{memory, 128}]),
%%       rs:r_source(Reads,Alqs,SFs,length(SFs),Sink),
      P = navel:call(source, erlang, spawn, [rs,r_source,[Reads,Alqs,SFs,length(SFs),Sink]]),
      true = navel:call(source, erlang,register,[source,P]),



throw(enough),

      log:info("Create instances..."),

%% Start source app
      {ok, SourceHost} = lingd:create(LingdRef, source, [{memory, 2048}]),
      {ok, _} = navel:call(source, source, start_link, [{launcher, launcher}]),


%%       IndexUrl = HttpStorage ++ "/fm_indices/index.json",
%%       {ok, LsJson} = http:get(IndexUrl),
%%       FilesList = jsx:decode(LsJson),

      BoxNbr = 1,
      BoxMem = 4000,
      ChunksList = schedule:chunks_to_box(ChromosomeList, lists:map(fun erlang:binary_to_list/1, FilesList), BoxNbr, BoxMem),
%log:info("ChunksList ~p=>~p", [{ChromosomeList,BoxNbr,BoxMem},ChunksList]),

%% Workaround for latest schedule:chunks_to_box, remove all source, sink.
      ChunksList1 = [lists:filter(fun({source, _}) -> false; ({sink, _}) -> false; (_) ->
        true end, Chunks) || Chunks <- ChunksList],

      lists:foreach(fun({source, _}) -> ignore; ({sink, _}) -> ignore; (Chunks) ->
%% Start workers
        Pids = lists:map(fun(Chunk = {ChunkName, _}) ->
%% Create a node
          NodeName = list_to_atom("chunk_" ++ lists:filter(fun($.) -> false;(_) -> true end, ChunkName)),
          {ok, _} = lingd:create(LingdRef, NodeName, [{memory, 2048}]),
%% Start worker app
          ok = navel:call(NodeName, application, set_env, [worker_bwt_app, base_url, HttpStorage ++ "/fm_indices/"]),
          {ok, _WorkerPid} = navel:call(NodeName, worker_bwt, start_link, [Chunk, {source, source}, {sink, sink}]),

%% Connect the node to the Source and to the Sink
          ok = navel:call(NodeName, navel, connect, [SourceHost]),
          ok = navel:call(NodeName, navel, connect, [SinkHost]),
          {NodeName, worker_bwt}
        end, Chunks),

%% Associate them with the Source
        ok = navel:call(source, source, register_workers, [source, Pids])
      end, ChunksList1),

      SeqFileNameUrl = filename:join(HttpStorage, SeqFileName),

%% Run everything
%%ok = navel:call(source, source, run, [source, SeqFileNameUrl, ChunksList1, {sink,sink}]).
%% {"init terminating in do_boot",{{badmatch,{ok,{'$call_no_return',erlang,send,[launcher,workers_ready]}}},[{navel,sort_mail,1,[{file,"src/navel.erl"},{line,156}]}]}}
      navel:call_no_return(source, source, run, [source, SeqFileNameUrl, ChunksList1, {sink, sink}]),

      Wait = fun Wait() ->
        receive
          {cigar, SeqName, Chromosome, Pos, CigarValue, CigarRate, RefSeq} ->
            io:format("~s      ~s      ~b      ~s      ~b      ~s~n", [SeqName, Chromosome, Pos, CigarValue, CigarRate, RefSeq]),
            Wait();
          {source_sink_done, Sec, WorkersNum, ReadsNum} ->
            {{Year, Month, Day}, {Hour, Min, Sec1}} = erlang:localtime(),
            StatTemplate = "~nReads: ~p~nReference seq: ~p~nChromosomes: ~p~nReads aligned: ~p~nAlignment completion time: ~.1f sec~nWorkers: ~p~nDate/time: ~4.10.0B-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B~n~n",
            io:format(StatTemplate, [SeqFileName, "human_g1k_v37_decoy.fasta", ChromosomeList, ReadsNum, Sec, WorkersNum, Year, Month, Day, Hour, Min, Sec1]),
            ok = lingd:destroy(LingdRef),
            halt(0)
        end
      end,
      Wait()


  end.
