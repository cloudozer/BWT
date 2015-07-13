#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ebin apps/source/ebin apps/sink/ebin deps/jsx/ebin -name client@127.0.0.1 -attached

main([]) ->
  io:format("Usage: start_local.sh SeqFileName Chromosome Debug?~n");

main([SeqFileName, ChromosomeList]) ->
  main([SeqFileName, ChromosomeList, "false"]);

main([SeqFileName, ChromosomeListRaw, DebugStr]) ->
  ChromosomeList = lists:foldl(fun(<<>>,A)->A; (B,A)->[binary_to_list(B)|A] end, [], 
    binary:split(list_to_binary(ChromosomeListRaw), [<<",">>, <<"[">>, <<"]">>], [global])), 
  Debug = list_to_atom(DebugStr),
  test(SeqFileName, ChromosomeList, Debug),
  receive
    {stop, Secs} ->
      io:format("It's all over. ~.1f sec.~n", [Secs])
  end.

test(SeqFileName, ChromosomeList, Debug) ->
  if Debug == true ->
    application:start(sasl);
  true ->
    ok
  end,

  %% Start navel
  navel:start(tester),

  %% Start lingd daemon
  {ok, {LingdNode,LingdPid}} = lingd:start_link(),
  LingdRef = {LingdNode,LingdPid},

  %% Start source app
  {ok,{SourceIp,SourcePort}} = lingd:create(LingdRef, source),
  {ok,_} = navel:call(source, source, start_link, []),

  %% Start sink app
  {ok,{SinkIp,SinkPort}} = lingd:create(LingdRef, sink),
  ok = navel:call(sink, application, start, [sink]),
  %% Connect the Sink to the Source
  log:info("lala ~p", [{{SinkIp,SinkPort},SourceIp,SourcePort}]),
  %ok = navel:call(sink, navel, connect, [SourceIp,SourcePort]),
  ok = navel:call(source, navel, connect, [SinkIp,SinkPort]),

  IndexUrl = "http://localhost:8888/fm_indices/index.json",
  inets:start(),
  {ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body}} =
          httpc:request(get, {IndexUrl, []}, [], [{body_format, binary}]),
  LsJson = Body,
FilesList = jsx:decode(LsJson),

BoxNbr = 1,
BoxMem = 4000,
ChunksList = schedule:chunks_to_box(ChromosomeList,lists:map(fun erlang:binary_to_list/1, FilesList),BoxNbr,BoxMem),
log:info("ChunksList ~p=>~p", [{ChromosomeList,BoxNbr,BoxMem},ChunksList]),

%% Workaround for latest schedule:chunks_to_box, remove all source, sink.
ChunksList1 = [lists:filter(fun({source,_})->false; ({sink,_})->false; (_)->true end, Chunks) || Chunks <- ChunksList],

	lists:foreach(fun({source,_})->ignore; ({sink,_})->ignore; (Chunks) ->

	  %% Start workers
	  WorkersNum = length(Chunks),
	  Pids = lists:map(fun(N) ->
	    %% Create a node
	    NodeName = list_to_atom("erl" ++ integer_to_list(N)),
	    {ok, _} = lingd:create(LingdRef, NodeName),

	    %% Start worker app
	    ok = navel:call(NodeName, application, set_env, [worker_bwt_app,base_url,"http://localhost:8888/fm_indices/"]),
	    {ok, WorkerPid} = navel:call(NodeName, worker_bwt, start_link, []),

	    %% Connect the node to the Source and to the Sink
	    ok = navel:call(NodeName, navel, connect, [SourceIp,SourcePort]),
	    ok = navel:call(NodeName, navel, connect, [SinkIp,SinkPort]),
	    {NodeName, WorkerPid}
	  end, lists:seq(0, WorkersNum-1)),

	  %% Associate them with the Source
	  ok = navel:call(source, source, register_workers, [source,Pids])
	end, ChunksList1),

  %% Run everything
  ok = navel:call(source, source, run, [source, SeqFileName, ChunksList1, self(), {sink,sink}]).
