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

  {LocalIP, LingdPort, SourcePort, SinkPort, WorkerStartPort} = {'127.0.0.1', 10, 20, 30, 40},

  %% Start navel
  navel:start(tester),

  %% Start lingd daemon
  {ok, {LingdNode,LingdPid}} = lingd:start_link(LocalIP, LingdPort),
  LingdRef = {LingdNode,LingdPid},

  %% Start source app
  {ok, SourceNode} = lingd:create_link(LingdRef, source, {LocalIP, SourcePort}),
  lingd:connect(LingdRef, SourceNode, LocalIP),
  {ok,_} = navel:call(SourceNode, source, start_link, []),

  %% Start sink app
  {ok, SinkNode} = lingd:create_link(LingdRef, sink, {LocalIP, SinkPort}),
  lingd:connect(LingdRef, SinkNode, LocalIP),
  ok = navel:call(SinkNode, application, start, [sink]),
  %% Connect the Sink to the Source
  lingd:connect(LingdRef, SinkNode, {LocalIP,SourcePort}),

  LsUrl = "http://localhost:8889/ls.json",
  inets:start(),
  {ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body}} =
          httpc:request(get, {LsUrl, []}, [], [{body_format, binary}]),
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
	    {ok, Node} = lingd:create_link(LingdRef, NodeName, {LocalIP, N}),

	    %% Start worker app (connect it firstly)
	    lingd:connect(LingdRef, Node, LocalIP),
	    ok = navel:call(Node, application, set_env, [worker_bwt_app,base_url,"http://localhost:8889/"]),
	    {ok, WorkerPid} = navel:call(Node, worker_bwt, start_link, []),

	    %% Connect the node to the Source and to the Sink
	    lingd:connect(LingdRef, Node, {LocalIP, SourcePort}),
	    lingd:connect(LingdRef, Node, {LocalIP, SinkPort}),
	    {NodeName, WorkerPid}
	  end, lists:seq(WorkerStartPort, WorkerStartPort+WorkersNum-1)),

	  %% Associate them with the Source
	  ok = navel:call(SourceNode, source, register_workers, [source,Pids])
	end, ChunksList1),

  %% Run everything
  ok = navel:call(SourceNode, source, run, [source, SeqFileName, ChunksList1, self(), {SinkNode,sink}]).
