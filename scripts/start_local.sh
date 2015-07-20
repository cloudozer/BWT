#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ebin apps/source/ebin apps/sink/ebin deps/jsx/ebin -name client@127.0.0.1 -attached

main([]) ->
  io:format("Usage: start_local.sh SeqFileName Chromosome HttpHost Debug?~n");

main([SeqFileName, ChromosomeList, HttpHost]) ->
  main([SeqFileName, ChromosomeList, HttpHost, "[]"]);

main([SeqFileName, ChromosomeListRaw, HttpHost, OptsStr]) ->
  ChromosomeList = lists:foldl(fun(<<>>,A)->A; (B,A)->[binary_to_list(B)|A] end, [], 
    binary:split(list_to_binary(ChromosomeListRaw), [<<",">>, <<"[">>, <<"]">>], [global])), 
  Opts = list_to_term(OptsStr),
  VM = proplists:get_value(vm, Opts, beam),
  Debug = proplists:get_value(debug, Opts, false),
  test(SeqFileName, ChromosomeList, HttpHost, Debug, VM),
  receive
    {stop, Secs} ->
      io:format("It's all over. ~.1f sec.~n", [Secs])
  end.

list_to_term(String) ->
    {ok, T, _} = erl_scan:string(String++"."),
    case erl_parse:parse_term(T) of
        {ok, Term} ->
            Term;
        {error, Error} ->
            Error
    end.

test(SeqFileName, ChromosomeList, HttpHost, Debug, VM) ->
  if Debug == true ->
    application:start(sasl);
  true ->
    ok
  end,

  true = register(tester, self()),

  %% Start navel
  navel_sup:start_link(tester),

  %% Start lingd daemon
  {ok, {LingdNode,LingdPid}} = lingd:start_link(VM),
  LingdRef = {LingdNode,LingdPid},

  %% Start source app
log:info("starting source.."),
  {ok,SourceHost} = lingd:create(LingdRef, source, [{memory,2024}]),
log:info("started source."),
  {ok,_} = navel:call(source, source, start_link, [{tester,tester}]),

  %% Start sink app
  {ok,SinkHost} = lingd:create(LingdRef, sink, [{memory, 1024}]),
log:info("starting sink app"),
  %WTF {error,{"no such file or directory","sink.app"}} ok = navel:call(sink, application, start, [sink]), 
  {ok,_} = navel:call(sink, sink, start_link, []),
log:info("connecting sink to source"),
  %% Connect the Sink to the Source
  ok = navel:call(sink, navel, connect, [SourceHost]),

  IndexUrl = "http://" ++ HttpHost ++ "/fm_indices/index.json",
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
	  Pids = lists:map(fun(Chunk={ChunkName,_}) ->
	    %% Create a node
	    NodeName = list_to_atom("chunk_" ++ lists:filter(fun($.)->false;(_)->true end,ChunkName)),
	    {ok, _} = lingd:create(LingdRef, NodeName, [{memory, 4024}]),
	    %% Start worker app
	    ok = navel:call(NodeName, application, set_env, [worker_bwt_app,base_url,"http://" ++ HttpHost ++ "/fm_indices/"]),
	    {ok, WorkerPid} = navel:call(NodeName, worker_bwt, start_link, [Chunk, {source, source}, {sink,sink}]),

	    %% Connect the node to the Source and to the Sink
	    ok = navel:call(NodeName, navel, connect, [SourceHost]),
	    ok = navel:call(NodeName, navel, connect, [SinkHost]),
	    {NodeName, worker_bwt}
	  end, Chunks),

	  %% Associate them with the Source
	  ok = navel:call(source, source, register_workers, [source,Pids])
	end, ChunksList1),

  SeqFileNameUrl = "http://" ++ filename:join(HttpHost, SeqFileName),

  %% Run everything
  %%ok = navel:call(source, source, run, [source, SeqFileNameUrl, ChunksList1, {sink,sink}]).
  %% {"init terminating in do_boot",{{badmatch,{ok,{'$call_no_return',erlang,send,[tester,workers_ready]}}},[{navel,sort_mail,1,[{file,"src/navel.erl"},{line,156}]}]}}
  navel:call_no_return(source, source, run, [source, SeqFileNameUrl, ChunksList1, {sink,sink}]),


  receive
    sink_done -> 
      log:info("sink_done. destroy instances..."), 
      ok = lingd:destroy(LingdRef),
      halt(0)
  end.
