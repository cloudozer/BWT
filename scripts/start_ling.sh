#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ebin deps/egator/ebin deps/jsx/ebin

-record(state, {source_ip, sink_ip, chunks}).

main([]) ->
  io:format("Usage: start_cluster.sh Chromosome, FastqFile, MasterIp, WorkersNumber~n"),


  {ok,_} = navel:start(client),
  true = register(client, self()),

%  egator:create(<<"source">>, <<"/home/yatagan/BWT/BWT.img">>, [{memory, 512},{extra, <<"-dhcp -home /BWT -pz /BWT/ebin -eval 'navel:start(source), ok = navel:connect({192,168,56,200}), navel:call_no_return(client, erlang, send, [client, source_came]).'">>}], []),

  starter(#state{}).
 
starter(State) ->
  receive 
    source_came -> 
      log:info("got source"),
      {ok,_SourcePid} = navel:call(source, source, start_link, [{client, client}]),
      {ok,Ifaddrs} = navel:call(source,inet,getifaddrs,[]),
      SourceIp = ip(Ifaddrs),
      egator:create(<<"sink">>, <<"/home/yatagan/BWT/BWT.img">>, [{memory, 512},{extra, <<"-dhcp -home /BWT -pz /BWT/ebin -eval 'navel:start(sink), ok = navel:connect({192,168,56,200}), navel:call_no_return(client, erlang, send, [client, sink_came]).'">>}], []),
      starter(State#state{source_ip = SourceIp});
    sink_came -> 
      log:info("got sink"),
      
{ok,_SinkPid} = navel:call(sink, sink, start_link, []),

      {ok,Ifaddrs} = navel:call(sink,inet,getifaddrs,[]),
      SinkIp = ip(Ifaddrs),	
      navel:call_no_return(sink, erlang, spawn_link, [navel, connect, [State#state.source_ip]]),
      
  IndexUrl = "http://localhost/BWT/fm_indices/index.json",
  inets:start(),
  {ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body}} =
          httpc:request(get, {IndexUrl, []}, [], [{body_format, binary}]),
  LsJson = Body,
FilesList = jsx:decode(LsJson),

BoxNbr = 1,
BoxMem = 4000,
ChromosomeList = ["GL000193.1"],
ChunksList = schedule:chunks_to_box(ChromosomeList,lists:map(fun erlang:binary_to_list/1, FilesList),BoxNbr,BoxMem),
log:info("ChunksList ~p=>~p", [{ChromosomeList,BoxNbr,BoxMem},ChunksList]),


%% Workaround for latest schedule:chunks_to_box, remove all source, sink.
ChunksList1 = [lists:filter(fun({source,_})->false; ({sink,_})->false; (_)->true end, Chunks) || Chunks <- ChunksList],

	lists:foreach(fun({source,_})->ignore; ({sink,_})->ignore; (Chunks) ->

	  %% Start workers
	  WorkersNum = length(Chunks),
	  Pids = lists:map(fun(N) ->
	    %% Create a node
 		NodeNameStr = "worker" ++ integer_to_list(N),
	    NodeName = list_to_atom(NodeNameStr),
	Extra = list_to_binary(io_lib:format("-dhcp -home /BWT -pz /BWT/ebin -eval 'navel:start(~p), ok = navel:connect(~p), ok = application:start(worker_bwt_app), navel:call_no_return(source, source, register_workers, [source, [{navel:get_node(),worker_bwt}]]).'", [NodeName, State#state.source_ip])),
      %egator:create(list_to_binary(NodeNameStr), <<"/home/yatagan/BWT/BWT.img">>, [{memory, 512},{extra, Extra}], []),

ok
	  end, lists:seq(0, WorkersNum-1))
	end, ChunksList1),

	starter(State#state{sink_ip = SinkIp, chunks = ChunksList1});

workers_ready ->
log:info("workers_ready"),


SeqFileName = "http://192.168.56.200/BWT/bwt_files/SRR770176_1.fastq_tiny",
ChunksList1 = State#state.chunks,
log:info("run..."),
ok = navel:call(source, source, run, [source, SeqFileName, ChunksList1, {sink,sink}]),
log:info("runned"),

starter(State);

done ->

egator:destroy(<<"source">>, []), 
egator:destroy(<<"sink">>, []), 
egator:destroy(<<"worker0">>, [])

  end.

ip(Ifaddrs) ->
    case [ X || {If,Props} =X <- Ifaddrs, If =/= "lo", lists:keymember(addr, 1, Props) ] of
	[] -> unassigned;
	[{_,Props}|_] -> proplists:get_value(addr, Props) end.
