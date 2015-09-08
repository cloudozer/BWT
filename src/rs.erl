% reads source module 
% 
% 
% Cloudozer(c), 2015
%


-module(rs).
-export([
  start/2,
  r_source/8
]).

-define(SINK_CONFIRM_TIMEOUT,10000).
-define(SEQ_FILE_CHUNK_SIZE,200000000).


start(SeqFileName, HttpStorage) ->
	Pid = spawn(fun() ->
		SeqFileNameUrl = HttpStorage ++ "/" ++ SeqFileName,

		{ok,Headers} = http:get_headers(SeqFileNameUrl),
		ContentLength = proplists:get_value(<<"Content-Length">>, Headers),
		{ok,Headers1,Reads} = http:get(SeqFileNameUrl, [{"Range", http:range(0, ?SEQ_FILE_CHUNK_SIZE-1)}]),
		SeqFileChunkSize = proplists:get_value(<<"Content-Length">>, Headers1),
		http:get_async(self(), SeqFileNameUrl, [{"Range", http:range(SeqFileChunkSize, 2*?SEQ_FILE_CHUNK_SIZE-1)}]),

		receive
			{run, Alqs,SFs, SinkRef} ->
StartTime = now(),
				r_source(Reads, <<>>, ContentLength, SeqFileChunkSize, Alqs,SFs,length(SFs),SinkRef),
io:format("Fastq completed within ~p secs.", [timer:now_diff(now(), StartTime) / 1000000])
		end
	end),
	Ref = {navel:get_node(), Pid},
	{ok, Ref}.



produce_workload(N, Fastq) ->
  produce_workload(N, Fastq, []).

produce_workload(0, Fastq, Acc) ->
  {Fastq, Acc};
produce_workload(_Size, <<>>, Acc) ->
  {<<>>, Acc};
produce_workload(Size, Bin, Acc) ->
  try 
  case binary:split(Bin, <<$\n>>) of
  [<<$@, SName/binary>>, Bin1] ->
	  [SData, Bin2] = binary:split(Bin1, <<$\n>>),
	  [<<$+>>, Bin3] = binary:split(Bin2, <<$\n>>),
	  [_Quality, Bin4] = binary:split(Bin3, <<$\n>>),
	  Seq = {SName, SData},
	  produce_workload(Size - 1, Bin4, [Seq | Acc]);
  [Fb, Bin1] ->
io:format("Bin1 ~p~n", [Fb]),
	  produce_workload(Size, Bin1, Acc)
  end
  catch _:_ ->
    {<<>>, Acc}
  end.

r_source(<<>>,<<>>,ContentLength,DownloadedSize,Alqs,SFs,0,Sink) when ContentLength == DownloadedSize ->
	terminate(Alqs,length(SFs)),

	io:format("~n\trs finished fastq distribution and is waiting for confirmation from sink~n"),
	case get_sink_confirmation(Sink) of
		{timeout,Time} -> 
			io:format("No confirmation got from sink during ~psec~n",[Time]),
			shutdown_cluster(Alqs,SFs,Sink);
		Time -> 
			io:format("Source waited for confirmation ~p msec~n",[Time]),
			% request next fastq file
			shutdown_cluster(Alqs,SFs,Sink)
	end;

r_source(<<>>,Reads,ContentLength,DownloadedSize,Alqs,SFs,N,Sink) ->

	r_source(Reads,<<>>,ContentLength,DownloadedSize,Alqs,SFs,N,Sink);

r_source(Reads,ReadsNext,ContentLength,DownloadedSize,Alqs,SFs,0,Sink) ->
  case produce_workload(10000, Reads) of
    {Reads1, []} ->
      r_source(Reads1,ReadsNext,ContentLength,DownloadedSize,Alqs,SFs,0,Sink);
    {Reads1, Batch} ->
      multicast(Batch,SFs),
      r_source(Reads1,ReadsNext,ContentLength,DownloadedSize,Alqs,SFs,length(SFs),Sink)
  end;

r_source(Reads,ReadsNext,ContentLength,DownloadedSize,Alqs,SFs,N,Sink) when N == 1 ->
	receive
		{Pid,ready} -> 
			io:format("ready ~p: ~p~n", [os:timestamp(), Pid]),
			r_source(Reads,ReadsNext,ContentLength,DownloadedSize,Alqs,SFs,N-1,Sink)
	end;
r_source(Reads,ReadsNext,ContentLength,DownloadedSize,Alqs,SFs,N,Sink) ->
	receive
		{Pid,ready} -> 
			r_source(Reads,ReadsNext,ContentLength,DownloadedSize,Alqs,SFs,N-1,Sink)
	end.


terminate(Alqs,0) ->
	% multicast it
	lists:foreach(  fun({ANode,APid})-> navel:call_no_return(ANode, erlang, send, [APid, fastq_done])
					end,Alqs);
terminate(Alqs,J) ->
	terminate(Alqs,J-1).
	

multicast(Batch,SFs) ->
%% 	lists:foreach(fun({_,SF})-> SF ! {data,Batch} end, SFs).  % {_,SF} = {box,pid}
	lists:foreach(fun({Node,SF})-> navel:call_no_return(Node,erlang,send,[SF,{data,Batch}]) end, SFs).  % {_,SF} = {box,pid}



get_sink_confirmation(Sink) ->
	statistics(wall_clock),
	receive
		{Sink,fastq_done} -> {_,T} = statistics(wall_clock), T;
    Err -> throw({Sink,Err})

	after 
		?SINK_CONFIRM_TIMEOUT -> {timeout,?SINK_CONFIRM_TIMEOUT/1000}
	end.



shutdown_cluster(Alqs,SFs,{SinkN,SinkP}) ->
  navel:call_no_return(SinkN,erlang,send,[SinkP,quit]),
	lists:foreach(fun({N,P})-> navel:call_no_return(N,erlang,send,[P,quit]) end, Alqs),
	lists:foreach(fun({N,P})-> navel:call_no_return(N,erlang,send,[P,quit]) end, SFs).


