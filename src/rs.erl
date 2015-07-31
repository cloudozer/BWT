% reads source module 
% 
% 
% Cloudozer(c), 2015
%


-module(rs).
-export([start_cluster/5,
  r_source/5
		]).

-define(SINK_CONFIRM_TIMEOUT,10000).

-include("bwt.hrl").


start_cluster(Boxes,ChromoLs,SeqFileName,HttpStorage,LingdRef) ->
  % TODO: refactor it
  % box RAM in Gb
  Box_mem = 24,

  %% TODO: add availability test
  IndexUrl = HttpStorage ++ "/fm_indices/index.json",
  {ok, LsJson} = http:get(IndexUrl),
  FilesList = jsx:decode(LsJson),
	case schedule:chunks_to_box(ChromoLs,FilesList,length(Boxes),Box_mem) of
		not_enough_memory ->
			io:format("Not enough memory~n"),
      throw(no_memory);

		Schedule ->
			Schedule1 = lists:zip(Boxes,Schedule),
			io:format("Schedule: ~p~n",[Schedule1]),

      Self = {navel:get_node(),self()},

      %% Start sink app
      {ok,SinkHost} = lingd:create(LingdRef, sink, [{memory, 128}]),
      {_Box,Sink,Schedule2} = navel:call(sink, sk, start_sink, [Schedule1,Self]),
      io:format("Sink started. Sink pid: ~p~nSchedule:~p~n",[Sink,Schedule2]),

      %% Create just one alq
      Schedule3 = alq:start_alq(Schedule2,SinkHost,Sink,LingdRef,HttpStorage), % {Box_id,Alq,Chunk_files}
      io:format("Alq started. New Schedule: ~p~n",[Schedule3]),

      {Alqs, SFs} = sf:start_SF(Schedule3,LingdRef,HttpStorage,Self),
      io:format("SFs started. Alqs & SFs: ~p~n~p~n",[Alqs,SFs]),

      SeqFileNameUrl = HttpStorage ++ "/" ++ SeqFileName,
      {ok,Reads} = http:get(SeqFileNameUrl),

			r_source(Reads,Alqs,SFs,length(SFs),Sink)
	end.

produce_workload(N, Fastq) ->
  produce_workload(N, Fastq, []).

produce_workload(0, Fastq, Acc) ->
  {Fastq, Acc};
produce_workload(_Size, <<>>, Acc) ->
  {<<>>, Acc};
produce_workload(Size, Bin, Acc) ->
  [<<$@, SName/binary>>, Bin1] = binary:split(Bin, <<$\n>>),
  [SData, Bin2] = binary:split(Bin1, <<$\n>>),
  [<<$+>>, Bin3] = binary:split(Bin2, <<$\n>>),
  [_Quality, Bin4] = binary:split(Bin3, <<$\n>>),
  Seq = {SName, SData},
  %% assert
  ?QSEC_LENGTH = size(SData),
  produce_workload(Size - 1, Bin4, [Seq | Acc]).


r_source(<<>>,Alqs,SFs,0,Sink) ->
	% multicast it
	lists:foreach(  fun({_,{_,{_,A}}})-> A ! fastq_done
					end,Alqs),
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

r_source(Reads,Alqs,SFs,0,Sink) ->
  case produce_workload(3, Reads) of
    {Reads1, []} ->
      r_source(Reads1,Alqs,SFs,0,Sink);
    {Reads1, Batch} ->
      multicast(Batch,SFs),
      r_source(Reads1,Alqs,SFs,length(SFs),Sink)
  end;

r_source(Reads,Alqs,SFs,N,Sink) ->
	receive
		{_,ready} -> r_source(Reads,Alqs,SFs,N-1,Sink)
	end.




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
	lists:foreach(fun({_,{_,{N,P}}})-> navel:call_no_return(N,erlang,send,[P,quit]) end, Alqs),
	lists:foreach(fun({N,P})-> navel:call_no_return(N,erlang,send,[P,quit]) end, SFs).


