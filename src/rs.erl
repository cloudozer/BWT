% reads source module 
% 
% 
% Cloudozer(c), 2015
%


-module(rs).
-export([start_cluster/2
		]).

-define(SINK_CONFIRM_TIMEOUT,10000).


start_cluster(Boxes,ChromoLs) ->
	Box_mem = 24, % box RAM in Gb
	case schedule:chunks_to_box(ChromoLs,length(Boxes),Box_mem) of
		not_enough_memory ->
			io:format("Not enough memory~n");

		Schedule ->
			Schedule1 = lists:zip(Boxes,Schedule),
			io:format("Schedule: ~p~n",[Schedule1]),
			{_Box,Sink,Schedule2} = sk:start_sink(Schedule1),
			io:format("Sink started. Sink pid: ~p~nSchedule:~p~n",[Sink,Schedule2]),
			Schedule3 = alq:start_alq(Schedule2,Sink), % {Box_id,Alq,Chunk_files}
			io:format("Alq started. New Schedule: ~p~n",[Schedule3]),
			{Alqs, SFs} = sf:start_SF(Schedule3),
			io:format("SFs started. Alqs & SFs: ~p~n~p~n",[Alqs,SFs]),

			Reads = http:get_fastq(),

			r_source(Reads,Alqs,SFs,length(SFs),Sink)
	end.



r_source([Batch|Reads],Alqs,SFs,0,Sink) ->
	multicast(Batch,SFs),
	r_source(Reads,Alqs,SFs,length(SFs),Sink);

r_source([],Alqs,SFs,0,Sink) ->
	lists:foreach(  fun(A)-> A ! fastq_done
					end,Alqs),
	io:format("~n\trs finished fastq distribution and is waiting for confirmation from sink~n"),
	case get_sink_confirmation(Sink) of
		{timeout,Time} -> 
			io:format("No confirmation got from sink during ~psec~n",[Time]),
			shutdown_cluster(Alqs,SFs,Sink);
		Time -> 
			io:format("Source waited for confirmation ~p msec~n",[Time]),
			shutdown_cluster(Alqs,SFs,Sink)
	end;

r_source(Reads,Alqs,SFs,N,Sink) ->
	receive
		{_,ready} -> r_source(Reads,Alqs,SFs,N-1,Sink)
	end.




multicast(Batch,SFs) ->
	lists:foreach(fun({_,SF})-> SF ! {data,Batch} end, SFs).  % {_,SF} = {box,pid}



get_sink_confirmation(Sink) ->
	statistics(wall_clock),
	receive
		{Sink,fastq_done} -> {_,T} = statistics(wall_clock), T

	after 
		?SINK_CONFIRM_TIMEOUT -> {timeout,?SINK_CONFIRM_TIMEOUT/1000}
	end.



shutdown_cluster(Alqs,SFs,Sink) ->
	Sink ! quit,
	lists:foreach(fun(A)-> A ! quit end, Alqs),
	lists:foreach(fun({_,A})-> A ! quit end, SFs).


