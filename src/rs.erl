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
	case shedule:chunks_to_box(ChromoLs,length(Boxes),Box_mem) of
		not_enough_memory ->
			io:format("Not enough memory~n");

		Schedule ->
			Sink = sink:start_sink(),
			Schedule1 = alq:start_alq(lists:zip(Boxes,Schedule),Sink), % {Box_id,Alq,Chunk_files}
			{Alqs, SFs} = sf:start_SF(Schedule1),

			Reads = http:get_fastq(),

			r_source(Reads,Alqs,SFs,length(SFs),Sink)
	end.



r_source([Batch|Reads],Alqs,SFs,0,Sink) ->
	multicast(Batch,SFs),
	r_source(Reads,Alqs,SFs,length(SFs),Sink);

r_source([],Alqs,SFs,0,Sink) ->
	lists:foreach(  fun(A)-> A ! fasq_done
					end,Alqs),
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
	lists:foreach(fun(SF)-> SF ! {data,Batch} end, SFs).



get_sink_confirmation(Sink) ->
	statistics(wallclock),
	receive
		{Sink,fastq_done} -> {_,T} = statistics(wallclock), T

	after 
		?SINK_CONFIRM_TIMEOUT -> {timeout,?SINK_CONFIRM_TIMEOUT/1000}
	end.



shutdown_cluster(Alqs,SFs,Sink) ->
	Sink ! quit,
	lists:foreach(fun(A)-> A ! quit end, Alqs),
	lists:foreach(fun(A)-> A ! quit end, SFs).


