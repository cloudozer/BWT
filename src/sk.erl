% sink module 
% 
% 
% Cloudozer(c), 2015
%


-module(sk).
-export([
  start/2,
	start_sink/2,
	sink/2
]).

start(SourceRef, AlqsNum) ->
	Pid = spawn(?MODULE,sink,[SourceRef,AlqsNum]),
	Ref = {navel:get_node(), Pid},
	{ok, Ref}.

start_sink(Schedule, Source) ->
	New_schedule = remove_source_sink(Schedule),
	start_sink(Schedule,New_schedule,Source).

start_sink([{Box,Ls}|Schedule],New_schedule,Source) ->
	case lists:keyfind(sink,1,Ls) of
		false -> start_sink(Schedule,Source);
		{sink,_} ->
			% call lingd:create()
			Sink = spawn(?MODULE,sink,[Source,length(New_schedule)]),
			{Box,{navel:get_node(),Sink},New_schedule}
	end.



sink(R_source,Alq_nbr) -> 
	{ok,Dev} = file:open("OUT.sam",write),
	sink(R_source,Alq_nbr,Alq_nbr,Dev,0).

sink(R_source={SourceN,SourceP},0,Alq_nbr,Dev,SAM_lines_qty) ->
	io:format("All aligning queues confirmed that they have no more tasks for cigar_makers~n"),
	io:format("~p matches found~n",[SAM_lines_qty]),
	file:close(Dev),
	file:rename("OUT.sam","fastq_OUT.sam"),
	navel:call(SourceN, erlang, send, [SourceP, {{navel:get_node(),self()},fastq_done}]),
	sink(R_source,Alq_nbr);
sink(R_source,J,Alq_nbr,Dev,Acc) ->
	receive
		quit -> ok;

		SAM_lines=[_|_] -> 
			lists:foreach(  fun(Line)-> io:format(Dev,"~p~n",Line)
							end,SAM_lines),
			sink(R_source,J,Alq_nbr,Dev,length(SAM_lines)+Acc);
			
		fastq_done -> 
			% store data obtained or send it farther
			io:format("\tSink: alq has no more tasks for cigar makers~n"),
			sink(R_source,J-1,Alq_nbr,Dev,Acc)
	end.


remove_source_sink(Chunks) -> 
	lists:foldl(fun({Box,Ls},Acc) -> 
		case lists:keydelete(sink,1,lists:keydelete(source,1,Ls)) of
			[] -> Acc;
			Ls1 -> [{Box,Ls1}|Acc]
		end
				end,[],Chunks).
	

