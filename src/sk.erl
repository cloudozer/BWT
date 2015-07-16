% sink module 
% 
% 
% Cloudozer(c), 2015
%


-module(sk).
-export([start_sink/1,
		sink/2
		]).


start_sink(Schedule) ->
	New_schedule = remove_source_sink(Schedule),
	start_sink(Schedule,New_schedule).

start_sink([{Box,Ls}|Schedule],New_schedule) ->
	case lists:keyfind(sink,1,Ls) of
		false -> start_sink(Schedule);
		{sink,_} -> 
			Sink = spawn(?MODULE,sink,[self(),length(New_schedule)]),
			{Box,Sink,New_schedule}
	end.



sink(R_source,Alq_nbr) -> sink(R_source,Alq_nbr,Alq_nbr,[]).

sink(R_source,0,Alq_nbr,_Acc) -> 
	io:format("All aligning queues confirmed that they have no more tasks for cigar_makers~n"),
	R_source ! {self(),fastq_done}, sink(R_source,Alq_nbr);
sink(R_source,J,Alq_nbr,Acc) ->
	receive
		quit -> ok;

		{_Read,_Chunk,_Pos,_Score,_CIGAR}=Rec ->
			io:format("Sink: got SAM line~n"),
			sink(R_source,J,Alq_nbr,[Rec|Acc]);
			
		fastq_done -> 
			% store data obtained or send it farther
			io:format("\tSink: alq has no more tasks for cigar makers~n"),
			sink(R_source,J-1,Alq_nbr,Acc)
		
	end.


remove_source_sink(Chunks) -> 
	lists:foldl(fun({Box,Ls},Acc) -> 
		case lists:keydelete(sink,1,lists:keydelete(source,1,Ls)) of
			[] -> Acc;
			Ls1 -> [{Box,Ls1}|Acc]
		end
				end,[],Chunks).
	

