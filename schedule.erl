% scheduling tasks 
% 
% Cloudozer(c), 2014
%

-module(schedule).
-export([get_schedule/3]).

-define(CHUNK_SIZE,2000).
-define(QSEQ_SIZE,100).

% returns the list of lists that contain tuples (start_point, end_point)
% each list is a schedule for a compute node
get_schedule(Index_file, Chunk_size, Nodes_nbr) ->
	case file:open(Index_file,read) of
		{ok,Dev} ->
			{Workload,[{Pos,Len}|Partitions]} = get_average_workload_and_parts(0, [], Dev, Chunk_size, Nodes_nbr),
			file:close(Dev),
			io:format("Average workload=~.2g~n",[Workload]),
			distribute_parts([], [], 0, Workload, Pos,Len,Partitions, Chunk_size);
			
		{error,Reason} ->
			io:format("index file: '~s' cannot be opened~n~p~n",[Index_file,Reason]),
			error
	end.



get_average_workload_and_parts(Sum, Acc, Dev, Chunk_size, Nodes_nbr) ->
	case file:read_line(Dev) of
		eof -> 
			{Sum / Nodes_nbr, lists:reverse(Acc)};

		{ok,Data} -> 
			[_,Pos,Len]=string:tokens(Data," \n"),
			get_average_workload_and_parts(Sum+list_to_integer(Len), 
				[{list_to_integer(Pos),list_to_integer(Len)}|Acc], Dev, Chunk_size, Nodes_nbr)
	end.



distribute_parts(Acc, Ls, Curr_workload, Avg_workload, 
	Curr_pos, Rest, Partitions, Chunk_size) when Curr_workload >= Avg_workload ->
	distribute_parts([Ls|Acc], [], 0, Avg_workload, Curr_pos, Rest, Partitions, Chunk_size);

distribute_parts(Acc, Ls, Curr_workload, Avg_workload, 
	Curr_pos, Rest, Partitions, Chunk_size) when Rest >= 1.5 * ?CHUNK_SIZE ->
	distribute_parts(Acc, [{Curr_pos,?CHUNK_SIZE}|Ls], Curr_workload+?CHUNK_SIZE-?QSEQ_SIZE, 
		Avg_workload, Curr_pos+?CHUNK_SIZE-?QSEQ_SIZE, Rest-?CHUNK_SIZE+?QSEQ_SIZE, Partitions, Chunk_size);

distribute_parts(Acc, Ls, Curr_workload, Avg_workload, Curr_pos, Rest, [{Pos,Len}|Partitions], Chunk_size) ->
	distribute_parts(Acc, [{Curr_pos,Rest}|Ls], Curr_workload+Rest, 
		Avg_workload, Pos, Len, Partitions, Chunk_size);

distribute_parts(Acc, Ls, _Curr_workload, _Avg_workload, Curr_pos, Rest, [], _Chunk_size) ->
	[lists:reverse([{Curr_pos,Rest}|Ls])|Acc].


