% scheduling tasks 
% 
% Cloudozer(c), 2015
%

-module(schedule).
-export([chunks_to_box/3]).

-define(MEM_PER_CHUNK,200).
-define(FM_INDEX_FOLDER,"fm_indices/").


% returns a list of list of chunks.
chunks_to_box(ChromoLs,Box_nbr,Box_mem) ->
	Chunks = lists:foldl(fun(Chromo,Acc) -> filelib:wildcard(Chromo++"_p*.fm",?FM_INDEX_FOLDER)++Acc
						end, [], ChromoLs),

	ChunkList = lists:sort([ {File,filelib:file_size(?FM_INDEX_FOLDER++File) bsr 20} || File <- Chunks]),

	case lists:sum([ Size || {_,Size} <- ChunkList ]) bsr 10 > Box_nbr*Box_mem of
		true -> not_enough_memory;
		false-> distribute(lists:reverse(ChunkList),lists:duplicate(Box_nbr,[]))
	end.


distribute(ChunkList,Buckets) -> distribute(ChunkList,lists:sort(fun(B1,B2)-> 
				lists:sum([ Size ||{_,Size}<-B1]) < lists:sum([ Size ||{_,Size}<-B2])
																end,Buckets),[]).

distribute([{File,Size}|ChunkList],[B|Buckets],Acc) -> distribute(ChunkList,Buckets,[[{File,Size}|B]|Acc]);
distribute([],Buckets,Acc) -> Buckets ++ Acc;
distribute(ChunkList,[],Acc) -> distribute(ChunkList,Acc).




