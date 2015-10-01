% scheduling tasks 
% 
% Cloudozer(c), 2015
%

-module(schedule).
-export([chunks_to_box/3, c2b/0]).
-export([pop_subcluster_element/2]).

-include_lib("eunit/include/eunit.hrl").

-define(EXTRA_MEM_PER_CHUNK,200).
-define(SOURCE_MEM,500).
-define(SINK_MEM,300).


%% returns a list of chunks distributed among boxes [ {box,[list_of_chunks]}, ... ]
c2b() ->
	ChunkList = [{source,600},{sink,400},
				{ch1,700},{ch2,700},{ch3,700},
				{ch4,760},{ch5,760},{ch6,760},{ch7,760},
				{ch8,770},{ch9,770},{ch10,770},{ch11,770},{ch12,770},
				{ch13,790},{ch14,790},{ch15,790},{ch16,790},{ch17,790},{ch18,790},
				{ch19,800},{ch20,800},{ch21,800},{ch22,800}],
	Boxes = [{box1,4,24},{box2,8,40},{box3,8,46}],

	CPC = length(ChunkList)/lists:sum([ Cores ||{_,Cores,_} <- Boxes ]),
	io:format("CPC: ~p~n",[CPC]),

	MPCs = lists:keysort(2,[ {Name,M/C} || {Name,C,M} <- Boxes ]),
	{Boxes_down,Boxes_up} = lists:split(round(length(MPCs) / 2), [ Name || {Name,_} <- MPCs]),
	%io:format("Boxes_up: ~p~nBoxes_down: ~p~n",[Boxes_up,Boxes_down]),

	Min_bucket = [ {Name,ceiling(CPC*Cores),Cores,Mem} || {Name,Cores,Mem} <- Boxes, lists:member(Name,Boxes_up)],
	Max_bucket = [ {Name,trunc(CPC*Cores),Cores,Mem} || {Name,Cores,Mem} <- Boxes, lists:member(Name,Boxes_down)],

	%io:format("Min boxes: ~p~nMax boxes: ~p~n",[Min_bucket,Max_bucket]),
	{MinB1,MaxB1} = adjust(length(ChunkList),Min_bucket,Max_bucket),
	%io:format("Min boxes: ~p~nMax boxes: ~p~n",[MinB1,MaxB1]),

	{Small_chunks,Large_chunks} = lists:split(lists:sum([Q || {_,Q,_,_}<-MaxB1]),lists:keysort(2,ChunkList)),
	%io:format("Small chunks: ~p~nLarge chunks: ~p~n",[Small_chunks,Large_chunks]),

	Total_chunk_sizeS = lists:sum([ S || {_,S} <- Small_chunks]),
	Total_mem_sizeS = lists:sum([ M || {_,_,_,M} <- MaxB1]),
	Chunk_size_per_boxS = [ {Total_chunk_sizeS*M/Total_mem_sizeS/Q,N,Q} || {N,Q,_,M} <- MaxB1],
	%io:format("Average chunk size per box: ~n~p~n",[Chunk_size_per_boxS]),

	Total_chunk_sizeL = lists:sum([ S || {_,S} <- Large_chunks]),
	Total_mem_sizeL = lists:sum([ M || {_,_,_,M} <- MinB1]),
	Chunk_size_per_boxL = [ {Total_chunk_sizeL*M/Total_mem_sizeL/Q,N,Q} || {N,Q,_,M} <- MinB1],
	%io:format("Average chunk size per box: ~n~p~n",[Chunk_size_per_boxL]),

	spread2boxes(Small_chunks,lists:sort(Chunk_size_per_boxS))++
	spread2boxes(Large_chunks,lists:sort(Chunk_size_per_boxL)).



spread2boxes(Chunks,Boxes) ->
	{[],Res} =
	lists:foldl(fun({Av,N,Q},{Chunks1,Distributed})->
				{Selection,Chunks2} = choose_chunks(Av,Q,Chunks1),
				{Chunks2,[{N,Selection}|Distributed]}
				end,{Chunks,[]},Boxes),
	Res.


choose_chunks(Av,Q,Chunks) -> choose_chunks(Av,Q,Chunks,[]).

choose_chunks(_,0,Chunks,Selected) -> {Selected,Chunks};
choose_chunks(Av,1,Chunks,Selected) -> 
	{Best,Chunks1} = get_best(Av,Chunks),
	{[Best|Selected],Chunks1};
choose_chunks(Av,Q,Chunks,Selected) ->
	[{F,Sf}|Chunks1] = Chunks,
	{L,Sl} = lists:last(Chunks),
	case Sf+Sl > Av of
		true -> 
			{Best,Chunks2} = get_best(Av,Chunks1),
			choose_chunks(Av,Q-2,Chunks2,[{F,Sf},Best|Selected]);
		false->
			Chunks2 = lists:droplast(Chunks),
			{Best,Chunks3} = get_best(Av,Chunks2),
			choose_chunks(Av,Q-2,Chunks3,[{L,Sl},Best|Selected])
	end.

get_best(Av,Chunks) ->
	[{_,Chunk,BestSize}|Rest] = lists:sort([ {abs(Av-Size),Ch,Size} || {Ch,Size} <- Chunks]),
	{{Chunk,BestSize},lists:keysort(2,[ {Ch,Size} || {_,Ch,Size}<-Rest])}.


adjust(Len,MinB,MaxB) ->
	Diff = lists:sum([ Qty || {_,Qty,_,_} <- MinB++MaxB]) - Len,
	if
		Diff =:= 0 -> {MinB,MaxB};
		Diff > 0 -> 
			[{N,Q,C,M}|MinB1] = lists:sort(fun({_,Qa,Ca,_},{_,Qb,Cb,_})-> Qa/Ca > Qb/Cb end,MinB),
			adjust(Len,MinB1,[{N,Q-1,C,M}|MaxB]);
		true ->
			[{N,Q,C,M}|MaxB1] = lists:sort(fun({_,Qa,Ca,_},{_,Qb,Cb,_})-> Qa/Ca < Qb/Cb end,MaxB),
			adjust(Len,[{N,Q+1,C,M}|MinB],MaxB1)
	end.



% returns a list of list of chunks.
% Boxes = [{Name,Host,Mem}|]
chunks_to_box(ChromoLs,FilesInfo,Boxes) ->
	ChunksInfo = lists:foldl(
		fun(Chromo,Acc) ->
			Filtered = lists:filter(fun([FileName, _FileSize]) ->
					case re:run(FileName, "^" ++ Chromo ++ "_p\\d+.fm$") of
						{match,_} -> true;
						nomatch -> false
					end
				end, FilesInfo),
			Normalized = lists:map(
				fun([FileName, FileSize]) when is_atom(FileName) ->
					{FileName, FileSize};
					([FileName, FileSize]) when is_binary(FileName) ->
						{binary_to_atom(FileName, latin1), FileSize}
				end, Filtered),
			Normalized ++ Acc
		end,
	[], ChromoLs),

	ChunkList = lists:keysort(2,[{source,?SOURCE_MEM},{sink,?SINK_MEM}] ++
	[ {File,(SizeBytes bsr 20)+?EXTRA_MEM_PER_CHUNK} || {File, SizeBytes} <- ChunksInfo]),

	TotalMemory = lists:foldl(fun({_Name,_Host,_Cpu,Mem},Acc) -> Mem + Acc end, 0, Boxes),

	MemoryNeed = (?SOURCE_MEM+lists:sum([ Size || {_,Size} <- ChunkList ])+?SINK_MEM ) bsr 10,
	case MemoryNeed > TotalMemory of
		true -> not_enough_memory;
		false->
			CPC = length(ChunkList)/lists:sum([ Cores ||{_,_,Cores,_} <- Boxes ]),
			io:format("CPC: ~p~n",[CPC]),

			MPCs = lists:keysort(2,[ {Name,M/C} || {Name,_Host,C,M} <- Boxes ]),
			{Boxes_down,Boxes_up} = lists:split(round(length(MPCs) / 2), [ Name || {Name,_} <- MPCs]),
			%io:format("Boxes_up: ~p~nBoxes_down: ~p~n",[Boxes_up,Boxes_down]),

			Min_bucket = [ {Name,ceiling(CPC*Cores),Cores,Mem} || {Name,_Host,Cores,Mem} <- Boxes, lists:member(Name,Boxes_up)],
			Max_bucket = [ {Name,trunc(CPC*Cores),Cores,Mem} || {Name,_Host,Cores,Mem} <- Boxes, lists:member(Name,Boxes_down)],

			%io:format("Min boxes: ~p~nMax boxes: ~p~n",[Min_bucket,Max_bucket]),
			{MinB1,MaxB1} = adjust(length(ChunkList),Min_bucket,Max_bucket),
			%io:format("Min boxes: ~p~nMax boxes: ~p~n",[MinB1,MaxB1]),

			{Small_chunks,Large_chunks} = lists:split(lists:sum([Q || {_,Q,_,_}<-MaxB1]),lists:keysort(2,ChunkList)),
			%io:format("Small chunks: ~p~nLarge chunks: ~p~n",[Small_chunks,Large_chunks]),

			Total_chunk_sizeS = lists:sum([ S || {_,S} <- Small_chunks]),
			Total_mem_sizeS = lists:sum([ M || {_,_,_,M} <- MaxB1]),
			Chunk_size_per_boxS = [ {Total_chunk_sizeS*M/Total_mem_sizeS/Q,N,Q} || {N,Q,_,M} <- MaxB1],
			%io:format("Average chunk size per box: ~n~p~n",[Chunk_size_per_boxS]),
			Chunk_size_per_boxS1 = lists:map(fun({X,N,Y}) -> {X,lists:keyfind(N,1,Boxes),Y} end, Chunk_size_per_boxS),

			Total_chunk_sizeL = lists:sum([ S || {_,S} <- Large_chunks]),
			Total_mem_sizeL = lists:sum([ M || {_,_,_,M} <- MinB1]),
			Chunk_size_per_boxL = [ {Total_chunk_sizeL*M/Total_mem_sizeL/Q,N,Q} || {N,Q,_,M} <- MinB1],
			%io:format("Average chunk size per box: ~n~p~n",[Chunk_size_per_boxL]),
			Chunk_size_per_boxL1 = lists:map(fun({X,N,Y}) -> {X,lists:keyfind(N,1,Boxes),Y} end, Chunk_size_per_boxL),

			spread2boxes(Small_chunks,lists:sort(Chunk_size_per_boxS1))++
			spread2boxes(Large_chunks,lists:sort(Chunk_size_per_boxL1))
	end.


distribute(ChunkList,Buckets) -> distribute(ChunkList,lists:sort(fun(B1,B2)->
	lists:sum([ Size ||{_,Size}<-B1]) < lists:sum([ Size ||{_,Size}<-B2])
end,Buckets),[]).
distribute([{File,Size}|ChunkList],[B|Buckets],Acc) -> distribute(ChunkList,Buckets,[[{File,Size}|B]|Acc]);
distribute([],Buckets,Acc) -> Buckets ++ Acc;
distribute(ChunkList,[],Acc) -> distribute(ChunkList,Acc).



%% returns {{element, memory, box, Schedule1}
pop_subcluster_element(Name, Schedule) ->
	pop_subcluster_element(Name, [], Schedule).

pop_subcluster_element(_,_,[]) ->
	false;
pop_subcluster_element(Name, NotFound, [{Box, List} | Schedule1]) ->
	case lists:keyfind(Name, 1, List) of
		{Name, Mem} = T ->
			Schedule2 = NotFound ++ [{Box, lists:delete(T, List)} | Schedule1],
			{Name, Mem, Box, Schedule2};
		false ->
			pop_subcluster_element(Name, [{Box, List} | NotFound], Schedule1)
	end.



%% Tests

chunks_to_box_test_() ->
	Json = os:cmd("./scripts/make_refs_list.py"),
	FilesInfo = jsx:decode(list_to_binary(Json)),
	[fun() ->
		Box = {box1, host, 2, 0.5},
		?assertEqual(
			not_enough_memory,
			chunks_to_box(["GL000193.1"],FilesInfo,[Box]))
		end,

		fun() ->
			Box = {box1, host, 2, 10},
			?assertMatch(
				[{Box, [{source,_},{'GL000193.1_p1.fm',_},{sink,_}]}],
				lists:sort(chunks_to_box(["GL000193.1"],FilesInfo,[Box])))
		end
	].

pop_subcluster_element_test_() ->
	Schedule =
		[{"Box1",
		[{'chr17_p6.fm',200},
		{'chr17_p3.fm',200},
		{'chr17_p1.fm',200},
		{source,500}]},
		{"Box2",
		[{'chr17_p4.fm',200},
		{'chr17_p5.fm',200},
		{'chr17_p2.fm',200},
		{sink,500}]}],
	[
		fun() ->
			?assertEqual(
				false,
				pop_subcluster_element(does_not_exist, Schedule)
			)
		end,
		fun() ->
			?assertEqual(
				{source,500,"Box1", [{"Box1",
					[{'chr17_p6.fm',200},
						{'chr17_p3.fm',200},
						{'chr17_p1.fm',200}]},
					{"Box2",
						[{'chr17_p4.fm',200},
							{'chr17_p5.fm',200},
							{'chr17_p2.fm',200},
							{sink,500}]}
				]},
				pop_subcluster_element(source, Schedule)
			)
		end,
		fun() ->
			?assertEqual(
				{ 'chr17_p5.fm' ,200, "Box2", [{"Box1",
					[{'chr17_p6.fm',200},
						{'chr17_p3.fm',200},
						{'chr17_p1.fm',200},
						{source,500}]},
					{"Box2",
						[{'chr17_p4.fm',200},
							{'chr17_p2.fm',200},
							{sink,500}]}
				]},
				pop_subcluster_element('chr17_p5.fm', Schedule)
			)
		end
	].

ceiling(X) when X < 0 ->
    trunc(X);
ceiling(X) ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T + 1
    end.
