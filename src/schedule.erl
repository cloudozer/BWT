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



c2b() ->
	ChunkList = [{source,600},{sink,400},
				{ch1,700},{ch2,700},{ch3,700},
				{ch4,760},{ch5,760},{ch6,760},{ch7,760},
				{ch8,770},{ch9,770},{ch10,770},{ch11,770},{ch12,770},
				{ch13,790},{ch14,790},{ch15,790},{ch16,790},{ch17,790},{ch18,790},
				{ch19,800},{ch20,800},{ch21,800},{ch22,800}],
	Boxes = [{box1,4,16},{box2,8,24},{box3,8,24}],

	CPC = length(ChunkList)/lists:sum([ Cores ||{_,Cores,_} <- Boxes ]),
	io:format("CPC: ~p~n",[CPC]),

	MPCs = lists:keysort(2,[ {Name,M/C} || {Name,C,M} <- Boxes ]),
	{Boxes_down,Boxes_up} = lists:split(round(length(MPCs) / 2), [ Name || {Name,_} <- MPCs]),
	%io:format("Boxes_up: ~p~nBoxes_down: ~p~n",[Boxes_up,Boxes_down]),

	Min_bucket = [ {Name,ceiling(CPC*Cores),Cores,Mem} || {Name,Cores,Mem} <- Boxes, lists:member(Name,Boxes_up)],
	Max_bucket = [ {Name,trunc(CPC*Cores),Cores,Mem} || {Name,Cores,Mem} <- Boxes, lists:member(Name,Boxes_down)],

	io:format("Min boxes: ~p~nMax boxes: ~p~n",[Min_bucket,Max_bucket]),
	{MinB1,MaxB1} = adjust(length(ChunkList),Min_bucket,Max_bucket),
	io:format("Min boxes: ~p~nMax boxes: ~p~n",[MinB1,MaxB1]),

	{Small_chunks,Large_chunks} = lists:split(lists:sum([Q || {_,Q,_,_}<-MaxB1]),lists:keysort(2,ChunkList)),
	io:format("Small chunks: ~p~nLarge chunks: ~p~n",[Small_chunks,Large_chunks]),
	Order = fun({_Na,Qa,ChunksA},{_Nb,Qb,ChunksB}) ->
				length(ChunksA)/Qa < length(ChunksB)/Qb
			end,
	spread2boxes(lists:reverse(Small_chunks),lists:sort(Order,[ {N,Q,[]} || {N,Q,_C,_M}<- MaxB1]), Order)++
	spread2boxes(lists:reverse(Large_chunks),lists:sort(Order,[ {N,Q,[]} || {N,Q,_C,_M}<- MinB1]), Order).



spread2boxes([{Chunk,Size}|Chunks],[{N,Q,Ch}|Boxes],Order) -> 
	spread2boxes(Chunks, lists:sort(Order,[{N,Q,[{Chunk,Size}|Ch]}|Boxes]),Order);
spread2boxes([],Boxes,_) -> Boxes.



adjust(Len,MinB,MaxB) ->
	Diff = lists:sum([ Qty || {_,Qty,_,_} <- MinB++MaxB]) - Len,
	if
		Diff =:= 0 -> {MinB,MaxB};
		Diff < 0 -> 
			[{N,Q,C,M}|MinB1] = lists:sort(fun({_,Qa,Ca,_},{_,Qb,Cb,_})-> Qa/Ca < Qb/Cb end,MinB),
			adjust(Len,[{N,Q+1,C,M}|MinB1],MaxB);
		true ->
			[{N,Q,C,M}|MaxB1] = lists:sort(fun({_,Qa,Ca,_},{_,Qb,Cb,_})-> Qa/Ca > Qb/Cb end,MaxB),
			adjust(Len,MinB,[{N,Q-1,C,M}|MaxB1])
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

	TotalMemory = lists:foldl(fun({_Name,_Host,Mem},Acc) -> Mem + Acc end, 0, Boxes),

	case (?SOURCE_MEM+lists:sum([ Size || {_,Size} <- ChunkList ])+?SINK_MEM ) bsr 10 > TotalMemory of
		true -> not_enough_memory;
		false->
			Schedule = distribute(lists:reverse(ChunkList),lists:duplicate(length(Boxes),[])),
			lists:zip(Boxes,Schedule)
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
	Json = os:cmd("./make_refs_list.py"),
	FilesInfo = jsx:decode(list_to_binary(Json)),
	[fun() ->
		Box = {box1, host, 1},
		?assertEqual(
			not_enough_memory,
			chunks_to_box(["GL000193.1"],FilesInfo,[Box]))
	end,
		fun() ->
			Box = {box1, host, 10},
			?assertMatch(
				[{Box, [{'GL000193.1_p1.fm',_},{source,_},{sink,_}]}],
				chunks_to_box(["GL000193.1"],FilesInfo,[Box]))
		end].

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
