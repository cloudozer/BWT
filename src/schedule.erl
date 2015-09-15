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
-define(SINK_MEM,500).



c2b() ->
	ChunkList = [{source,600},{sink,400},
				{ch1,700},{ch2,700},{ch3,700},
				{ch4,760},{ch5,760},{ch6,760},{ch7,760},
				{ch8,770},{ch9,770},{ch10,770},{ch11,770},{ch12,770},
				{ch13,790},{ch14,790},{ch15,790},{ch16,790},{ch17,790},{ch18,790},
				{ch19,800},{ch20,800},{ch21,800}],
	Boxes = [{4,16},{8,24},{8,24},{8,32}],

	CPC = length(ChunkList)/lists:sum([ Cores ||{Cores,_} <- Boxes ]),
	io:format("CPC: ~p~n",[CPC]).


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
