% seed finder module 
% 
% 
% Cloudozer(c), 2015
%


-module(sf).
-export([start_SF/4,
		seed_finder/4
		]).



% returns {Alqs, SFs} - list of Alq processes and list of SF processes
start_SF(Schedule,Lingd,HttpStorage,Source) -> start_SF(Schedule,Lingd,HttpStorage,Source,[],[]).

start_SF([{Box_id,Alq={AlqHost,_},Chunk_files}|Schedule],Lingd,HttpStorage,Source,Alqs,SFs) ->
	SFs1 = lists:foldl( fun(Chunk,Acc)->
		  NodeName = list_to_atom("chunk_" ++ lists:filter(fun($.) -> false;(_) -> true end, binary_to_list(Chunk))),
		  {ok,_} = lingd:create(Lingd, NodeName, [{memory, 128}]),
		%% self?
		  ok = navel:call(NodeName, navel, connect, [AlqHost]),
			SF = navel:call(NodeName, erlang, spawn, [?MODULE,seed_finder,[Chunk,Alq,Source,HttpStorage]]),
			[{NodeName,SF}|Acc]
						end, SFs,Chunk_files),
	start_SF(Schedule,Lingd,HttpStorage,Source,[{Box_id,Alq}|Alqs],SFs1);

start_SF([],_,_,_,Alqs,SFs) ->
	% global pids
	{Alqs, SFs}.




seed_finder(Chunk,Alq={_,{AlqN,AlqP}},R_source,HttpStorage) ->
	{ok, FmIndexBin} = http:get(HttpStorage ++ "/fm_indices/" ++ binary_to_list(Chunk)),
	{Meta,FM} = binary_to_term(FmIndexBin),

	{Pc,Pg,Pt,Last} = proplists:get_value(pointers, Meta),
	Shift = proplists:get_value(shift, Meta),
	navel:call_no_return(AlqN, erlang, send, [AlqP, {shift,Chunk,Shift}]),
	seed_finder(Chunk,Alq,R_source,FM,Pc,Pg,Pt,Last).

seed_finder(Chunk,Alq={_,{AlqN,AlqP}},R_source={SN,SP},FM,Pc,Pg,Pt,Last) ->
	navel:call_no_return(SN,erlang,send,[SP,{{navel:get_node(),self()},ready}]),
	receive
		quit -> ok;
		{data,[]} -> throw({fs, empty_batch});
		{data,Batch} ->
%% 			io:format("seed finder ~p got ~p~n",[Chunk,length(Batch)]),

			lists:foreach(
				fun({_Qname,Qseq}=Read) ->
					Seeds = sga:sga(FM,Pc,Pg,Pt,Last,binary_to_list(Qseq)),
					navel:call_no_return(AlqN, erlang, send, [AlqP, {Read,Chunk,Seeds}])
				end, Batch),

			seed_finder(Chunk,Alq,R_source,FM,Pc,Pg,Pt,Last)
	end.


	