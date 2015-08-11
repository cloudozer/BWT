% seed finder module 
% 
% 
% Cloudozer(c), 2015
%


-module(sf).
-export([start_SF/4,
		seed_finder/4
		]).


-define(REF_EXTENSION_LEN, 200).


% returns {Alqs, SFs} - list of Alq processes and list of SF processes
start_SF(Schedule,Lingd,HttpStorage,Source) -> start_SF(Schedule,Lingd,HttpStorage,Source,[],[]).

start_SF([{Box_id,Alq={AlqHost,_},Chunk_files}|Schedule],Lingd,HttpStorage,Source,Alqs,SFs) ->
	SFs1 = lists:foldl( fun(Chunk,Acc)->
		  NodeName = list_to_atom("chunk_" ++ lists:filter(fun($.) -> false;(_) -> true end, binary_to_list(Chunk))),
		  {ok,_} = lingd:create(Lingd, NodeName, [{memory, 2024}]),
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

	RefFileName = re:replace(Chunk, ".fm", ".ref", [{return, list}]),
	{ok, Ref} = http:get(HttpStorage ++ "/fm_indices/" ++ RefFileName),
	Extension = list_to_binary(lists:duplicate(?REF_EXTENSION_LEN, $N)),
	Ref1 = <<Extension/binary, Ref/binary, Extension/binary>>,

	{Pc,Pg,Pt,Last} = proplists:get_value(pointers, Meta),
	Shift = proplists:get_value(shift, Meta),
	SavedSeqs = dict:new(),
	seed_finder(Chunk,Alq,R_source,FM,SavedSeqs,Ref1,Pc,Pg,Pt,Last,Shift).

seed_finder(Chunk,Alq={_,{AlqN,AlqP}},R_source={SN,SP},FM,SavedSeqs,Ref,Pc,Pg,Pt,Last,Shift) ->
	navel:call_no_return(SN,erlang,send,[SP,{{navel:get_node(),self()},ready}]),
	receive
		quit -> print_stat(SavedSeqs);
		{data,[]} -> throw({fs, empty_batch});
		{data,Batch} ->
%% 			io:format("seed finder ~p got ~p~n",[Chunk,length(Batch)]),

			lists:foreach(
				fun({Qname,Qseq}) ->
					Seeds = sga:sga(FM,SavedSeqs,Pc,Pg,Pt,Last,binary_to_list(Qseq)),
					Seeds1 = lists:map(fun({SeedEnd,D}) ->
						Ref_len = size(Qseq) + D,
						GlobalPos = SeedEnd - Ref_len + Shift,
						Start_pos = SeedEnd - Ref_len + ?REF_EXTENSION_LEN,

						<<_:Start_pos/bytes,Ref_seq:Ref_len/bytes,_/binary>> = Ref,
						Ref_seq1 = binary_to_list(Ref_seq),

						{GlobalPos,Ref_seq1}
					end, Seeds),
					navel:call_no_return(AlqN, erlang, send, [AlqP, {Qname,Chunk,Qseq,Seeds1}])
				end, Batch),

			seed_finder(Chunk,Alq,R_source,FM,SavedSeqs,Ref,Pc,Pg,Pt,Last,Shift)
	end.



print_stat(SavedSeqs) ->
	N = dict:size(SavedSeqs),
	Entries = dict:fetch_keys(SavedSeqs),
	io:format("There are ~w entries in saved sequences dict~n",[N]),
	io:format("~p% are 'no_seeds'~n",[length(lists:filters(fun(K) -> dict:fetch(K,SavedSeqs)=:=no_seeds end,Entries))/N*100]),
	io:format("~p% are 'too_many_seeds'~n",[length(lists:filters(fun(K) -> dict:fetch(K,SavedSeqs)=:=too_many_seeds end,Entries))/N*100]).




