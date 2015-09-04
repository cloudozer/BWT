% seed finder module 
% 
% 
% Cloudozer(c), 2015
%


-module(sf).
-export([
	start/4,
	seed_finder/4
]).


-define(REF_EXTENSION_LEN, 200).

start(Chunk,AlqRef,SourceRef,HttpStorage) ->
	Pid = spawn_link(?MODULE,seed_finder,[Chunk,AlqRef,SourceRef,HttpStorage]),
	Ref = {navel:get_node(), Pid},
	{ok, Ref}.



seed_finder(Chunk,Alq={AlqN,AlqP},R_source={SN,SP},HttpStorage) ->
	ChunkList = atom_to_list(Chunk),
	{ok, FmIndexBin} = http:get(HttpStorage ++ "/fm_indices/" ++ ChunkList),
	{Meta,FM} = binary_to_term(FmIndexBin),

	RefFileName = re:replace(ChunkList, ".fm", ".ref", [{return, list}]),
	{ok, Ref} = http:get(HttpStorage ++ "/fm_indices/" ++ RefFileName),
	Extension = list_to_binary(lists:duplicate(?REF_EXTENSION_LEN, $N)),
	Ref1 = <<Extension/binary, Ref/binary, Extension/binary>>,

	{Pc,Pg,Pt,Last} = proplists:get_value(pointers, Meta),
	Shift = proplists:get_value(shift, Meta),
	SavedSeqs = dict:new(),
	navel:call_no_return(SN,erlang,send,[SP,{{navel:get_node(),self()},ready}]),
	seed_finder(Chunk,Alq,R_source,FM,SavedSeqs,Ref1,Pc,Pg,Pt,Last,Shift).

seed_finder(Chunk,Alq={AlqN,AlqP},R_source={SN,SP},FM,SavedSeqs,Ref,Pc,Pg,Pt,Last,Shift) ->
	receive
		quit -> quit; %% print_stat(SavedSeqs);
		{data,[]} -> throw({fs, empty_batch});
		{data,Batch} ->
			navel:call_no_return(SN,erlang,send,[SP,{{navel:get_node(),self()},ready}]),
%% 			io:format("seed finder ~p got ~p~n",[Chunk,length(Batch)]),

			SavedSeqs1 = lists:foldl(
				fun({Qname,Qseq},SavedSeqsAcc) ->
					{Seeds, SavedSeqsAcc1} = sga:sga(FM,SavedSeqsAcc,Pc,Pg,Pt,Last,binary_to_list(Qseq)),
					Seeds1 = lists:map(fun({SeedEnd,D}) ->
						Ref_len = size(Qseq) + D,
						GlobalPos = SeedEnd - Ref_len + Shift,
						Start_pos = SeedEnd - Ref_len + ?REF_EXTENSION_LEN,

						<<_:Start_pos/bytes,Ref_seq:Ref_len/bytes,_/binary>> = Ref,
						Ref_seq1 = binary_to_list(Ref_seq),

						{GlobalPos,Ref_seq1}
					end, Seeds),
					navel:call_no_return(AlqN, erlang, send, [AlqP, {Qname,Chunk,Qseq,Seeds1}]),
					SavedSeqsAcc1
				end, SavedSeqs, Batch),

			seed_finder(Chunk,Alq,R_source,FM,SavedSeqs1,Ref,Pc,Pg,Pt,Last,Shift)
	end.



print_stat(SavedSeqs) ->
	N = dict:size(SavedSeqs),
	Entries = dict:fetch_keys(SavedSeqs),
	io:format("There are ~w entries in saved sequences dict~n",[N]),
	io:format("~p% are 'no_seeds'~n",[length(lists:filter(fun(K) -> dict:fetch(K,SavedSeqs)=:=no_seeds end,Entries))/N*100]),
	io:format("~p% are 'too_many_seeds'~n",[length(lists:filter(fun(K) -> dict:fetch(K,SavedSeqs)=:=too_many_seeds end,Entries))/N*100]).




