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

-include("bwt.hrl").


start(Chunk,AlqRef,SourceRef,HttpStorage) ->
	Pid = spawn_link(?MODULE,seed_finder,[Chunk,AlqRef,SourceRef,HttpStorage]),
	Ref = {navel:get_node(), Pid},
	{ok, Ref}.



seed_finder(Chunk,Alq={AlqN,AlqP},R_source={SN,SP},HttpStorage) ->
	ChunkList = atom_to_list(Chunk),
	{ok, _, FmIndexBin} = http:get(HttpStorage ++ "/fm_indices/" ++ ChunkList),
	{Meta,FM} = binary_to_term(FmIndexBin),

	RefFileName = re:replace(ChunkList, ".fm", ".ref", [{return, list}]),
	{ok, _, Ref} = http:get(HttpStorage ++ "/fm_indices/" ++ RefFileName),
	Extension = list_to_binary(lists:duplicate(?REF_EXTENSION_LEN, $N)),
	Ref1 = <<Extension/binary, Ref/binary, Extension/binary>>,

	{Pc,Pg,Pt,Last} = proplists:get_value(pointers, Meta),
	Shift = proplists:get_value(shift, Meta),
	SavedSeqs = ets:new(saved_seqs,[]),
	navel:call_no_return(SN,erlang,send,[SP,{{navel:get_node(),self()},ready}]),
	[Chromo|_] = strings:tokens(Chunk,"_p"),
	seed_finder(Chromo,Alq,R_source,FM,SavedSeqs,Ref1,Pc,Pg,Pt,Last,Shift).

seed_finder(Chromo,Alq={AlqN,AlqP},R_source={SN,SP},FM,SavedSeqs,Ref,Pc,Pg,Pt,Last,Shift) ->
	receive
		quit -> quit; 
		{data,[]} -> throw({fs, empty_batch});
		{data,Batch} ->
			navel:call_no_return(SN,erlang,send,[SP,{{navel:get_node(),self()},ready}]),

			SeedBatch = lists:foldl(
				fun({Qname,Qseq},SeedsAcc) ->
					Seeds = sga:sga(FM,SavedSeqs,Pc,Pg,Pt,Last,binary_to_list(Qseq)),
					SeedSeqs = lists:map(fun({SeedEnd,D}) ->
						Ref_len = size(Qseq) + D,
						GlobalPos = SeedEnd - Ref_len + Shift,
						Start_pos = SeedEnd - Ref_len + ?REF_EXTENSION_LEN,

						<<_:Start_pos/bytes,Ref_seq:Ref_len/bytes,_/binary>> = Ref,
						Ref_seq1 = binary_to_list(Ref_seq),

						{GlobalPos,Ref_seq1}
					end, Seeds),
					[ {Qname,Chromo,Qseq,SeedSeqs} | SeedsAcc ]
				end, [], Batch),

			case length(SeedBatch) =:= 0 of
				true -> ok;
				_ -> 
					navel:call_no_return(AlqN, erlang, send, [AlqP, SeedBatch])
			end,		
			seed_finder(Chromo,Alq,R_source,FM,SavedSeqs,Ref,Pc,Pg,Pt,Last,Shift)
	end.





