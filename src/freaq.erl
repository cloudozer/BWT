% Freaquency analysis of different nuclio sequences
% 
% Cloudozer(c), 2014
%

-module(freaq).
-export([freaqs/1]).

freaqs(Chr_name) -> 
	RefFile = "../bwt_files/human_g1k_v37_decoy.fasta",
	
	{Chr_pos, Chr_len} = msw:get_reference_position(Chr_name,RefFile),
	io:format("Chromosome position:~p~n",[Chr_pos]),
	Chunk = msw:get_chunk(RefFile, Chr_pos, Chr_len div 10),

	Ls = dict:to_list(freaqs(Chunk,dict:new()) ),
	Lsort = lists:sort(fun({_,V1},{_,V2})-> V1>V2 end, Ls),
	file:write_file("../distribution.data", io_lib:fwrite("~p\n", [Lsort])).

freaqs([S1,S2,S3,S4,S5,S6,S7|Chunk],Acc) ->
	Key = {S1,S2,S3,S4,S5,S6,S7},
	Acc1 = dict:update_counter(Key,1,Acc),
	case Chunk of
		[] -> Acc1;
		_  -> freaqs([S2,S3,S4,S5,S6,S7|Chunk],Acc1)
	end.



