% Smith-Waterman prety print
% for finding best local matching between two sequences
%

-module(sw_pp).
-export([pp/2]).


pp(Qseq,Ref) -> sw:sw(Qseq,Ref,fun(Qs,Rs,Tab,V) -> pp(Qs,Rs,Tab,V) end).
	



pp(Que_rev,Ref,Tab,Vmax) ->
	Tab1 = sw:get_starting_point(Tab,Vmax),
	io:format("Tail contains ~p symbols~n",[length(Tab)-length(Tab1)]),
	io:format("Reversed Qseq: ~p~n",[Que_rev]).
	


	%io:format("Tab: ~n~p~n",[Tab1]),
%	[ [{_,Dir}|Column] | Tab2 ] = Tab1,
%	
%	{Vmax,	case Dir of
%				up -> get_CIGAR(Tab2,Column,1,Dir,1,""); % eat current column 
%				_ -> get_CIGAR(Tab2,1,Dir,1,"")  % eat next column
%			end
%	}.
%
%get_CIGAR([Column|Tab],Skip,Dir,N,CIGAR) ->
%	%io:format("Next Col. CIGAR: ~p~n",[CIGAR]),
%	ColTail = lists:nthtail(Skip,Column),
%	get_CIGAR(Tab,ColTail,Skip,Dir,N,CIGAR).
%
%
%get_CIGAR(_,[{_,undef}],_,Dir,N,CIGAR) -> get_str(N,Dir)++CIGAR;
%
%get_CIGAR(Tab,[{_,up}|Column],Skip,up,N,CIGAR) ->	
%	get_CIGAR(Tab,Column,Skip+1,up,N+1,CIGAR);
%get_CIGAR(Tab,[{_,up}|Column],Skip,Dir,N,CIGAR)	->
%	get_CIGAR(Tab,Column,Skip+1,up,1,get_str(N,Dir)++CIGAR);
%get_CIGAR(Tab,[{_,Dir}|_],Skip,Dir,N,CIGAR) ->
%	case Dir of
%		up_left -> get_CIGAR(Tab,Skip+1,Dir,N+1,CIGAR);
%		left -> get_CIGAR(Tab,Skip,Dir,N+1,CIGAR)
%	end;
%
%get_CIGAR(Tab,[{_,Dir2}|_],Skip,Dir1,N,CIGAR) ->
%	case Dir2 of
%		up_left -> get_CIGAR(Tab,Skip+1,Dir2,1,get_str(N,Dir1)++CIGAR);
%		left ->    get_CIGAR(Tab,Skip,  Dir2,1,get_str(N,Dir1)++CIGAR)
%	end.


