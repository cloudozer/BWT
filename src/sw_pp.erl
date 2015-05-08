% Smith-Waterman prety print
% for finding best local matching between two sequences
%

-module(sw_pp).
-export([pp/2]).

-include("bwt.hrl").
-define(SPACE,32).
-define(BAR,$|).
-define(DASH,$-).


pp(Qseq,Ref) -> sw:sw(Qseq,Ref,fun(Qs,Rs,Tab,V) -> pp(Qs,lists:reverse(Rs),Tab,V) end).
	


pp(Que_rev,Ref_rev,Tab,Vmax) ->
	Tab1 = sw:get_starting_point(Tab,Vmax),
	Tail_len = length(Tab) - length(Tab1),
	{Tail,Ref1} = lists:split(Tail_len,Ref_rev),
	Spaces = lists:duplicate(Tail_len,?SPACE),
	{S1,S2,S3} = get_comb(Que_rev,Ref1,Tab1,1,Spaces,Spaces,lists:reverse(Tail)),
	io:format("~p~n~p~n~p~n",[S1,S2,S3]).
	
get_comb([],Ref,_,_,Qcomb,Mcomb,Rcomb) -> 
	L = length(Ref),
	{lists:duplicate(L,?SPACE)++Qcomb,
	lists:duplicate(L,?SPACE)++Mcomb,
	lists:reverse(Ref)++Rcomb};
get_comb(Qseq,[],_,_,Qcomb,Mcomb,Rcomb) ->
	L = length(Qseq),
	{lists:reverse(Qseq)++Qcomb,
	lists:duplicate(L,?SPACE)++Mcomb,
	lists:duplicate(L,?SPACE)++Rcomb};
get_comb([Sq|Qseq],[Sr|Ref],[Col|Tab],N,Qcomb,Mcomb,Rcomb) ->
	{_,Dir} = lists:nth(N,Col),
	Match = sw:sigma(Sr,Sq),
	case { Dir, Match==?MATCH orelse Match==?UNKNOWN } of
		{up,false} -> % INSERTION
			get_comb(Qseq,[Sr|Ref],[Col|Tab],N+1,[Sq|Qcomb],[?SPACE|Mcomb],[?DASH|Rcomb]);
		{left,false} -> % DELETION
			get_comb([Sq|Qseq],Ref,Tab,N,[?DASH|Qcomb],[?SPACE|Mcomb],[Sr|Rcomb]);
		{up_left,true} -> % MATCH
			get_comb(Qseq,Ref,Tab,N+1,[Sq|Qcomb],[?BAR|Mcomb],[Sr|Rcomb]);
		{up_left,_} -> % MISMATCH
			get_comb(Qseq,Ref,Tab,N+1,[Sq|Qcomb],[?SPACE|Mcomb],[Sr|Rcomb])
	end.


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


