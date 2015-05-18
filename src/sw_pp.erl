% Smith-Waterman prety print
% for finding best local matching between two sequences
%

-module(sw_pp).
-export([pp/2,ppp/2]).

-include("bwt.hrl").
-define(SPACE,32).
-define(BAR,$|).
-define(DASH,$-).


ppp(Qseq,Ref) ->
	{L1,L2,L3} = pp(Qseq,Ref),
	io:format("~s~n~s~n~s~n",[L1,L2,L3]).


pp(Qseq,Ref) -> sw:sw(Qseq,Ref,fun(Qs,Rs,Tab,V) -> pp(Qs,lists:reverse(Rs),Tab,V) end).
	


pp(Que_rev,Ref_rev,Tab,Vmax) ->
	Tab1 = sw:get_starting_point(Tab,Vmax),
	Tail_len = length(Tab) - length(Tab1),
	{Tail,Ref1} = lists:split(Tail_len,Ref_rev),
	Spaces = lists:duplicate(Tail_len,?SPACE),
	get_comb(Que_rev,Ref1,Tab1,1,Spaces,Spaces,lists:reverse(Tail)).
	
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

