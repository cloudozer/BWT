% Smith-Waterman algo
% for finding best local matching between two sequences
%
% http://en.wikipedia.org/wiki/Smith%E2%80%93Waterman_algorithm
%

-module(sw).
-export([
		t/0,
		sw/2,
		sw/3,
		simple_match/2,
		sigma/2,
		rand_seq/1,
		get_starting_point/2]).

-include("bwt.hrl").



t() ->
	N = 5000,
	t(os:timestamp(), N,0). 
	
t(T1, 0, J) -> 
	T2 = os:timestamp(),
	io:format("SW took ~ps; found ~p matches~n",[timer:now_diff(T2,T1) / 1000000, J ]);
t(T1, N, J) ->
	CIGAR = simple_match("ACGTTGCAGTGCATGACAGTAGACATGACAGTACAGTAGCACCCCCATTAATTGGGGGGGATTAGATAGATTGGGAACTAGTGACATAGACAGATACCGG",
		"ACGTTGCAGTGCATGACAGTAGACATGACAGTACAGTAGCACCCCCATTAATTGGAAGGGATTAGATAGATTGGGAACTAGTGACATAGACAGATACCGG"),
	case CIGAR of
		no_match -> t(T1,N-1,J);
		_ -> t(T1,N-1,J+1)
	end.
	

rand_seq(N)->
	rand_seq([],N).
rand_seq(Acc,0) -> Acc;
rand_seq(Acc,N) -> 
	case random:uniform() < 0.0 of
		true ->
			rand_seq([lists:nth(random:uniform(10),[$Y,$R,$B,$D,$K,$M,$N,$S,$V,$W])|Acc], N-1);
		_ ->
			rand_seq([lists:nth(random:uniform(4),[$A,$C,$G,$T])|Acc], N-1)
	end.




sw([_|_]=Qseq,[_|_]=Ref) -> 
	F = fun(_Qseq,_Ref,Tab,V) -> get_CIGAR(Tab,V) end,
	sw(Qseq,Ref,F);
sw(N1,N2) when N1 =< N2 -> 
	random:seed(),
	rand_seq(10),
	sw(rand_seq(N1),rand_seq(N2));
sw(_,_) -> no_match.

sw(Qseq,Ref,F) ->
	%io:format("Seq: ~p~n",[Qseq]),
	%io:format("Ref: ~p~n",[Ref]),
	Lq = length(Qseq),
	%%%%%% {Header,[column]}
	Tab0 = [ [{0,undef}] || _ <- lists:seq(0,length(Ref)) ],
	Thershold = -?MATCH*(Lq div 11),
	build_tab(F,Tab0,Ref,Qseq,[],0,Thershold).





build_tab(_,_,_,_,_,V1max,Vthr) when V1max < Vthr -> 
	%io:format(" SW: no match~n"),
	no_match;
build_tab(F,[[{V,Dir}|FirstCol]|Tab1],Ref,[S|Qseq],Qseq_rev,_,Vthr) -> % Vs1, Vs2 - starting scores for the previous and curr rows
	V1 = case FirstCol of
		[] -> V+?GAP_PENALTY;
		_ -> V+?GAP_EXT_PENALTY
	end,
	{V2max,Tab2} = add_row2tab(Tab1,Ref,S,[[{V1,up},{V,Dir}|FirstCol]], V1), % Gap = Insertion
	
	build_tab(F,Tab2,Ref,Qseq,[S|Qseq_rev],V2max,Vthr+?MATCH);
build_tab(F,Tab,Ref,[],Qseq_rev,Vmax,_) -> F(Qseq_rev,Ref,Tab,Vmax).
	%io:format("Tab:~n~p~n",[Tab]),
	%get_CIGAR(Tab,Vmax).

	



add_row2tab([[{Vup,Dup}|_]=Col|Tab],[Sr|Ref], Sq, [[{Vleft,Dleft},{Vul,_}|_]|_]=AccT, Vmax) ->
	
	{V,Dir} = lists:max([
		{Vul+sigma(Sq,Sr),up_left},
		{get_Lvalue(Vleft,Dleft),left},
		{get_Uvalue(Vup,Dup),up} 
		]),
	%io:format("~n  Vul: ~p,\tVup: ~p~n",[Vul,Vup]),
	%io:format("Vleft: ~p,\tV: ~p\tSref: ~c, Sqsec: ~c  Dir: ~p~n",[Vleft,V,Sr,Sq,Dir]),
	
	add_row2tab(Tab,Ref,Sq,[[{V,Dir}|Col]|AccT], max(Vmax,V));

add_row2tab([],[],_,AccT,Vmax) -> {Vmax,lists:reverse(AccT)}.


get_Lvalue(V,left) -> V+?GAP_EXT_PENALTY;
get_Lvalue(V,_) -> V+?GAP_PENALTY.

get_Uvalue(V,up) -> V+?GAP_EXT_PENALTY;
get_Uvalue(V,_) -> V+?GAP_PENALTY.


get_starting_point(Tab,Vmax) -> get_starting_point(Tab,Vmax,[]).
get_starting_point([[{Vmax,D}|Col]|_],Vmax,Acc) -> [[{Vmax,D}|Col]|Acc];
get_starting_point([Col|Tab],Vmax,Acc) -> get_starting_point(Tab,Vmax,[Col|Acc]).

	
% d=deletion, i=insertion, m=match/mismatch
get_CIGAR(Tab,Vmax) -> 
	%io:format("Max: ~p~n",[Vmax]),
	Tab1 = get_starting_point(Tab,Vmax),

	%io:format("Tab: ~n~p~n",[Tab1]),
	[ [{_,Dir}|Column] | Tab2 ] = Tab1,
	
	{trunc(Vmax),	case Dir of
				up -> get_CIGAR(Tab2,Column,1,Dir,1,""); % eat current column 
				_ -> get_CIGAR(Tab2,1,Dir,1,"")  % eat next column
			end
	}.
	
get_CIGAR([Column|Tab],Skip,Dir,N,CIGAR) ->
	%io:format("Next Col. CIGAR: ~p~n",[CIGAR]),
	ColTail = lists:nthtail(Skip,Column),
	get_CIGAR(Tab,ColTail,Skip,Dir,N,CIGAR).


get_CIGAR(_,[{_,undef}],_,Dir,N,CIGAR) -> get_str(N,Dir)++CIGAR;

get_CIGAR(Tab,[{_,up}|Column],Skip,up,N,CIGAR) ->	
	get_CIGAR(Tab,Column,Skip+1,up,N+1,CIGAR);
get_CIGAR(Tab,[{_,up}|Column],Skip,Dir,N,CIGAR)	->
	get_CIGAR(Tab,Column,Skip+1,up,1,get_str(N,Dir)++CIGAR);
get_CIGAR(Tab,[{_,Dir}|_],Skip,Dir,N,CIGAR) ->
	case Dir of
		up_left -> get_CIGAR(Tab,Skip+1,Dir,N+1,CIGAR);
		left -> get_CIGAR(Tab,Skip,Dir,N+1,CIGAR)
	end;

get_CIGAR(Tab,[{_,Dir2}|_],Skip,Dir1,N,CIGAR) ->
	case Dir2 of
		up_left -> get_CIGAR(Tab,Skip+1,Dir2,1,get_str(N,Dir1)++CIGAR);
		left ->    get_CIGAR(Tab,Skip,  Dir2,1,get_str(N,Dir1)++CIGAR)
	end.



get_str(N,Dir) ->
	integer_to_list(N)++case Dir of
							up_left -> "M";
							left  -> "D";
							up  -> "I"
						end.
 


simple_match(Ls1,Ls2) when length(Ls1) =:= length(Ls2) -> 
	simple_match(Ls1,Ls2,?MATCH*length(Ls1),?MATCH*length(Ls1)*0.9).

simple_match(_,_,Val,Thr) when Val < Thr -> no_match;
simple_match([$N|Ls1],[_|Ls2],Val,Thr) -> simple_match(Ls1,Ls2,Val-?MATCH+?UNKNOWN,Thr);
simple_match([S|Ls1],[S|Ls2],Val,Thr) -> simple_match(Ls1,Ls2,Val,Thr);
simple_match([_|Ls1],[_|Ls2],Val,Thr) -> simple_match(Ls1,Ls2,Val-?MATCH+?MISMATCH,Thr);
simple_match([],[],Val,_) -> Val.



sigma($N,$N)-> ?UNKNOWN;
sigma(S,S)  -> ?MATCH;
sigma($N,_) -> ?UNKNOWN;
sigma(_,$N) -> ?UNKNOWN;

sigma($A,$R) -> ?MATCH;
sigma($G,$R) -> ?MATCH;
sigma(_, $R) -> ?MISMATCH;

sigma($C,$Y) -> ?MATCH;
sigma($T,$Y) -> ?MATCH;
sigma(_, $Y) -> ?MISMATCH;

sigma($A,$B) -> ?MISMATCH;
sigma(_, $B) -> ?MATCH;

sigma($C,$D) -> ?MISMATCH;
sigma(_, $D) -> ?MATCH;

sigma($G,$K) -> ?MATCH;
sigma($T,$K) -> ?MATCH;
sigma(_, $K) -> ?MISMATCH;

sigma($A,$M) -> ?MATCH;
sigma($C,$M) -> ?MATCH;
sigma(_, $M) -> ?MISMATCH;

sigma($G,$S) -> ?MATCH;
sigma($C,$S) -> ?MATCH;
sigma(_, $S) -> ?MISMATCH;

sigma($A,$W) -> ?MATCH;
sigma($T,$W) -> ?MATCH;
sigma(_, $W) -> ?MISMATCH;

sigma($T,$V) -> ?MISMATCH;
sigma(_, $V) -> ?MATCH;

sigma(_,_) -> ?MISMATCH.

