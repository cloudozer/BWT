% Smith-Waterman algo
% for finding best local matching between two sequences
%
% http://en.wikipedia.org/wiki/Smith%E2%80%93Waterman_algorithm
%

-module(sw).
-export([sw/2,
		rand_seq/1]).

-define(MATCH,1).
-define(MISMATCH,-1).
-define(GAP_PENALTY,-1).
-define(UNDEF,1).

-define(THRESHOLD, 5).



rand_seq(N)->
	rand_seq([],N).
rand_seq(Acc,0) -> Acc;
rand_seq(Acc,N) -> rand_seq([lists:nth(random:uniform(4),[$A,$C,$G,$T])|Acc], N-1).






sw([_|_]=W1,[_|_]=W2) when length(W1) > length(W2) -> no_match;

sw([_|_]=W1,[_|_]=W2) ->
	io:format("Seq: ~p~n",[W1]),
	io:format("Ref: ~p~n",[W2]),
	
	%%%%%% {Header,[column]}
	Tab = [ {L1,[{0,undef}]} || L1 <- W1 ],
	case build_tab(Tab,W2,0,0) of
		low_score -> no_match;
		Tab1 -> 
			%io:format("Tab: ~p~n",[Tab1]),
			get_CIGAR(Tab1)
	end;

sw(N1,N2) when N1 < N2 -> sw(rand_seq(N1),rand_seq(N2));
sw(N1,N2) when N1 >= N2 -> sw(rand_seq(N2),rand_seq(N1)).

build_tab(_,_,V1max,Vpmax) when Vpmax-V1max >= ?THRESHOLD -> low_score;
build_tab(Tab,[S2|W2],V1max,Vpmax) ->
	io:format("Vpmax:~p~n",[Vpmax]),
	{V2max,Tab1} = add_row2tab(Tab, S2, [], 0, 0, V1max),
	build_tab(Tab1,W2,V2max, min(Vpmax+?MATCH, ?MATCH*(length(Tab1)-?THRESHOLD) ));
build_tab(Tab,[],_,_) -> [ list_to_tuple(Col) || {_,Col} <- Tab ].
	

add_row2tab([{S1,[{V01,_}|_]=Ls}|Tab], S2, AccT, V11, V10, Vmax) ->
	%io:format("V11:~p, V10:~p, S1:~c, V01:~p~n",[V11,V10,S1,V01]),
	{V,Dir} = lists:max([
		{0,undef},
		{V11+sigma(S1,S2),ul},
		{V10+?GAP_PENALTY,l},
		{V01+?GAP_PENALTY,u} 
		]),
	add_row2tab(Tab, S2, [{S1,[{V,Dir}|Ls]}|AccT], V01, V, max(Vmax,V));

add_row2tab([],_,AccT,_,_,Vmax) -> {Vmax,lists:reverse(AccT)}.


% d=deletion, i=insertion, m=match/mismatch
get_CIGAR(Tab) -> 
	[Last_col|Tab1] = lists:reverse(Tab),
	{V,Dir} = lists:max(tuple_to_list(Last_col)),
	J = count(Last_col,1,V),
	io:format("Index of last element: ~p, element:~p~n",[J,Dir]),
	case Dir of
		ul -> get_CIGAR(Tab1, J+1, ul, 1, "");
		l ->  get_CIGAR(Tab1, J,   l,  1, "");
		u ->  get_CIGAR(Tab1, J+1, u,  1, "");
		undef -> get_CIGAR(Tab1, J+1, ul, 1, "")
	end.

get_CIGAR([Column|Tab], J, Dir1, N, CIGAR) ->
	{_,Dir2} = element(J,Column),
	io:format("element:~p~n",[Dir2]),
	if 
		Dir1 == Dir2 -> get_CIGAR(Tab, next_index(J,Dir2), Dir1, N+1, CIGAR);
		Dir2 == undef andalso Dir1 == ul -> get_CIGAR(Tab, next_index(J,ul), ul, N+1, CIGAR);
		Dir2 == undef -> get_CIGAR(Tab, next_index(J,ul), ul, 1, integer_to_list(N)++get_op(Dir1)++CIGAR);
		true -> get_CIGAR(Tab, next_index(J,ul), Dir2, 1, integer_to_list(N)++get_op(Dir1)++CIGAR)
	end;
get_CIGAR([], _, Dir1, N, CIGAR) -> integer_to_list(N)++get_op(Dir1)++CIGAR.

next_index(J,Dir) ->
	case Dir of
		l -> J;
		_ -> J+1
	end.


get_op(Dir) ->
	case Dir of
		ul -> "M";
		l  -> "I";
		u  -> "D"
	end.
 

count(Tup,J,V) ->
	case element(J,Tup) of
		{V,_} -> J;
		_ -> count(Tup,J+1,V)
	end.

find_max(Tab,W2,Vmax) ->
	%Tab1 = remove_last_columns(Vmax,lists:reverse(Tab)),
	[{_,Col}|_] = Tab,
	Index = get_index(1,Col,Vmax),
	{extract_matches([],[],[],Index,Tab,lists:reverse(W2)), Vmax}.




extract_matches(X1, Match, X2, Index, [{S1,Column}|Tab],W2) ->
	{V,S2, Dir} = get_next(Index,Column,W2),
	case V =:= 0 of
		true -> {X1,Match,X2};
		_ -> 
			case Dir of
				u -> extract_matches([$-|X1], [32|Match], [S2|X2], Index+1, [{S1,Column}|Tab], W2);
				l -> extract_matches([S1|X1],  [32|Match], [$-|X2], Index, Tab, W2);
				ul when S1=/=S2 -> extract_matches([S1|X1],  [32|Match], [S2|X2], Index+1, Tab, W2);
				ul -> extract_matches([S1|X1],  [$||Match], [S2|X2], Index+1, Tab, W2)
			end
	end;
extract_matches(X1,Match,X2,_,[],_) -> {X1,Match,X2}.



get_next(1,[{V,Dir}|_],[S2|_]) -> {V,S2,Dir};
get_next(Index,[_|Column],[_|W2]) -> get_next(Index-1, Column, W2);
get_next(_,_,[]) -> {0, 32, undef}.




get_index(Index,[{Vmax,_}|_],Vmax) -> Index;
get_index(Index,[_|Col],Vmax) ->
	get_index(Index+1,Col,Vmax).



remove_last_columns(Vmax,[{_,Col}|Rest]=Tab) ->
	{V,_} = lists:max(Col),
	case V < Vmax of
		true -> remove_last_columns(Vmax,Rest);
		_ -> Tab
	end.



sigma(S,S) -> ?MATCH;
sigma(_,$N) -> ?UNDEF;
sigma($N,_) -> ?UNDEF;
sigma(_,_) -> ?MISMATCH.

