% Smith-Waterman algo
% for finding best local matching between two sequences
%
% http://en.wikipedia.org/wiki/Smith%E2%80%93Waterman_algorithm
%

-module(sw).
-export([sw/2]).

-define(MATCH,2).
-define(MISMATCH,-1).
-define(GAP_PENALTY,-1).



sw(W1,W2) ->
	Tab = [ {L1,[{0,undef}]} || L1 <- W1 ],
	Tab1 = build_tab(Tab,W2),
	{C1,Match,C2} = find_max(Tab1,W2),
	io:format("~s~n~s~n~s~n",[C1,Match,C2]).


build_tab(Tab,[S2|W2]) ->
	Tab1 = build_tab([], 0, 0, Tab, S2),
	build_tab(Tab1,W2);
build_tab(Tab,[]) -> Tab.


build_tab(AccT, V11, V10, [{S1,[{V01,_}|_]=Ls}|Tab], S2) ->
	%io:format("V11:~p, V10:~p, S1:~c, V01:~p~n",[V11,V10,S1,V01]),
	{V,Dir} = lists:max([
		{0,undef},
		{V11+sigma(S1,S2),ul},
		{V10+?GAP_PENALTY,l},
		{V01+?GAP_PENALTY,u} 
		]),
	build_tab([{S1,[{V,Dir}|Ls]}|AccT], V01, V, Tab, S2);

build_tab(AccT,_,_,[],_) -> lists:reverse(AccT).



find_max(Tab,W2) ->
	%io:format("Tab~n~p~n",[Tab]),
	{Vmax,_} = lists:max([ lists:max(Column) || {_,Column} <- Tab ]),
	%io:format("Vmax:~p~n",[Vmax]),
	Tab1 = remove_last_columns(Vmax,lists:reverse(Tab)),
	%io:format("Tab after removal~n~p~n",[Tab1]),
	[{_,Col}|_] = Tab1,
	Index = get_index(1,Col,Vmax),
	%io:format("Index:~p~n",[Index]),
	extract_matches([],[],[],Index,Tab1,lists:reverse(W2)).




extract_matches(X1, Match, X2, Index, [{S1,Column}|Tab],W2) ->
	{V,S2, Dir} = get_next(Index,Column,W2),
	case V =:= 0 of
		true -> {X1,Match,X2};
		_ -> 
			case Dir of
				u -> extract_matches(["-"|X1], [" "|Match], [S2|X2], Index+1, Tab, W2);
				l -> extract_matches([S1|X1],  [" "|Match], ["-"|X2], Index, Tab, W2);
				ul when S1=/=S2 -> extract_matches([S1|X1],  [" "|Match], [S2|X2], Index+1, Tab, W2);
				ul -> extract_matches([S1|X1],  ["|"|Match], [S2|X2], Index+1, Tab, W2)
			end
	end;
extract_matches(X1,Match,X2,_,[],_) -> {X1,Match,X2}.



get_next(1,[{V,Dir}|_],[S2|_]) -> {V,S2,Dir};
get_next(Index,[_|Column],[_|W2]) -> get_next(Index-1, Column, W2);
get_next(_,_,[]) -> {0, " ", undef}.




get_index(Index,[{Vmax,_}|_],Vmax) -> Index;
get_index(Index,[_|Col],Vmax) ->
	get_index(Index+1,Col,Vmax).



remove_last_columns(Vmax,[{_,Col}|Rest]=Tab) ->
	{V,_} = lists:max(Col),
	case V < Vmax of
		true ->
			remove_last_columns(Vmax,Rest);
		_ ->
			Tab
	end.



sigma(S1,S1) -> ?MATCH;
sigma(_,_) -> ?MISMATCH.

