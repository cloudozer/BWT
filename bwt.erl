% A simple implementation of BWT
% 
% Cloudozer(c), 2014
%

-module(bwt).
-export([bwt/1]).



bwt(X) ->
	T = lists:reverse(["$"|lists:reverse(X)]),
	N = length(T),

	Permutations = get_all_permutations([],T,N,N),
	%lists:foreach(fun(S)-> io:format("~s~n",[S]) end, Permutations),
	get_last_column(Permutations).


get_all_permutations(Acc,_,0,_) -> lists:sort(fun(A,B)-> A>B end,Acc);
get_all_permutations(Acc,T,K,N) -> 
	T1 = [lists:nth(N,T)|lists:sublist(T,N-1)],
	get_all_permutations([T1|Acc],T1,K-1,N).



get_last_column(P) ->
	[T|_] = P,
	get_last_column([],P,length(T)).

get_last_column(Acc,[T|P],N) ->
	get_last_column([lists:nth(N,T)|Acc],P,N);
get_last_column(Acc,[],_) ->
	io:format("~nOutput: ~ts~n",[Acc]),
	Acc.

