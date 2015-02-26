% A simple implementation of BWT
% 
% Cloudozer(c), 2014
%

-module(bwt).
-export([bwt/1,get_suffs/1,bwt_sa/1]).



bwt(X) ->
	T = lists:reverse([$$|lists:reverse(X)]),

	N = length(T),

	Permutations = get_all_permutations([],T,N,N),
	%lists:foreach(fun(S)-> io:format("~p~n",[S]) end, Permutations),
	%io:format("~n"),
	[ lists:nth(N,Seq) || Seq <- Permutations].
	

get_all_permutations(Acc,_,0,_) -> lists:sort(Acc);
get_all_permutations(Acc,T,K,N) -> 
	{T1,Last} = lists:split(N-1,T),
	NewT = Last++T1,
	%io:format("NewT:~s~n",[NewT]),
	get_all_permutations([NewT|Acc],NewT,K-1,N).


bwt_sa(X) ->
	Ls = lists:sort(get_suffs(X)),
	[ case N of
		0 -> $$;
		J -> lists:nth(J,X)
	  end || {_,N} <- Ls].

get_suffs(X) ->
	get_suffs("",0,X++"$").

get_suffs(Acc, N, [H|X]) ->
	get_suffs([{[H|X],N}|Acc], N+1, X);
get_suffs(Acc,_,[]) -> Acc.


