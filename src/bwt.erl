% A simple implementation of BWT
% 
% Cloudozer(c), 2014
%

-module(bwt).
-export([bwt/1,
		get_suffs/1,
		fm/1,
		rank_all/1,
		sa/1,
		test/1]).

-record(fm,{f,l,d,a,c,g,t,sa}).



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


fm(X) ->
	Ls = lists:sort(get_suffs(X)),
	%io:format("Sufs:~p~n",[Ls]),
	Index = lists:reverse( lists:foldl(fun({[H|_],N},Acc)->
		case N of
			0 -> [#fm{f=H, l=$$, sa=0}|Acc];
			J -> [#fm{f=H, l=lists:nth(J,X), sa=J}|Acc]
		end
										end,[],Ls)),
	rank_all(Index).
	

rank_all(FM) ->
	rank_all(FM,[],0,0,0,0,0).

rank_all([Rec=#fm{l=L}|FM],Acc,Dn,An,Cn,Gn,Tn) ->
	case L of
		$A -> rank_all(FM,[Rec#fm{d=Dn,a=An+1,c=Cn,g=Gn,t=Tn}|Acc],Dn,An+1,Cn,Gn,Tn);
		$C -> rank_all(FM,[Rec#fm{d=Dn,a=An,c=Cn+1,g=Gn,t=Tn}|Acc],Dn,An,Cn+1,Gn,Tn);
		$G -> rank_all(FM,[Rec#fm{d=Dn,a=An,c=Cn,g=Gn+1,t=Tn}|Acc],Dn,An,Cn,Gn+1,Tn);
		$T -> rank_all(FM,[Rec#fm{d=Dn,a=An,c=Cn,g=Gn,t=Tn+1}|Acc],Dn,An,Cn,Gn,Tn+1);
		$$ -> rank_all(FM,[Rec#fm{d=Dn+1,a=An,c=Cn,g=Gn,t=Tn}|Acc],Dn+1,An,Cn,Gn,Tn)
	end;
rank_all([],Acc,_,_,_,_,_) -> lists:reverse(Acc).
	

test(X) ->
	FM = fm(X),
	rank_all(FM).



sa(X) ->
	Ls = lists:sort(get_suffs(X)),
	[ N || {_,N} <- Ls].



get_suffs(X) ->
	get_suffs("",0,X++"$").

get_suffs(Acc, N, [H|X]) ->
	get_suffs([{[H|X],N}|Acc], N+1, X);
get_suffs(Acc,_,[]) -> Acc.



count_matches(Qsec, FM) ->
	[H|Tail] = lists:reverse(Qsec),
	count_matches(H,0,inf,Tail,FM).

count_matches(C, Low_bound, Upper_bound, Qsec, FM) ->
	0.




