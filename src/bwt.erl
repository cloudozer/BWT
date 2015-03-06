% A simple implementation of BWT
% 
% Cloudozer(c), 2014
%

-module(bwt).
-export([bwt/1,
		get_suffs/1,
		fm/1,
		sa/1,
		count_matches/2
		]).

%-record(fm,{f,l,d,a,c,g,t,sa}).



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


%% returns an FM index for a given reference sequence
fm(X) ->
	Ls = lists:sort(get_suffs(X)),
	%io:format("Sufs:~p~n",[Ls]),
	{FM, Dq, Aq, Cq, Gq, Tq} = fm(X,Ls,[],1,{[],[],[],[],[]}),
	list_to_tuple(add_indices(FM,[],Dq,Aq,Cq,Gq,Tq)).


fm(X,[{[S|_],N}|Ls], Acc, K, {Dq,Aq,Cq,Gq,Tq}) ->
	case N of
		0 -> Acc1 = [{S,$$,0}|Acc];
		J -> Acc1 = [{S,lists:nth(J,X),J}|Acc]
	end,
	case S of
		$A -> fm(X,Ls,Acc1,K+1,{Dq,[K|Aq],Cq,Gq,Tq});
		$C -> fm(X,Ls,Acc1,K+1,{Dq,Aq,[K|Cq],Gq,Tq});
		$G -> fm(X,Ls,Acc1,K+1,{Dq,Aq,Cq,[K|Gq],Tq});
		$T -> fm(X,Ls,Acc1,K+1,{Dq,Aq,Cq,Gq,[K|Tq]});
		$$ -> fm(X,Ls,Acc1,K+1,{[K|Dq],Aq,Cq,Gq,Tq})
	end;
fm(_,[], Acc, _, {Dq,Aq,Cq,Gq,Tq}) -> 
	{lists:reverse(Acc),
	lists:reverse(Dq),
	lists:reverse(Aq),
	lists:reverse(Cq),
	lists:reverse(Gq),
	lists:reverse(Tq)
	}.
				

add_indices([{F,L,SA}|FM],Acc,Dq,Aq,Cq,Gq,Tq) ->
	case L of
		$A -> 
			[I|Aq1] = Aq, 
			add_indices(FM,[{F,L,I,SA}|Acc],Dq,Aq1,Cq,Gq,Tq);
		$C -> 
			[I|Cq1] = Cq, 
			add_indices(FM,[{F,L,I,SA}|Acc],Dq,Aq,Cq1,Gq,Tq);
		$G -> 
			[I|Gq1] = Gq, 
			add_indices(FM,[{F,L,I,SA}|Acc],Dq,Aq,Cq,Gq1,Tq);
		$T -> 
			[I|Tq1] = Tq, 
			add_indices(FM,[{F,L,I,SA}|Acc],Dq,Aq,Cq,Gq,Tq1);
		$$ -> 
			[I|Dq1] = Dq, 
			add_indices(FM,[{F,L,I,SA}|Acc],Dq1,Aq,Cq,Gq,Tq)
	end;
add_indices([],Acc,[],[],[],[],[]) -> lists:reverse(Acc).
	



sa(X) ->
	Ls = lists:sort(get_suffs(X)),
	[ N || {_,N} <- Ls].



get_suffs(X) ->
	get_suffs("",0,X++"$").

get_suffs(Acc, N, [H|X]) ->
	get_suffs([{[H|X],N}|Acc], N+1, X);
get_suffs(Acc,_,[]) -> Acc.



%% returns the list of matches of the given Qsec sequence against 
%% the reference sequence represented as FM index
count_matches(Qsec, FM) ->
	[H|Tail] = lists:reverse(Qsec),
	case count_matches(H,1,size(FM),Tail,FM,size(FM)) of
		no_matches -> 0;
		{Sp,Se} -> 1+Se-Sp
	end.

count_matches(C2, Sp, Ep, [C1|Qsec], FM, Size) ->
	Sp1 = search_down(Sp,C2,C1,FM,Size),
	Ep1 = search_up(Ep,C2,C1,FM),
	%io:format("New range:~p - ~p~n",[Sp1,Ep1]),
	case Sp1 =< Ep1 of
		true -> count_matches(C1, Sp1, Ep1, Qsec, FM, Size);
		_ -> no_matches
	end;

count_matches(_, Sp, Ep, [], _, _) ->
	%io:format("We got matches from ~p to ~p~n",[Sp,Ep]),
	{Sp,Ep}.


search_down(Size,_,_,_,Size) -> 
	%io:format("Match not found when going down~n"),
	Size;
search_down(Sp,C2,C1,FM,Size) ->
	Tup = {F,L,_,_} = element(Sp,FM),
	case {F,L} of
		{C2,C1} -> element(3,Tup);
		_ ->
			%io:format("{~p,~p} did not match~n",[F,L]), 
			search_down(Sp+1,C2,C1,FM,Size)
	end.

search_up(1,_,_,_) -> 
	%io:format("Match not found when going up~n"),
	1;
search_up(Se,C2,C1,FM) ->
	Tup = {F,L,_,_} = element(Se,FM),
	case {F,L} of
		{C2,C1} -> element(3,Tup);
		_ -> 
			%io:format("{~p,~p} did not match~n",[F,L]), 
			search_up(Se-1,C2,C1,FM)
	end. 


