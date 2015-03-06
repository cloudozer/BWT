% A simple implementation of BWT
% 
% Cloudozer(c), 2014
%

-module(bwt).
-export([bwt/1,
		get_suffs/1,
		fm/1,
		sa/1,
		find_match/2
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
find_match(Qsec, FM) ->
	[H|Tail] = lists:reverse(Qsec),
	%initialize_sp_ep(H,1,size(FM),FM)
	case find_match(H,1,size(FM),Tail,FM) of
		no_match -> [];
		{Sp,Se} -> get_pos([],Sp,Se,FM)
	end.

find_match(C1, Sp, Ep, [C2|Qsec], FM) ->
	io:format("Looking for {~p,~p}~n",[C1,C2]),
	io:format("Range:[~p - ~p]~n",[Sp,Ep]),
	case se(Sp,not_found,Ep,not_found,C1,C2,FM) of
		not_found -> no_match;
		{Sp1,Ep1} -> find_match(C2, Sp1, Ep1, Qsec, FM)
	end;

find_match(_, Sp, Ep, [], _) -> {Sp,Ep}.


se(Sp,found,Ep,found,_,_,FM) -> 
	{_,_,Sp1,_} = element(Sp,FM),
	{_,_,Ep1,_} = element(Ep,FM),
	{Sp1,Ep1};
se(Sp,State1,Ep,State2,C1,C2,FM) ->
	case State1 of
		not_found ->
			{State11, Sp1} = get_state(Sp,down,C1,C2,FM);
		found -> State11 = found, Sp1 = Sp
	end,
	case State2 of
		not_found ->
			{State22, Ep1} = get_state(Ep,up,C1,C2,FM);
		found -> State22 = found, Ep1 = Ep
	end,
	io:format("(~p,~p), (~p,~p)~n",[Sp1,State11,Ep1,State22]),
	
	case Sp1 > Ep1 of
		true -> not_found;
		_ -> se(Sp1,State11,Ep1,State22,C1,C2,FM)
	end. 


get_state(P,Dir,C1,C2,FM) ->
	{F,L,_,_} = element(P,FM),
	case {F,L} of
		{C1,C2} -> {found,P};
		_ ->
			case Dir of
				down -> {not_found,P+1};
				up -> {not_found,P-1}
			end
	end.



% returns a list of SA positions where the matches happened
get_pos(Acc,Ep,Ep,FM) -> 
	{_,_,_,SA} = element(Ep,FM),
	[SA|Acc];
get_pos(Acc,Sp,Ep,FM) ->
	{_,_,_,SA} = element(Sp,FM),
	get_pos([SA|Acc],Sp+1,Ep,FM).

