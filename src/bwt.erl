% A simple implementation of BWT
% 
% Cloudozer(c), 2014
%

-module(bwt).
-export([bwt/1,
		get_suffs/1,
		fm/1,
		sa/1,
		find_match/2,
		make_index/0,
		test/0
		]).

-define(TRSH,0.8).
-define(RANGE,10).


%-record(fm,{f,l,d,a,c,g,t,sa}).

test() ->
	File = "../bwt_files/human_g1k_v37_decoy.fasta",
	%"GGCTCTGTTAGAGTAGATAGCTAGCTAGACATGAACAGGAGGGGGAGCTCCTGGAAAAGG",
	%"GAAAGTCTGTGAAGGCTCACCTGGAGGGACCACCAAAAATGCACATATTAGTAGCATCTC",
	%"TAGTGCTGGAGTGGATGGGCACTTGTCAATTGTGGTTAGGAGGGAGAAGAGGTACCTACG",
	%"CAGAAACACCCTAGAACTTCTCTTAAGGTGCCCCAATCGGATGGGCGCGGTGGCTCACGC",
	%"CTGTAATCCCAGCACTTTGGGAGGCCGAGGCGGGTGGATCATGAGGTCAGGAGATCGAGA",
	Qseq = "CCATCCTGGCTAACAAGGTGAAACCCCGTCTCTACTAAAAATACAAAAAATTAGCCGGGC",
	FM = get_index(),
	Pos = get_subseq(Qseq),
	Subseq = lists:sublist(Qseq,length(Qseq)-Pos),
	io:format("Subseq: ~p, Pos: ~p ~n",[Subseq,Pos]),
	Seeds = [ X-length(Qseq)+Pos || X<-find_match(Subseq,FM) ],
	io:format("~p seeds found~n~p~n",[length(Seeds),Seeds]),
	
	{Ref_pos,Len} = msw:get_reference_position("21", File),
	Ref = msw:get_chunk(File,Ref_pos+Len div 2,Len div 10),
	io:format("~p~n",[Qseq]),
	io:format("~s~n",[[$-|| _ <- lists:seq(0,60)]]),
	lists:foreach(fun(P)-> 
		io:format("~p~n",[lists:sublist(Ref,P,60)])
				end, Seeds).

	
make_index() ->
	File = "../bwt_files/human_g1k_v37_decoy.fasta",
	{Pos,Len} = msw:get_reference_position("21",File),

	Bin = term_to_binary(fm(msw:get_chunk(File,Pos+Len div 2,Len div 10))),
	file:write_file("../bwt_files/fm_index",Bin).


get_index() ->
	{ok,Bin} = file:read_file("../bwt_files/fm_index"),
	binary_to_term(Bin).



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
	_ = statistics(runtime),
	Ls = lists:sort(get_suffs(X)),
	{_,T2} = statistics(runtime),
	io:format("Suffix array generation took: ~psec~n",[T2/1000]),
	%io:format("Sufs:~p~n",[Ls]),
	{FM, Dq, Aq, Cq, Gq, Tq} = fm(X,Ls,[],1,[],[],[],[],[]),
	{_,T3} = statistics(runtime),
	io:format("Building the queues took ~p sec~n",[T3/1000]),

	list_to_tuple(add_indices(FM,[],Dq,Aq,Cq,Gq,Tq)).
	%{_,T4} = statistics(runtime),
	%io:format("Building the index took ~p sec~n",[T4/1000]).



fm(X,[{[S|_],N,P}|Ls], Acc, K, Dq,Aq,Cq,Gq,Tq) ->
	%case N of
		%%%%%%%%%%%  {F,L,SA}
	%	0 -> Acc1 = [{S,$$,P}|Acc];
	%	J -> Acc1 = [{S,lists:nth(J,X),J}|Acc]
	%end,
	case S of
		$A -> fm(X,Ls,[{S,P,N}|Acc],K+1,Dq,[K|Aq],Cq,Gq,Tq);
		$C -> fm(X,Ls,[{S,P,N}|Acc],K+1,Dq,Aq,[K|Cq],Gq,Tq);
		$G -> fm(X,Ls,[{S,P,N}|Acc],K+1,Dq,Aq,Cq,[K|Gq],Tq);
		$T -> fm(X,Ls,[{S,P,N}|Acc],K+1,Dq,Aq,Cq,Gq,[K|Tq]);
		$$ -> fm(X,Ls,[{S,P,N}|Acc],K+1,[K|Dq],Aq,Cq,Gq,Tq)
	end;
fm(_,[], Acc, _, Dq,Aq,Cq,Gq,Tq) -> 
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
	[ N || {_,N,_} <- Ls].



get_suffs(X) ->
	get_suffs([],0,X++"$",$$).

get_suffs(Acc, N, [H|X],P) ->
	get_suffs([{[H|X],N,P}|Acc], N+1, X, H);
get_suffs(Acc,_,[],_) -> Acc.



%% returns the list of matches of the given Qseq sequence against 
%% the reference sequence represented by FM index
find_match(Qseq, FM) ->
	[H|Tail] = lists:reverse(Qseq),
	%initialize_sp_ep(H,1,size(FM),FM)
	case find_match(H,1,size(FM),Tail,FM) of
		no_match -> [];
		{Sp,Se} -> get_pos([],Sp,Se,FM)
	end.

find_match(C1, Sp, Ep, [C2|Qseq], FM) ->
	%io:format("Looking for {~p,~p}~n",[C1,C2]),
	io:format("Range:[~p - ~p]~n",[Sp,Ep]),
	case se(Sp,not_found,Ep,not_found,C1,C2,FM) of
		not_found -> {Sp,Ep};
		{Sp1,Ep1} when Ep1-Sp1 > ?RANGE -> find_match(C2, Sp1, Ep1, Qseq, FM);
		{Sp1,Ep1} -> {Sp1,Ep1} 
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
	%io:format("(~p,~p), (~p,~p)~n",[Sp1,State11,Ep1,State22]),
	
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



get_subseq(Qseq) -> get_subseq(lists:reverse(Qseq), [], 0).

get_subseq(_,Queue,Pos) when length(Queue) == 9 -> Pos;
get_subseq([_],_,_) -> not_found;
get_subseq([X1,X2|Seq], Queue, Pos) ->
	%io:format("{~p,~p}, Q: ~p~n",[X1,X2,Queue]),
	case lists:keyfind({X1,X2},1,Queue) of
		false -> get_subseq([X2|Seq], [{{X1,X2},1}|Queue], Pos);
		_ -> 
			{Queue1,Pos1} = remove(X1,X2, lists:reverse([{{X1,X2},1}|Queue]), 1 ),
			get_subseq([X2|Seq], Queue1, Pos+Pos1)
	end.


remove(X1,X2, [{{X1,X2},1}|Ls], N) -> {lists:reverse(Ls),N};
remove(X1,X2, [_|Ls], N) -> remove(X1,X2, Ls, N+1).
