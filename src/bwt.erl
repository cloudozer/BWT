% A simple implementation of BWT
% 
% Cloudozer(c), 2014
%

-module(bwt).
-export([t/0,
		bwt/1,
		get_suffs/1,
		fm/1,
		sa/1,
		test/0,
		test_mm9/0
		]).

-include("bwt.hrl").

%-define(TRSH,0.8).

t() ->
	N = 20000,
	{Meta,FM} = fm_index:get_index("21",1),
	{Pc,Pg,Pt,Last} = proplists:get_value(pointers, Meta),
	T1 = os:timestamp(),
	t(T1,N,FM,Pc,Pg,Pt,Last,0).
t(T1,0,_FM,_Pc,_Pg,_Pt,_Last,Acc) ->
	T2 = os:timestamp(),
	io:format("Seeds finding took ~psec; ~p Seeds found~n",
		[timer:now_diff(T2,T1)/1000000, Acc]);
t(T1,N,FM,Pc,Pg,Pt,Last,Acc) ->
	Qseq = sw:rand_seq(150),
	Seeds = sga:sga(FM,Pc,Pg,Pt,Last,Qseq),
	t(T1,N-1,FM,Pc,Pg,Pt,Last,Acc+length(Seeds)).


test() ->
  %{Meta,FM} = get_index("GL000192.1"),
  {Meta,FM} = fm_index:get_index("GL000207.1",1),
  {Pc,Pg,Pt,Last} = proplists:get_value(pointers, Meta),
  Qs = [
  	"ACCCCACGTTTTTGGAGTTATATGTTGGCACTGATACTGGCCATAGAATTCCCTATGGTA",
    "ACCNNNNCCACGTTTTTGGAGTTATATGTTGATACTGGCCATAGAATTCCCTATNGGTA",
    "ACCCCACGTTTTTGGAGTTATATGTTGTTCTTGCACTGATACTGGCCATAGAATTCCCTATGGTA",
    
    "TAATCAGAACAGGTTTACAACATAAATAAATAGATTGAACTTACTTTGTATAAAAATTGT",

    "CTCAGCCTCCATAATTATGTGAACCAGTTCCCCTAATGAATCTTCTCTCATCTGTCTACA",
    "TATATCCTATTGATTCTGCCTTTCTGGAGACCCCTGACTAATGTGATTACAATAACTACA",
    "CAATTCACTAGTTTATATAGAAGACTTGGTTTTTGTCTTTGCCCCATTTTATATTTGTAT",
    "TATAACTATGTATCTGGAAAATGGAACAAGTTTTTTCTTCTTCATATGAGGGCTAAGGCT",
    "TTTTTCTCACCAATATTTTTGGAGATTTTAAAGATTTTCTTTTTTTTTGACATAGAATCT",
    "TATGGAGGCTGAGAAATAATTTTTTTTCTATTTTATTCTTCAGCCCCAGGTGTTTGCTTT",
    "TGCAGATTCTTGAGCACACTGAGAGCCTCCAAGGCATGGAGTGGGGTGCCTGAAGTTTCA",
    "CAATATCATCAGAAGCCTATAACCAGAGTACCTGTCATAGTCTTTTCTGTGAGTCTCAGA",
    "GGGAGTCCTGTCTTGGAGACGAACATTCTGACCTGTAGTTGATTGCAGGAGCTTTCAGGA",
    "AAGCATCAGGGGGAAATAATATCTAAATGACGAAAAGTATGAAATGGCTGTGATGAAAGA",
    "TCTGATGAGAGTTCATTATACCACAACTGACAAGGATATTCGATTTTTTCTGTGGCAGAC",
    "AACATTTATTTATTTCTTTATTTAGAGACAGAGTCTTGCTCTGTCGCCCAGGCTGGAGTG",
    "CAGCGGTGCGATCTCGGCTCACTGCAAGCTCTGTCTCCTGGGTTCACGCCATTCTCCTGC",
    "CTCAGCCTCCCGAGTAGCTGGGACTACAGGTGCCTGCCATCACGCCCGGCTAATTTTTTA",
    "TATTTTTAATAGAGATGGAGATTCACCGGGTTAGCCAGGATGGTCTGGATCTCCTGACCT",
    "TGTGATCCACCCGCCTCAGCCTCCTAAAGTGCTGGGATTATAGGCATGAGCCACTGTGCC",
    "TGGCACAACATTTAAAGTAATAATTGGAATTATGACTCATTACTCTATAGTGGCACATAG",
    "CATGGATAAGGAGGACATTGACAAACTTCCAGGAATTTTATATAATTTCTGAAAACATAA",
    "CATTTTACCCATACAAATATAACACAGGGAAGGTTAGGTATCTCTTTTTATTTGTATCTT",
    "CTGTATGGTTTTCCTTATAAAAAATGCAACCTACTTTACTTGCGAAACATGCCCTACTTT",
    "TCTTGCATGCTTTGCATAGAGTTGTTTCTAGTTATTCTATTATTTCTAGTAGTTTTATTT",
    "ACATATATTGATTATAATTTTAATACTTAGTAATCTTTTATTTTCCAGAGAAAACTAGGA",
    "AGTAGACAGTTATAAACTGTCATATATTAGCATTCTATAGTAGGTTAGAAAATGTATGAA",
    "TATACCATCTCCCAACATCTAGAGGGATGTGTTTTCTCATAATACAATTCCTCAGTGTGG",
    "CAGAAAAAAACATGTTTATTAACGGGCCAAAATATCTTTAGTCTCTCTGTAAAAACAGGA",
    "AGCCAAAAGTATATAAACTTGAATTATTTATGTTCAGTAATTAATGTTTTAGTATTGTAT",
    "CTTATTTATAAATGGTCTAGATATTTAATGCAAATCTTTTACTTAGCTTAACTTTAAGGT",
    "TAAAAATTACCAAAAGTACTTTGGAAACTATTCTTAGGCAGATTTACTGTAAACAAATTA"
  ],

  lists:foreach(fun(Qseq) -> {T,_}=timer:tc(sga,sga,[FM,Pc,Pg,Pt,Last,Qseq]), io:format("~p <--> ~b~n", [Qseq,T]) end, Qs).


test_mm9() ->
  {Meta,FM} = fm_index:get_index("chr17",1),
  {Pc,Pg,Pt,Last} = proplists:get_value(pointers, Meta),
  Qs = [
		"TCTCCCATCATTCATTATACCTTTTCTGCACCCCTG",
		"GGGTGGTTGTAAACAGCAGGCAGATGTGAGAAATGC",
		"GACTCCCTAAACTTCCTTCTGAGGGACACAAATCAA",
		"GCGGGGTGGGGAGCTTGGAGCCAGCTGTAGGCAGGG",
		"TACTAGTGGCACCTCCCTGGGCTAAGACTCGGCTAG"
  ],

  lists:foreach(fun(Qseq) -> {T,Seeds}=timer:tc(sga,sga,[FM,Pc,Pg,Pt,Last,Qseq]), io:format("~p <--> ~b, Seeds: ~p~n", [Qseq,T,Seeds]) end, Qs).




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
	%SA = st:sa_seq(X),
	%SA = dsa:dsa(X),
	SA = suff_arr_master:main(X,7),
	%io:format("~p~n~p~n",[SA,length(SA)]),
	
	{_,T2} = statistics(runtime),
	io:format("Suffix array generation took: ~psec~n",[T2/1000]),
	{Dq,Aq,Cq,Gq,Tq} = fm(SA,0,[],[],[],[],[]),
	%io:format("~p~n",[FM]),
	{_,T3} = statistics(runtime),
	io:format("Building the queues took ~p sec~n",[T3/1000]),

	%list_to_tuple(add_indices(FM,[],Dq,Aq,Cq,Gq,Tq)).
	list_to_tuple(fmi:assemble_index(SA,[],0,Dq,Aq,Cq,Gq,Tq)).
	
	%{_,T4} = statistics(runtime),
	%io:format("Building the index took ~p sec~n",[T4/1000]).



fm([{F,_,_}|SA],K, Dq,Aq,Cq,Gq,Tq) ->
	case F of
		$A -> fm(SA,K+1,Dq,[K|Aq],Cq,Gq,Tq);
		$C -> fm(SA,K+1,Dq,Aq,[K|Cq],Gq,Tq);
		$G -> fm(SA,K+1,Dq,Aq,Cq,[K|Gq],Tq);
		$T -> fm(SA,K+1,Dq,Aq,Cq,Gq,[K|Tq]);
		$$ -> fm(SA,K+1,[K|Dq],Aq,Cq,Gq,Tq)
	end;
fm([],_, Dq,Aq,Cq,Gq,Tq) -> 
	{
	lists:reverse(Dq),
	lists:reverse(Aq),
	lists:reverse(Cq),
	lists:reverse(Gq),
	lists:reverse(Tq)
	}.
				



sa(X) -> [ N || {_,N,_} <- lists:sort(get_suffs(X)) ].



get_suffs(X) ->
	X1 = lists:reverse([$$|lists:reverse(X)]),
	get_suffs([],0,X1,$$).

get_suffs(Acc, N, [H|X],P) ->
	get_suffs([{[H|X],N,P}|Acc], N+1, X, H);
get_suffs(Acc,_,[],_) -> 
	%io:format("Suffices:~n~p~n",[Acc]),
	Acc.


