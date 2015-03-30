% A simple implementation of BWT
% 
% Cloudozer(c), 2014
%

-module(bwt).
-export([bwt/1,
		get_suffs/1,
		fm/1,
		sa/1,
		make_index/0, make_index/1,
		get_subseq/1,
		index_to_sequence/3,
		get_3_pointers/1,
		get_index/0,
		pp/1,
		test/1,test/0
		]).

%-define(TRSH,0.8).


%-record(fm,{f,l,d,a,c,g,t,sa}).

test() ->
  FM = get_index(),
  F = fun()-> 
    Qs = [
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
    lists:foreach(fun(Qseq) -> sga:sga(FM,Qseq) end, Qs)
  end,
  {T,V} = timer:tc(F),
  io:format("tt ~p~n", [T]).
  %eflame:apply(F, []).

test(Qseq) ->
	%File = "../bwt_files/human_g1k_v37_decoy.fasta",
	%"CTCAGCCTCCATAATTATGTGAACCAGTTCCCCTAATGAATCTTCTCTCATCTGTCTACA",
	%"TATATCCTATTGATTCTGCCTTTCTGGAGACCCCTGACTAATGTGATTACAATAACTACA",
	%CAATTCACTAGTTTATATAGAAGACTTGGTTTTTGTCTTTGCCCCATTTTATATTTGTAT
	%TATAACTATGTATCTGGAAAATGGAACAAGTTTTTTCTTCTTCATATGAGGGCTAAGGCT
	%TTTTTCTCACCAATATTTTTGGAGATTTTAAAGATTTTCTTTTTTTTTGACATAGAATCT
	%TATGGAGGCTGAGAAATAATTTTTTTTCTATTTTATTCTTCAGCCCCAGGTGTTTGCTTT
	%TGCAGATTCTTGAGCACACTGAGAGCCTCCAAGGCATGGAGTGGGGTGCCTGAAGTTTCA
	FM = get_index(),
	{Time,Value} = timer:tc(sga,sga,[FM,Qseq]),
	io:format("time:~pusec~n",[Time]),
	Value.
	

% returns a subsequence of N characters from a reference sequence
% represented by FM-index starting from a given point <Start> 
index_to_sequence(Start,FM,N) -> "".



	
make_index() ->
	File = "../bwt_files/human_g1k_v37_decoy.fasta",
	{Pos,Len} = msw:get_reference_position("21",File),
	io:format("Pos:~p, Len:~p~n",[Pos,Len]),
	Chunk = msw:get_chunk(File,Pos+3*(Len div 4),(Len div 4) - 13000),
	io:format("Chunk len:~p~n",[length(Chunk)]),
	Bin = term_to_binary(fm(Chunk)),
	file:write_file("../bwt_files/fm_index",Bin).


make_index(Chrom) ->
	File = "../bwt_files/human_g1k_v37_decoy.fasta",
	{Pos,Len} = msw:get_reference_position(Chrom,File),
	{Shift,Ref_seq} = msw:get_ref_seq(File,Pos,Len),
	io:format("Removed ~p 'NNN' in the beginning~n",[Shift]),
	io:format("Length of the ref genome: ~p~n",[length(Ref_seq)]),
	statistics(runtime),
	Ref_seq1 = lists:map(fun($N)->$A;
							($B)->$C;
							($D)->$G;
							($R)->$A;
							($Y)->$C;
							($K)->$T;
							($M)->$A;
							($S)->$C;
							($W)->$A;
							($V)->$A;
							($A)->$A;
							($C)->$C;
							($G)->$G;
							($T)->$T
						end, Ref_seq),
	{_,T1} = statistics(runtime),
	io:format("Maping takes: ~pms~n",[T1]),
	io:format("Ref seq:~n~p~n",[Ref_seq1]),
	Bin = term_to_binary(fm(Ref_seq1)),
	file:write_file("../bwt_files/"+Chrom+".fm",Bin).



get_index() ->
	{ok,Bin} = file:read_file("../bwt_files/fm_index"),
	binary_to_term(Bin).


get_3_pointers(FM) ->
	Pc = find_pointer(FM,$C,2),
	Pg = find_pointer(FM,$G,Pc+1),
	Pt = find_pointer(FM,$T,Pg+1),
	{Pc,Pg,Pt}.


find_pointer(FM,Char,P) ->
	{F,_,_,_} = element(P,FM),
	case F of
		Char -> P;
		_ -> find_pointer(FM,Char,P+1)
	end.


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
	Ls = sort_chuncks(get_suffs(X),100000),
	{_,T2} = statistics(runtime),
	io:format("Suffix array generation took: ~psec~n",[T2/1000]),
	%io:format("Sufs:~p~n",[Ls]),
	{FM,Dq,Aq,Cq,Gq,Tq} = fm(X,Ls,[],1,[],[],[],[],[]),
	{_,T3} = statistics(runtime),
	io:format("Building the queues took ~p sec~n",[T3/1000]),

	list_to_tuple(add_indices(FM,[],Dq,Aq,Cq,Gq,Tq)).
	%{_,T4} = statistics(runtime),
	%io:format("Building the index took ~p sec~n",[T4/1000]).



fm(X,[{[S|_],N,P}|Ls], Acc, K, Dq,Aq,Cq,Gq,Tq) ->
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
				

sort_chuncks(Ls,Size) -> sort_chuncks(Ls,Size,[]).
sort_chuncks(Ls,Size,Acc) when length(Ls) > Size ->
	{Chunk,Tail} = lists:split(Size,Ls),
	io:format("Chunk sorted\t"),
	sort_chuncks(Tail,Size,[lists:sort(Chunk)|Acc]);
sort_chuncks(Ls,Size,Acc) ->
	[lists:sort(Ls)|Acc],
	lists:merge(Acc).


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
	X1 = lists:reverse([$$|lists:reverse(X)]),
	get_suffs([],0,X1,$$).

get_suffs(Acc, N, [H|X],P) ->
	get_suffs([{[H|X],N,P}|Acc], N+1, X, H);
get_suffs(Acc,_,[],_) -> 
	%io:format("Suffices:~n~p~n",[Acc]),
	Acc.




% returns a position referenced from the end of the query sequence, which is a good pattern for seeds
get_subseq(Qseq) -> get_subseq(lists:reverse(Qseq), [], 0).

get_subseq(_,Queue,Pos) when length(Queue) == 7 -> Pos;
get_subseq([_],_,_) -> not_found;
get_subseq([X1,X2|Seq], Queue, Pos) when X1==$C; X1==$G; X2==$C; X2==$G ->
	get_subseq([X2|Seq], [{{X1,X2},1}|Queue], Pos);
get_subseq(_,_,Pos) when Pos >= 7 -> 1;
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


pp(FM) -> pp(FM, 1, size(FM)).

pp(FM,N,N) -> 
	{F,L,P,SA} = element(N,FM),
	io:format("~c ~c\t~p\t~p~n",[F,L,P,SA]);
pp(FM,J,N) ->
	{F,L,P,SA} = element(J,FM),
	case {F,L} of
		{$N,$N} -> ok;
		_ ->
			io:format("~c ~c\t~p\t~p~n",[F,L,P,SA])
	end,
	pp(FM,J+1,N).
