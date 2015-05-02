% distributed Suffix Tree construction
%
% 
% Cloudozer(c), 2015
%

-module(st).
-export([
		t/0,
		sa/1,
		sa_seq/1,
		st/4,
		append/2
		]).



t() ->
	Tests = [
		%"21"
		"GL000207.1",
		"GL000226.1",
		"GL000229.1",
		"GL000231.1",
		%"GL000210.1",
		"GL000239.1",
		"GL000235.1",
		"GL000201.1",
		"GL000247.1",
		"GL000245.1",
		%"GL000197.1",
		"GL000192.1",
		"GL000193.1"
	],
	t(Tests).

t([Chrom|Tests]) ->
	File = "bwt_files/human_g1k_v37_decoy.fasta",
	
	{Pos,Len} = msw:get_reference_position(Chrom,File),
	{_,Ref_seq} = msw:get_ref_seq(File,Pos,Len),
	find_other_chars(Ref_seq),
	io:format("~nCromosome:~p, Length:~p~n",[Chrom,length(Ref_seq)]),
	Str = append($$,Ref_seq),

	%io:format("~nCromosome:~p - ",[Chrom]),
	statistics(runtime),
	DSA = [ N ||{_,_,N} <- dsa:dsa(Str) ],
	{_,T1} = statistics(runtime),
	io:format(" DSA: ~pms~n",[T1]),
	SA = bwt:sa(Ref_seq),
	{_,T2} = statistics(runtime),
	io:format(" SA: ~pms~n",[T2]),
	io:format("test passed: ~p~n",[DSA==SA]),

	t(Tests);
t([]) -> ok.




append(S,Str) -> lists:reverse([S|lists:reverse(Str)]).



sa_seq(Str) ->
	Alphabet = "ACGT",
	[$$,L1,L2|_] = lists:reverse(Str),
	Keys = lists:sort(fun(A,B)-> A>B end,
		[ {L1,$$,$$,$$}, {L2,L1,$$,$$} |[ {I,J,K,M} || I <- Alphabet, J <- Alphabet, 
									K <- Alphabet, M <- [$$|Alphabet] ]]),
	Str_bin = list_to_binary(Str),

	[ {$$,L1,length(Str)-1} | lists:foldl(fun(Key,Acc1) -> 

					lists:foldl(fun(Row,Acc2) -> [Row|Acc2] end, Acc1, get_chunk_sa(Key,Str,Str_bin))
					
											end, [], Keys)
		].



get_chunk_sa({L1,$$,$$,$$},Str,_) -> 
	[$$,L1,L2|_] = lists:reverse(Str),
	io:format("completed~nProcessing ~c$ -",[L1]),
	[{L1,L2,length(Str)-2}];
get_chunk_sa({L2,L1,$$,$$},Str,_) -> 
	[$$,L1,L2,L3|_] = lists:reverse(Str),
	io:format("completed~nProcessing ~c~c$ -",[L2,L1]),
	[{L2,L3,length(Str)-3}];
get_chunk_sa({L3,L2,L1,$$},Str,_) -> 
	[$$,C1,C2,C3,C4|_] = lists:reverse(Str),
	case C1=:=L1 andalso C2=:=L2 andalso C3=:=L3 of
		true ->
			io:format("completed~nProcessing ~c~c~c$ -",[L3,L2,L1]), 
			[{L3,C4,length(Str)-4}];
		_ -> []
	end;
get_chunk_sa({C1,C2,C3,C4}=Key,Str,Str_bin) -> 

	io:format("completed~nProcessing ~c~c~c~c...",[C1,C2,C3,C4]),
	Index = get_chunk_sa(Key,$$,Str,0,[]),
	io:format(" ~p suffixes selected. Sorting... ",[length(Index)]),
	Bin_index = fun(N) -> binary:at(Str_bin, N) end,
	
	F = fun(F,X1,X2) -> 
			Vx = Bin_index(X1), Vy = Bin_index(X2),
			case Vx =:= Vy of true -> F(F,X1+1,X2+1); _ -> Vx > Vy end
		end,
	
	Compare = fun({_,_,X1},{_,_,X2}) -> F(F,X1,X2) end,	
	lists:sort(Compare,Index).

get_chunk_sa({I,J,K,$$},L,[I,J,K,$$],N,Acc) -> [{I,L,N}|Acc];
get_chunk_sa({_,_,_,_},_,[_,_,_,$$],_,Acc) -> Acc;
get_chunk_sa({I,J,K,M},L,[I|[J,K,M|_]=Str],N,Acc) -> get_chunk_sa({I,J,K,M},I,Str,N+1,[{I,L,N}|Acc]);
get_chunk_sa(Key,_,[L|Str],N,Acc) -> get_chunk_sa(Key,L,Str,N+1,Acc).




sa(Str) ->
	Ps = scan(0,Str,dict:new()),
	io:format("Keys:~p~n",[dict:fetch_keys(Ps)]),
	io:format("Scanning is complete. ~p workers started~n",[dict:size(Ps)]),
	lists:foldl(fun(Key, Acc) -> 
		
		io:format("Processing branch ~p... ",[Key]),
		dict:fetch(Key,Ps) ! finish,
		receive
			{Key,Index} -> 
				io:format("done~n"),
				lists:foldl(fun(S,AccI) -> [S|AccI] end, Acc,lists:reverse(Index))
		end
				end, [], lists:sort(fun(A,B)-> A>B end, dict:fetch_keys(Ps))).



scan(N,<<I:8,$$>>,Ps) ->
	%io:format("Msg:~p~n",[N]),
	Pid1 = spawn(?MODULE,st,[self(),<<>>,N,{I,$$}]), Pid1 ! N,
	Pid2 = spawn(?MODULE,st,[self(),<<>>,N+1,{$$}]), Pid2 ! N+1,
	dict:store({$$},Pid2,dict:store({I,$$},Pid1,Ps));
	
scan(N,<<I:8,J:8,Str/bits>>,Ps) ->
	%io:format("Msg:~p~n",[N]),
	case dict:is_key({I,J},Ps) of
		true -> 
			dict:fetch({I,J},Ps) ! N,
			scan(N+1,<<J:8,Str/bits>>,Ps);
	
		_ ->
			Pid = spawn(?MODULE,st,[self(),Str,N,{I,J}]),
			Ps1 = dict:store({I,J},Pid,Ps),
			Pid ! N,
			scan(N+1,<<J:8,Str/bits>>,Ps1)
	end.
	






st(Master,Str,Shift,Key) -> 
	st(Master,Str,Shift,Key,[]).

st(Master,Str,Shift,Key,Acc) ->
	Bin_index = fun(N) -> binary:at(Str, N-Shift) end,
	
	F = fun(F,X1,X2) -> 
			Vx = Bin_index(X1), Vy = Bin_index(X2),
			case Vx =:= Vy of true -> F(F,X1+1,X2+1); _ -> Vx < Vy end
		end,
	
	Compare = fun(X1,X2) -> F(F,X1,X2) end,

	receive
		finish -> Master ! {Key,lists:sort(Compare,Acc)};
		J -> st(Master,Str,Shift,Key,[J|Acc])
	end.



find_other_chars([$A|Str]) -> find_other_chars(Str);
find_other_chars([$C|Str]) -> find_other_chars(Str);
find_other_chars([$G|Str]) -> find_other_chars(Str);
find_other_chars([$T|Str]) -> find_other_chars(Str);
find_other_chars([Char|Str]) ->
	io:format("Found: ~c~n",[Char]),
	find_other_chars(Str);
find_other_chars([]) -> ok.

	

