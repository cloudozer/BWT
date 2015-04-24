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
		"21"
		%"GL000207.1",
		%"GL000226.1"
		%"GL000229.1",
		%"GL000231.1",
		%"GL000210.1",
		%"GL000239.1",
		%"GL000235.1",
		%"GL000201.1",
		%"GL000247.1",
		%"GL000245.1",
		%"GL000197.1"
	],
	t(Tests).

t([Chrom|Tests]) ->
	File = "bwt_files/human_g1k_v37_decoy.fasta",
	
	{Pos,Len} = msw:get_reference_position(Chrom,File),
	{_,Ref_seq} = msw:get_ref_seq(File,Pos,Len),
	io:format("~nCromosome:~p, Length:~p~n",[Chrom,length(Ref_seq)]),
	Str = append($$,Ref_seq),

	%io:format("~nCromosome:~p - ",[Chrom]),
	statistics(runtime),
	DSA = sa_seq(Str),
	{_,T1} = statistics(runtime),
	io:format("~n DSA: ~pms~n",[T1]),
	SA = bwt:sa(Ref_seq),
	{_,T2} = statistics(runtime),
	io:format(" SA: ~pms~n",[T2]),
	io:format("test passed:~p~n",[DSA==SA]),

	t(Tests);
t([]) -> ok.




append(S,Str) -> lists:reverse([S|lists:reverse(Str)]).



sa_seq(Str) ->
	Alphabet = "ACGT",
	Last = lists:nth(length(Str)-1,Str),
	io:format("Last char: ~c~n",[Last]),
	Keys = lists:sort(fun(A,B)-> A>B end,
		[ {Last,$$,$$} |[ {I,J,K} || I <- Alphabet, J <- Alphabet, K <- [$$|Alphabet] ]]),
	Index = lists:foldl(fun(Key,Acc1)-> 

					ChunkInd = get_chunk_sa(Key,Str),
					io:format("completed.~n"),
					lists:foldl(fun(S,Acc2)-> [S|Acc2] end,
					Acc1,ChunkInd)
					%io:format("complete~n")

				end, [], Keys),
	[length(Str)-1|Index].


get_chunk_sa({_,$$,$$},Str) -> [length(Str)-2];
get_chunk_sa(Key,Str) -> 
	io:format("Processing chunk ~p ... ",[Key]),
	Index = get_chunk_sa(Key,Str,0,[]),
	io:format(" selected ~p sufixes. Sorting ... ",[length(Index)]),
	Str_bin = list_to_binary(Str),
	Bin_index = fun(N) -> binary:at(Str_bin, N) end,
	
	F = fun(F,X1,X2) -> 
			Vx = Bin_index(X1), Vy = Bin_index(X2),
			case Vx =:= Vy of true -> F(F,X1+1,X2+1); _ -> Vx > Vy end
		end,
	
	Compare = fun(X1,X2) -> F(F,X1,X2) end,	
	lists:sort(Compare,Index).

get_chunk_sa({I,J,$$},[I,J,$$],N,Acc) -> [N|Acc];
get_chunk_sa({_,_,_},[_,_,$$],_,Acc) -> Acc;
get_chunk_sa({I,J,K},[I|[J,K|_]=Str],N,Acc) -> get_chunk_sa({I,J,K},Str,N+1,[N|Acc]);
get_chunk_sa(Key,[_|Str],N,Acc) -> get_chunk_sa(Key,Str,N+1,Acc).




sa(Str) ->
	Ps = scan(0,Str,dict:new()),
	io:format("Keys:~p~n",[dict:fetch_keys(Ps)]),
	io:format("Scanning is complete. ~p workers started~n",[dict:size(Ps)]),
	lists:foldl(fun(Key, Acc) -> 
		
		io:format("Processing branch ~p ... ",[Key]),
		dict:fetch(Key,Ps) ! finish,
		receive
			{Key,Index} -> 
				io:format("complete~n"),
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


