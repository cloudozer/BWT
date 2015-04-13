% An implementation of OSACA algorithm
% invented by Ge Nong
% 
% Cloudozer(c), 2015
%

-module(osaca).
-export([t/0,
		t1/0,
		t2/0,
		t3/0,
		sais/1,
		sa/1,
		random_seq/2
		]).

t() ->
	File = "../bwt_files/human_g1k_v37_decoy.fasta",
	Chrom = "GL000192.1",
	%Chrom = "21",
	{Pos,Len} = msw:get_reference_position(Chrom,File),
	statistics(runtime),
	{Shift,Ref_seq} = msw:get_ref_seq(File,Pos,Len),
	{_,T0} = statistics(runtime),
	io:format("~nCromosome:~p, Length:~p~n",[Chrom,length(Ref_seq)]),
	io:format("Reading from file:~ps~n",[T0/1000]),
	
	%Bin = list_to_binary("AGTGTACGTGACCTAAATAGTAAACTTGCGCAG$"),
	Bin = list_to_binary(append($$,Ref_seq)),
	Bin_index = fun(N) -> 
		J = N-1,
		<<_:J/bytes,Y:1/bytes,_/binary>> = Bin,
		Y
				end,

	F = fun(F,X1,X2) -> 
		Vx = Bin_index(X1),
		Vy = Bin_index(X2),
		case Vx =:= Vy of
			true -> F(F,X1+1,X2+1);
			_ -> Vx < Vy
		end
				end,

	Compare = fun(X1,X2) -> F(F,X1,X2) end,

	statistics(runtime),
	SA = lists:sort(Compare,lists:seq(1,size(Bin))),
	{_,T1} = statistics(runtime),
	io:format("Suffix array built for ~pms~n",[T1]),
	bwt:sa(Ref_seq),
	{_,T2} = statistics(runtime),
	io:format("Simple algo took: ~pms~n",[T2]),
	SA.
	
	

t2() ->
	Tests = [
		%"GL000207.1"
		%% "GL000226.1",
		%"GL000229.1"
		%% "GL000231.1"
		%"GL000210.1",
		%"GL000239.1",
		%"GL000235.1",
		"GL000201.1"
		%"GL000247.1",
		%"GL000245.1",
		%"GL000197.1"
	],
	t2(Tests).

t2([Chrom|Tests]) ->
	File = "../bwt_files/human_g1k_v37_decoy.fasta",
	
	{Pos,Len} = msw:get_reference_position(Chrom,File),
	{_,Ref_seq} = msw:get_ref_seq(File,Pos,Len),
	io:format("~nCromosome:~p, Length:~p~n",[Chrom,length(Ref_seq)]),
	Str = append($$,Ref_seq),

	io:format("~nCromosome:~p - ",[Chrom]),
	statistics(runtime),
	SAIS = sais(Str),
	{_,T1} = statistics(runtime),
	io:format(" SAIS: ~pms~n",[T1]),
	SA = sa(Str),
	{_,T2} = statistics(runtime),
	io:format(" SA: ~pms~n",[T2]),
	io:format("test passed:~p~n",[SAIS==SA]),
	t2(Tests);
t2([]) -> ok.
		
	
t3() -> t3(1).
t3(N) ->
	Str = random_seq(9,"ACGT"),
	io:format("~n(~p) Str: ~p~n",[N,Str]),
	io:format("~n(~p) Str: ~p, SA: ~p~n~n",[N,Str,sais(Str)]),
	t3(N+1).

random_seq(N,Alphabet) ->
	J = length(Alphabet),
	Tup = list_to_tuple(Alphabet),
	[ element(random:uniform(J),Tup) || _ <- lists:seq(1,N)] ++ "$".


%%%%%%%%%%%%% ALTERNATIVE IMPLEMENTATION of SA-IS %%%%%%%%%%%%%%%%

t1() ->
	Str = "AMAMMMASASMASASSAARA$",
	%Str = "AMIISSISSIPPII$",
	io:format("Str: ~p~n",[Str]),
	sais(Str).
	


% reduces the Str to the smaller one and returns a tuple {is_unique,Str1},
% where is_unique is True if all characters of Str1 is unique. Otherwise is False.
sais(Str) -> 
	{LmsStrs,Bkts,LmsIndices} = lms(Str),
	io:format(" Step 1. LMS indices: ~p~n",[LmsIndices]),
	Tup = list_to_tuple(Str),
	Bkts1 = lr_scan(Tup,Bkts),
	io:format(" Step2:~p~n",[Bkts1]),
	Bkts2 = rl_scan(Tup,Bkts1),
	io:format(" Step3:~p~n",[Bkts2]),
	
	SA = 
	case rename(Bkts2,LmsStrs) of
		{true,Ls} -> 
			%Sf = get_str(Ls,LmsIndices),
			%M = length(Sf)+1,
			%[ M-X || X <- Sf ];
			sa(get_str(Ls,LmsIndices));


		{false,Ls} -> 
			Str1 = get_str(Ls,LmsIndices),
			io:format("~nNew recursion. String: ~p~n",[Str1]),
			sais(Str1)
	end,
	io:format(" Step5: SA = ~p~n",[SA]),
	io:format(" Golden SA = ~p~n",[sa(get_str(Ls,LmsIndices))]),
	Inv = inverse_index(SA),
	io:format("Inverse index: ~p~n",[Inv]),
	Lms_sorted = sort_lms(LmsIndices,Inv),
	io:format("Sorted lms indices: ~p~n",[Lms_sorted]),
	Bkts3 = bucket(Lms_sorted,Tup),
	io:format(" Step6. Buckets: ~p~n",[Bkts3]),
	Bkts4 = lr_scan7(Tup,Bkts3),
	io:format(" Step7:~p~n",[Bkts4]),
	Bkts5 = rl_scan8(Tup,Bkts4),
	io:format(" Step8:~p~n",[Bkts5]),
	%io:format("Index: ~p~n",[flatten(Bkts5)]),
	%[7,6,1,2,4,3,5].
	Index = flatten(Bkts5),
	Golden = sa(Str),
	case Index == Golden of
		true -> Index;
		false->
			%io:format(" Step5: SA = ~p~n",[SA]),
			%io:format("Inverse index: ~p~n",[Inv]),
			%io:format(" Step6. Buckets: ~p~n",[Bkts3]),
			%io:format(" Step7:~p~n",[Bkts4]),
			%io:format(" Step8:~p~n",[Bkts5]),
			io:format("Index: ~p~n",[Index]),
			io:format("Golden:~p~n~n>>>~n~n",[Golden]),
			throw(error)
	end.
	
	


flatten(Buckets) ->
	[{_,Ltype,Stype}|Bkts] = lists:reverse(Buckets),
	flatten(Bkts,Ltype,Stype,[]).
flatten([{_,L,S}|Bkts],Ltype,Stype,Acc) -> flatten(Bkts,L,S,Ltype++Stype++Acc);
flatten([],L,S,Acc) -> L++S++Acc.

	

% returns a dict containing key=index, value=lms_substring and list of buckets containing lms-indices
lms(Str) ->
	L = length(Str),
	[Last|Str1] = lists:reverse(Str),
	lms(Str1,L,[Last],stype,dict:new(),[],[]).

lms([S|Str],J,[S1|LMS],stype,Dict,Acc,Ls) when S > S1 ->
	lms(Str,J-1,[S,S1],ltype,dict:store(J,[S1|LMS],Dict),add_ind(Acc,S1,J),[J|Ls]);
lms([S|Str],J,[S1|LMS],ltype,Dict,Acc,Ls) when S > S1 ->
	lms(Str,J-1,[S,S1|LMS],ltype,Dict,Acc,Ls);
lms([S1|Str],J,[S1|LMS],Type,Dict,Acc,Ls) -> 
	lms(Str,J-1,[S1,S1|LMS],Type,Dict,Acc,Ls);
lms([S2|Str],J,[S1|LMS],_,Dict,Acc,Ls) ->
	lms(Str,J-1,[S2,S1|LMS],stype,Dict,Acc,Ls);
lms([],1,_,_,Dict,Acc,Ls) -> {Dict,lists:sort(Acc),Ls}.


add_ind(Acc,S1,J) ->
	case lists:keyfind(S1,1,Acc) of
		{S1,[],Ls} -> lists:keyreplace(S1,1,Acc,{S1,[],[J|Ls]});
		false -> [{S1,[],[J]}|Acc]
	end. 


% scans buckets from left to right and modifies them (Step 2)
lr_scan(Tup,[{Key,Lbkt,Sbkt}|Buckets]) -> 
	lr_scan(Tup,Key,Lbkt,Sbkt,[],Buckets,[]).

lr_scan(Tup,Key,[Ind|Lbkt],Sbkt,Acc,Buckets,Nb) ->
	case Ind of
		1 -> Ind1 = size(Tup);
		_ -> Ind1 = Ind-1
	end,
	case (Key1=element(Ind1,Tup)) >= element(Ind,Tup) of
		true -> 
			case Key1 =:= Key of
				true -> lr_scan(Tup,Key,append(Ind1,Lbkt),Sbkt,Acc,Buckets,Nb);
				_ -> lr_scan(Tup,Key,Lbkt,Sbkt,Acc,add2ltype(Buckets,Key1,Ind1),Nb)
			end;
		_ -> lr_scan(Tup,Key,Lbkt,Sbkt,[Ind|Acc],Buckets,Nb)
	end;  
lr_scan(Tup,Key,[],Sbkt,AccL,Buckets,Nb) -> lr_scan(Tup,Key,[],Sbkt,[],AccL,Buckets,Nb).

lr_scan(Tup,Key,[],[Ind|Sbkt],Acc,AccL,Buckets,Nb) ->
	case Ind of
		1 -> Ind1 = size(Tup);
		_ -> Ind1 = Ind-1
	end,
	case (Key1=element(Ind1,Tup)) >= element(Ind,Tup) of
		true -> 
			case Key1 =:=Key of
				true -> lr_scan(Tup,Key,[],Sbkt,Acc,[Ind1|AccL],Buckets,Nb);
				_ -> lr_scan(Tup,Key,[],Sbkt,Acc,AccL,add2ltype(Buckets,Key1,Ind1),Nb)
			end;
		_ -> lr_scan(Tup,Key,[],Sbkt,[Ind|Acc],AccL,Buckets,Nb)
	end;  
lr_scan(Tup,Key,[],[],AccS,AccL,[{Key1,Lbkt1,Sbkt1}|Buckets],Nb) -> 
	io:format("Bucket finished. New bucket: ~p~n",[{Key1,lists:reverse(Lbkt1),Sbkt1}]),
	lr_scan(Tup,Key1,lists:reverse(Lbkt1),Sbkt1,[],Buckets,[{Key,lists:reverse(AccL),lists:reverse(AccS)}|Nb]);
lr_scan(_,Key,[],[],AccS,AccL,[],Nb) -> lists:reverse([{Key,lists:reverse(AccL),lists:reverse(AccS)}|Nb]).


%reverse_ltype(Buckets) -> lists:map(fun({X,Ltype,Stype})-> 
%										{X,lists:reverse(Ltype),Stype} 
%									end,Buckets).


add2ltype(Buckets,Key,J) ->
	case lists:keyfind(Key,1,Buckets) of
		{Key,Ltype,Stype} -> lists:keyreplace(Key,1,Buckets,{Key,[J|Ltype],Stype});
		false -> lists:sort([{Key,[J],[]}|Buckets])
	end.


% scans buckets from right to left and modifies them (Step 3)
rl_scan(Tup,Buckets) -> 
	[{Key,Ltype,Stype}|Rev_bkt] = lists:reverse(Buckets),
	rl_scan(Tup,Rev_bkt,Key,Ltype,lists:reverse(Stype),[],[]).

rl_scan(Tup,Buckets,Key,Ltype,[Ind|Stype],Acc,Nb) ->
	case Ind of
		1 -> Ind1 = size(Tup);
		_ -> Ind1 = Ind-1
	end,
	case (Key1=element(Ind1,Tup)) =< element(Ind,Tup) of
		true ->
			case Key1 =:= Key of
				true -> rl_scan(Tup,Buckets,Key,Ltype,append(Ind1,Stype),Acc,Nb);
				_ -> rl_scan(Tup,add2stype(Buckets,Key1,Ind1),Key,Ltype,Stype,Acc,Nb)
			end;
		_ -> rl_scan(Tup,Buckets,Key,Ltype,Stype,[Ind|Acc],Nb)
	end;
rl_scan(Tup,Buckets,Key,Ltype,[],AccS,Nb) -> 
	rl_scan(Tup,Buckets,Key,lists:reverse(Ltype),[],[],AccS,Nb).

rl_scan(Tup,Buckets,Key,[Ind|Ltype],[],Acc,AccS,Nb) ->
	case Ind of
		1 -> Ind1 = size(Tup);
		_ -> Ind1 = Ind-1
	end,
	case (Key1=element(Ind1,Tup)) =< element(Ind,Tup) of
		true ->
			case Key1 =:= Key of
				true -> rl_scan(Tup,Buckets,Key,Ltype,[],Acc,[Ind1|AccS],Nb);
				_ -> rl_scan(Tup,add2stype(Buckets,Key1,Ind1),Key,Ltype,[],Acc,AccS,Nb)
			end;
		_ -> rl_scan(Tup,Buckets,Key,Ltype,[],[Ind|Acc],AccS,Nb)
	end;
rl_scan(Tup,[{Key1,Ltype1,Stype1}|Buckets],Key,[],[],AccL,AccS,Nb) -> 
	io:format("Bucket finished. New bucket: ~p~n",[{Key1,lists:reverse(Ltype1),Stype1}]),
	rl_scan(Tup,Buckets,Key1,Ltype1,lists:reverse(Stype1),[],[{Key,AccL,AccS}|Nb]);
rl_scan(_,[],Key,[],[],AccL,AccS,Nb) ->
	[{Key,AccL,AccS}|Nb].


append(Ind,Ls) -> lists:reverse([Ind|lists:reverse(Ls)]).


add2stype(Buckets,Key,J) ->
	case lists:keyfind(Key,1,Buckets) of
		{Key,Ltype,Stype} -> lists:keyreplace(Key,1,Buckets,{Key,Ltype,[J|Stype]});
		false -> lists:sort(fun(A,B)-> A>B end, [{Key,[],[J]}|Buckets])
	end.


% assign new names to lms-strings represented in Buckets
rename([{_,Ltype,Stype}|Bkts],Dict) ->
	rename(none,Ltype,Stype,Bkts,Dict,[],true).

rename(none,[J|Ltype],Stype,Bkts,Dict,[],true) -> 
	rename(dict:fetch(J,Dict),Ltype,Stype,Bkts,Dict,[{J,1}],true);
rename(none,[],[J|Stype],Bkts,Dict,[],true) -> 
	rename(dict:fetch(J,Dict),[],Stype,Bkts,Dict,[{J,1}],true);
rename(_,[],[],[{_,Ltype,Stype}|Bkts],Dict,Acc,U) -> rename(none,Ltype,Stype,Bkts,Dict,Acc,U);

rename(none,[J|Ltype],Stype,Bkts,Dict,[{K,N}|Acc],U) ->
	rename(dict:fetch(J,Dict),Ltype,Stype,Bkts,Dict,[{J,N+1},{K,N}|Acc],U);
rename(none,[],[J|Stype],Bkts,Dict,[{K,N}|Acc],U) ->
	rename(dict:fetch(J,Dict),[],Stype,Bkts,Dict,[{J,N+1},{K,N}|Acc],U);

rename(LMS,[J|Ltype],Stype,Bkts,Dict,[{K,N}|Acc],U) ->
	case dict:fetch(J,Dict) of
		LMS -> rename(LMS,Ltype,Stype,Bkts,Dict,[{J,N},{K,N}|Acc],false);
		LMS1-> rename(LMS1,Ltype,Stype,Bkts,Dict,[{J,N+1},{K,N}|Acc],U)
	end;	
rename(LMS,[],[J|Stype],Bkts,Dict,[{K,N}|Acc],U) ->
	case dict:fetch(J,Dict) of
		LMS -> rename(LMS,[],Stype,Bkts,Dict,[{J,N},{K,N}|Acc],false);
		LMS1-> rename(LMS1,[],Stype,Bkts,Dict,[{J,N+1},{K,N}|Acc],U)
	end;	

rename(_,[],[],[],_,Acc,U) -> {U,Acc}.


% creates a new string 
get_str(Ls,LmsIndices) ->
	get_str(Ls,lists:reverse(LmsIndices),[]).

get_str(Ls,[J|Str],Acc) ->
	{J,Name} = lists:keyfind(J,1,Ls),
	get_str(Ls,Str,[Name|Acc]);
get_str(_,[],Acc) -> Acc.



inverse_index(SA) -> inverse_index(SA,1,array:new(length(SA))).
inverse_index([J|SA],N,Arr) -> inverse_index(SA,N+1,array:set(J-1,N,Arr));
inverse_index([],_,Arr) -> array:to_list(Arr).


sort_lms(LmsIndices,Inv) -> sort_lms(LmsIndices,Inv,array:new(length(LmsIndices))).
sort_lms([J|Lms],[Ind|Inv],Arr) -> sort_lms(Lms,Inv,array:set(Ind-1,J,Arr));
sort_lms([],[],Arr) -> array:to_list(Arr).


bucket(Lms,Tup) -> bucket(lists:reverse(Lms),Tup,[]).
bucket([J|Lms],Tup,Bkts) -> 
	Key = element(J,Tup),
	bucket(Lms,Tup,add_ind(Bkts,Key,J));
bucket([],_,Bkts) -> lists:sort(Bkts).



% scans buckets from left to right and modifies them (Step 7)
lr_scan7(Tup,[{Key,Lbkt,Sbkt}|Buckets]) -> 
	lr_scan7(Tup,Key,Lbkt,Sbkt,[],Buckets,[]).

lr_scan7(Tup,Key,[Ind|Lbkt],Sbkt,Acc,Buckets,Nb) ->
	case Ind of
		1 -> Ind1 = size(Tup);
		_ -> Ind1 = Ind-1
	end,
	case (Key1=element(Ind1,Tup)) >= element(Ind,Tup) of
		true -> 
			case Key1 =:= Key of
				true -> lr_scan7(Tup,Key,append(Ind1,Lbkt),Sbkt,[{Ind}|Acc],Buckets,Nb);
				_ -> lr_scan7(Tup,Key,Lbkt,Sbkt,[{Ind}|Acc],add2ltype(Buckets,Key1,Ind1),Nb)
			end;
		_ -> lr_scan7(Tup,Key,Lbkt,Sbkt,[Ind|Acc],Buckets,Nb)
	end;  
lr_scan7(Tup,Key,[],Sbkt,AccL,Buckets,Nb) -> lr_scan7(Tup,Key,[],Sbkt,[],AccL,Buckets,Nb).

lr_scan7(Tup,Key,[],[Ind|Sbkt],Acc,AccL,Buckets,Nb) ->
	case Ind of
		1 -> Ind1 = size(Tup);
		_ -> Ind1 = Ind-1
	end,
	case (Key1=element(Ind1,Tup)) >= element(Ind,Tup) of
		true -> 
			case Key1 =:= Key of
				true -> lr_scan7(Tup,Key,[],Sbkt,Acc,[Ind1|AccL],Buckets,Nb);
				_ -> lr_scan7(Tup,Key,[],Sbkt,Acc,AccL,add2ltype(Buckets,Key1,Ind1),Nb)
			end;
		_ -> lr_scan7(Tup,Key,[],Sbkt,[Ind|Acc],AccL,Buckets,Nb)
	end;  
lr_scan7(Tup,Key,[],[],AccS,AccL,[{Key1,Lbkt1,Sbkt1}|Buckets],Nb) -> 
	io:format("Bucket finished. New bucket: ~p~n",[{Key1,lists:reverse(Lbkt1),Sbkt1}]),
	lr_scan7(Tup,Key1,lists:reverse(Lbkt1),Sbkt1,[],Buckets,[{Key,lists:reverse(AccL),lists:reverse(AccS)}|Nb]);
lr_scan7(_,Key,[],[],AccS,AccL,[],Nb) -> lists:reverse([{Key,lists:reverse(AccL),lists:reverse(AccS)}|Nb]).



% scans buckets from right to left and modifies them (Step 8)
rl_scan8(Tup,Buckets) -> 
	[{Key,Ltype,Stype}|Rev_bkt] = lists:reverse(Buckets),
	rl_scan8(Tup,Rev_bkt,Key,Ltype,lists:reverse(Stype),[],[]).

rl_scan8(Tup,Buckets,Key,Ltype,[Ind|Stype],Acc,Nb) ->
	case Ind of
		1 -> Ind1 = size(Tup);
		_ -> Ind1 = Ind-1
	end,
	case (Key1=element(Ind1,Tup)) =< element(Ind,Tup) of
		true ->
			case Key1 == Key of
				true -> rl_scan8(Tup,Buckets,Key,Ltype,append(Ind1,Stype),[Ind|Acc],Nb);
				_ -> rl_scan8(Tup,add2stype(Buckets,Key1,Ind1),Key,Ltype,Stype,[Ind|Acc],Nb)
			end;
		_ -> rl_scan8(Tup,Buckets,Key,Ltype,Stype,[Ind|Acc],Nb)
	end;
rl_scan8(Tup,Buckets,Key,Ltype,[],AccS,Nb) -> 
	rl_scan8(Tup,Buckets,Key,lists:reverse(Ltype),[],[],AccS,Nb).

rl_scan8(Tup,Buckets,Key,[{Ind}|Ltype],[],Acc,AccS,Nb) ->
	rl_scan8(Tup,Buckets,Key,Ltype,[],[Ind|Acc],AccS,Nb);

rl_scan8(Tup,Buckets,Key,[Ind|Ltype],[],Acc,AccS,Nb) ->
	case Ind of
		1 -> Ind1 = size(Tup);
		_ -> Ind1 = Ind-1
	end,
	case (Key1=element(Ind1,Tup)) =< element(Ind,Tup) of
		true -> 
			case Key1 =:= Key of
				true -> rl_scan8(Tup,Buckets,Key,Ltype,[],[Ind|Acc],[Ind1|AccS],Nb);
				_ -> rl_scan8(Tup,add2stype(Buckets,Key1,Ind1),Key,Ltype,[],[Ind|Acc],AccS,Nb)
			end;
		_ -> rl_scan8(Tup,Buckets,Key,Ltype,[],[Ind|Acc],AccS,Nb)
	end;
rl_scan8(Tup,[{Key1,Ltype1,Stype1}|Buckets],Key,[],[],AccL,AccS,Nb) -> 
	io:format("Bucket finished. New bucket: ~p~n",[{Key1,lists:reverse(Ltype1),Stype1}]),
	rl_scan8(Tup,Buckets,Key1,Ltype1,lists:reverse(Stype1),[],[{Key,AccL,AccS}|Nb]);
rl_scan8(_,[],Key,[],[],AccL,AccS,Nb) ->
	[{Key,AccL,AccS}|Nb].
	

%%%%%%%%%%%%%% STRAIGHT-FORWARD IMPLEMENTATION %%%%%%%%%%%%%%%%

sa(X) ->
	Ls = lists:sort(get_suffs(X)),
	[ N || {_,N,_} <- Ls].



get_suffs(X) ->
	[Last|_] = lists:reverse(X),
	get_suffs([],1,X,Last).

get_suffs(Acc, N, [H|X],P) ->
	get_suffs([{[H|X],N,P}|Acc], N+1, X, H);
get_suffs(Acc,_,[],_) -> 
	%io:format("Suffices:~n~p~n",[Acc]),
	Acc.

