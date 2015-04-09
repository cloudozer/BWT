% An implementation of OSACA algorithm
% invented by Ge Nong
% 
% Cloudozer(c), 2015
%

-module(osaca).
-export([t/0,
		t1/0,
		t2/1,
		lms/1,
		lr_scan/2
		]).

t() ->
	File = "../bwt_files/human_g1k_v37_decoy.fasta",
	%Chrom = "GL000192.1",
	Chrom = "21",
	{Pos,Len} = msw:get_reference_position(Chrom,File),
	statistics(runtime),
	{Shift,Ref_seq} = msw:get_ref_seq(File,Pos,Len),
	{_,T0} = statistics(runtime),
	io:format("~nCromosome:~p, Length:~p~n",[Chrom,length(Ref_seq)]),
	io:format("Reading from file:~ps~n",[T0/1000]).
	
	

t2(N) ->
	Ls = lists:sort([ {random:uniform(),J} || J <- lists:seq(0,N-1) ]),
	statistics(runtime),
	Arr = array:new(N),
	{_,T1} = statistics(runtime),
	t2(Arr,Ls),
	{_,T2} = statistics(runtime),
	io:format("Array initialization: ~pms~n",[T1]),
	io:format("Array filling in: ~pms~n",[T2]).

t2(Arr,[]) -> Arr;
t2(Arr,[{V,J}|Ls]) -> t2(array:set(J,V,Arr),Ls).


%%%%%%%%%%%%% ALTERNATIVE IMPLEMENTATION of SA-IS %%%%%%%%%%%%%%%%

t1() ->
	Str = "AMAMMMASASMASASSAARA$",
	%Str = "AMIISSISSIPPII$",
	io:format("Str: ~p~n",[Str]),
	sais(Str).
	


% reduces the Str to the smaller one and returns a tuple {is_unique,Str1},
% where is_unique is True if all characters of Str1 is unique. Otherwise is False.
sais(Str) -> 
	{LmsStrs,Bkts,LmsIndices} = osaca:lms(Str),
	io:format(" Step 1. LMS indices: ~p~n",[LmsIndices]),
	Tup = list_to_tuple(Str),
	Bkts1 = lr_scan(Tup,Bkts),
	io:format(" Step2:~p~n",[Bkts1]),
	Bkts2 = rl_scan(Tup,Bkts1),
	io:format(" Step3:~p~n",[Bkts2]),
	
	SA = 
	case rename(Bkts2,LmsStrs) of
		{true,Ls} -> 
			Sf = get_str(Ls,LmsIndices),
			M = length(Sf)+1,
			[ M-X || X <- Sf ];


		{false,Ls} -> 
			Str1 = get_str(Ls,LmsIndices),
			io:format("~nNew recursion. String: ~p~n",[Str1]),
			sais(Str1)
	end,
	io:format(" Step5: SA = ~p~n",[SA]),
	Inv = inverse_index(SA),
	io:format("Inverse index: ~p~n",[Inv]),
	Lms_sorted = sort_lms(LmsIndices,Inv),
	io:format("Sorted lms indices: ~p~n",[Lms_sorted]),
	Bkts3 = bucket(Lms_sorted,Tup),
	io:format(" Step6. Buckets: ~p~n",[Bkts3]),
	Bkts4 = lr_scan7(Tup,Bkts3),
	io:format(" Step7:~p~n",[Bkts4]),
	Bkts5 = rl_scan8(Tup,Bkts4),
	io:format(" Step8:~p~n",[Bkts5]).
	


	

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
	lr_scan(Tup,Key,ltype,Lbkt,[],Sbkt,reverse_ltype(Buckets),[]).

lr_scan(Tup,Key,ltype,[1|Lbkt],Acc,Sbkt,Buckets,Nb) ->
	lr_scan(Tup,Key,ltype,Lbkt,[1|Acc],Sbkt,Buckets,Nb);
lr_scan(Tup,Key,ltype,[Ind|Lbkt],Acc,Sbkt,Buckets,Nb) ->
	%io:format("Ind:~p ltype~n",[Ind]),
	Key1 = element(Ind-1,Tup),
	case Key1 >= element(Ind,Tup) of
		true -> 
			case Key1 == Key of
				true -> lr_scan(Tup,Key,ltype,Lbkt,Acc,[Ind-1|Sbkt],Buckets,Nb);
				_ -> lr_scan(Tup,Key,ltype,Lbkt,Acc,Sbkt,add2ltype(Buckets,Key1,Ind-1),Nb)
			end;
		_ -> lr_scan(Tup,Key,ltype,Lbkt,[Ind|Acc],Sbkt,Buckets,Nb)
	end;  
lr_scan(Tup,Key,ltype,[],Acc,Sbkt,Buckets,Nb) -> lr_scan(Tup,Key,stype,Sbkt,[],Acc,Buckets,Nb);
lr_scan(Tup,Key,stype,[1|Sbkt],Acc,Lbkt,Buckets,Nb) ->
	lr_scan(Tup,Key,stype,Sbkt,[1|Acc],Lbkt,Buckets,Nb);
lr_scan(Tup,Key,stype,[Ind|Sbkt],Acc,Lbkt,Buckets,Nb) ->
	%io:format("Ind:~p stype~n",[Ind]),
	case (Key1=element(Ind-1,Tup)) >= element(Ind,Tup) of
		true -> 
			case Key1 == Key of
				true -> lr_scan(Tup,Key,stype,[Ind-1|Sbkt],Acc,Lbkt,Buckets,Nb);
				_ -> lr_scan(Tup,Key,stype,Sbkt,Acc,Lbkt,add2ltype(Buckets,Key1,Ind-1),Nb)
			end;
		_ -> lr_scan(Tup,Key,stype,Sbkt,[Ind|Acc],Lbkt,Buckets,Nb)
	end;  
lr_scan(Tup,Key,stype,[],Acc,Lbkt,[{Key1,Lbkt1,Sbkt1}|Buckets],Nb) -> 
	io:format("Bucket finished. New bucket: ~p~n",[{Key1,lists:reverse(Lbkt1),Sbkt1}]),
	lr_scan(Tup,Key1,ltype,lists:reverse(Lbkt1),[],Sbkt1,Buckets,[{Key,lists:reverse(Lbkt),Acc}|Nb]);
lr_scan(_,Key,stype,[],Acc,Lbkt,[],Nb) -> lists:reverse([{Key,lists:reverse(Lbkt),Acc}|Nb]).


reverse_ltype(Buckets) -> lists:map(fun({X,Ltype,Stype})-> 
										{X,lists:reverse(Ltype),Stype} 
									end,Buckets).


add2ltype(Buckets,Key,J) ->
	case lists:keyfind(Key,1,Buckets) of
		{Key,Ltype,Stype} -> lists:keyreplace(Key,1,Buckets,{Key,[J|Ltype],Stype});
		false -> lists:sort([{Key,[J],[]}|Buckets])
	end.


% scans buckets from right to left and modifies them (Step 3)
rl_scan(Tup,Buckets) -> 
	[{Key,Ltype,Stype}|Rev_bkt] = lists:reverse(Buckets),
	rl_scan(Tup,Rev_bkt,Ltype,Key,stype,lists:reverse(Stype),[],[]).

rl_scan(Tup,Buckets,Ltype,Key,stype,[1|Stype],Acc,Nb) ->
	rl_scan(Tup,add2stype(Buckets,$$,size(Tup)),Ltype,Key,stype,Stype,Acc,Nb);
rl_scan(Tup,Buckets,Ltype,Key,stype,[Ind|Stype],Acc,Nb) ->
	%io:format("Ind:~p stype~n",[Ind]),
	case (Key1=element(Ind-1,Tup)) =< element(Ind,Tup) of
		true ->
			case Key1 == Key of
				true -> rl_scan(Tup,Buckets,Ltype,Key,stype,append(Ind-1,Stype),Acc,Nb);
				_ -> rl_scan(Tup,add2stype(Buckets,Key1,Ind-1),Ltype,Key,stype,Stype,Acc,Nb)
			end;
		_ -> rl_scan(Tup,Buckets,Ltype,Key,stype,Stype,[Ind|Acc],Nb)
	end;
rl_scan(Tup,Buckets,Ltype,Key,stype,[],Acc,Nb) -> 
	rl_scan(Tup,Buckets,Acc,lists:reverse(Ltype),Key,ltype,[],Nb);

rl_scan(Tup,Buckets,Stype,[1|Ltype],Key,ltype,Acc,Nb) ->
	rl_scan(Tup,add2stype(Buckets,$$,size(Tup)),Stype,Ltype,Key,ltype,Acc,Nb);
rl_scan(Tup,Buckets,Stype,[Ind|Ltype],Key,ltype,Acc,Nb) ->
	case (Key1=element(Ind-1,Tup)) =< element(Ind,Tup) of
		true ->
			case Key1 == Key of
				true -> rl_scan(Tup,Buckets,Stype,Key,append(Ind-1,Ltype),ltype,Acc,Nb);
				_ -> rl_scan(Tup,add2stype(Buckets,Key1,Ind-1),Stype,Ltype,Key,ltype,Acc,Nb)
			end;
		_ -> rl_scan(Tup,Buckets,Stype,Ltype,Key,ltype,[Ind|Acc],Nb)
	end;
rl_scan(Tup,[{Key1,Ltype1,Stype1}|Buckets],Stype,[],Key,ltype,Ltype,Nb) -> 
	io:format("Bucket finished. New bucket: ~p~n",[{Key1,lists:reverse(Ltype1),Stype1}]),
	rl_scan(Tup,Buckets,Ltype1,Key1,stype,lists:reverse(Stype1),[],[{Key,Ltype,Stype}|Nb]);
rl_scan(_,[],Stype,[],Key,ltype,Ltype,Nb) ->
	[{Key,Ltype,Stype}|Nb].


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


bucket(Lms,Tup) -> bucket(Lms,Tup,[]).
bucket([J|Lms],Tup,Bkts) -> 
	Key = element(J,Tup),
	bucket(Lms,Tup,add_ind(Bkts,Key,J));
bucket([],_,Bkts) -> Bkts.



% scans buckets from left to right and modifies them (Step 7)
lr_scan7(Tup,[{Key,Lbkt,Sbkt}|Buckets]) -> 
	lr_scan7(Tup,Key,Lbkt,Sbkt,[],[],Buckets,[]).

lr_scan7(Tup,Key,[1|Lbkt],Sbkt,Acc,Buckets,Nb) ->
	lr_scan7(Tup,Key,Lbkt,Sbkt,[1|Acc],Buckets,Nb);
lr_scan7(Tup,Key,[Ind|Lbkt],Sbkt,Acc,Buckets,Nb) ->
	%io:format("Ind:~p ltype~n",[Ind]),
	Key1 = element(Ind-1,Tup),
	case Key1 >= element(Ind,Tup) of
		true -> 
			case Key1 == Key of
				true -> lr_scan7(Tup,Key,Lbkt,[Ind-1|Sbkt],[Ind|Acc],Buckets,Nb);
				_ -> lr_scan7(Tup,Key,Lbkt,Sbkt,[Ind|Acc],add2ltype(Buckets,Key1,Ind-1),Nb)
			end;
		_ -> lr_scan7(Tup,Key,Lbkt,Sbkt,[Ind|Acc],Buckets,Nb)
	end;  
lr_scan7(Tup,Key,[],Sbkt,AccL,Buckets,Nb) -> lr_scan7(Tup,Key,[],Sbkt,[],AccL,Buckets,Nb).

lr_scan7(Tup,Key,[],[1|Sbkt],Acc,AccL,Buckets,Nb) ->
	lr_scan7(Tup,Key,[],Sbkt,[1|Acc],AccL,Buckets,Nb);
lr_scan7(Tup,Key,[],[Ind|Sbkt],Acc,AccL,Buckets,Nb) ->
	%io:format("Ind:~p stype~n",[Ind]),
	case (Key1=element(Ind-1,Tup)) >= element(Ind,Tup) of
		true -> 
			case Key1 == Key of
				true -> lr_scan7(Tup,Key,[],[Ind-1|Sbkt],Acc,AccL,Buckets,Nb);
				_ -> lr_scan7(Tup,Key,[],Sbkt,Acc,AccL,add2ltype(Buckets,Key1,Ind-1),Nb)
			end;
		_ -> lr_scan7(Tup,Key,[],Sbkt,[Ind|Acc],AccL,Buckets,Nb)
	end;  
lr_scan7(Tup,Key,[],[],AccS,AccL,[{Key1,Lbkt1,Sbkt1}|Buckets],Nb) -> 
	io:format("Bucket finished. New bucket: ~p~n",[{Key1,lists:reverse(Lbkt1),Sbkt1}]),
	lr_scan7(Tup,Key1,lists:reverse(Lbkt1),Sbkt1,[],Buckets,[{Key,lists:reverse(AccL),AccS}|Nb]);
lr_scan7(_,Key,[],[],AccS,AccL,[],Nb) -> lists:reverse([{Key,lists:reverse(AccL),AccS}|Nb]).



% scans buckets from right to left and modifies them (Step 8)
rl_scan8(Tup,Buckets) -> 
	[{Key,Ltype,Stype}|Rev_bkt] = lists:reverse(Buckets),
	rl_scan8(Tup,Rev_bkt,Ltype,Key,stype,lists:reverse(Stype),[],[]).

rl_scan(Tup,Buckets,Ltype,Key,stype,[1|Stype],Acc,Nb) ->
	rl_scan(Tup,add2stype(Buckets,$$,size(Tup)),Ltype,Key,stype,Stype,Acc,Nb);
rl_scan(Tup,Buckets,Ltype,Key,stype,[Ind|Stype],Acc,Nb) ->
	%io:format("Ind:~p stype~n",[Ind]),
	case (Key1=element(Ind-1,Tup)) =< element(Ind,Tup) of
		true ->
			case Key1 == Key of
				true -> rl_scan(Tup,Buckets,Ltype,Key,stype,append(Ind-1,Stype),Acc,Nb);
				_ -> rl_scan(Tup,add2stype(Buckets,Key1,Ind-1),Ltype,Key,stype,Stype,Acc,Nb)
			end;
		_ -> rl_scan(Tup,Buckets,Ltype,Key,stype,Stype,[Ind|Acc],Nb)
	end;
rl_scan(Tup,Buckets,Ltype,Key,stype,[],Acc,Nb) -> 
	rl_scan(Tup,Buckets,Acc,lists:reverse(Ltype),Key,ltype,[],Nb);

rl_scan(Tup,Buckets,Stype,[1|Ltype],Key,ltype,Acc,Nb) ->
	rl_scan(Tup,add2stype(Buckets,$$,size(Tup)),Stype,Ltype,Key,ltype,Acc,Nb);
rl_scan(Tup,Buckets,Stype,[Ind|Ltype],Key,ltype,Acc,Nb) ->
	case (Key1=element(Ind-1,Tup)) =< element(Ind,Tup) of
		true ->
			case Key1 == Key of
				true -> rl_scan(Tup,Buckets,Stype,Key,append(Ind-1,Ltype),ltype,Acc,Nb);
				_ -> rl_scan(Tup,add2stype(Buckets,Key1,Ind-1),Stype,Ltype,Key,ltype,Acc,Nb)
			end;
		_ -> rl_scan(Tup,Buckets,Stype,Ltype,Key,ltype,[Ind|Acc],Nb)
	end;
rl_scan(Tup,[{Key1,Ltype1,Stype1}|Buckets],Stype,[],Key,ltype,Ltype,Nb) -> 
	io:format("Bucket finished. New bucket: ~p~n",[{Key1,lists:reverse(Ltype1),Stype1}]),
	rl_scan(Tup,Buckets,Ltype1,Key1,stype,lists:reverse(Stype1),[],[{Key,Ltype,Stype}|Nb]);
rl_scan(_,[],Stype,[],Key,ltype,Ltype,Nb) ->
	[{Key,Ltype,Stype}|Nb].

