% distributed Suffix array construction
%
% 
% Cloudozer(c), 2015
%

-module(dsa).
-export([
		dsa/1
		]).

-include("bwt.hrl").


dsa(Str) ->
	Alphabet = "TGCA", % sorted in descenting order
	[$$,L|_] = lists:reverse(Str),
	Bin = list_to_binary(Str),
	[ {$$,L,length(Str)-1} | dsa(Str,Bin,Alphabet,[]) ].

dsa(Str,Bin,[S|Alphabet],AccFM) -> 	
		dsa(Str,Bin,Alphabet,get_branch(Str,Bin,S,$$,0,[])++AccFM);
dsa(_,_,[],AccFM) -> AccFM.

get_branch([S|Str],Bin,S,Prev,N,Acc) -> 
	get_branch(Str,Bin,S,S,N+1,[{S,Prev,N}|Acc]);
get_branch([P|Str],Bin,S,_,N,Acc) -> get_branch(Str,Bin,S,P,N+1,Acc);
get_branch([],Bin,_,_,_,Acc) -> 
	Size = size(Bin),
	Bin_index = fun(N) when N < Size-1 -> binary:at(Bin, N); (_) -> $$ end,
	F = fun(F,X1,X2) -> Vx = Bin_index(X1), Vy = Bin_index(X2),
						case Vx =:= Vy of true -> F(F,X1+1,X2+1); _ -> Vx < Vy end
		end,	
	Compare = fun({_,_,X1},{_,_,X2}) -> F(F,X1,X2) end,	
	
	case length(Acc) < ?SORTED_MAX of
		true -> 
			io:format("completed~nadding a sub-index of ~p length... ",[length(Acc)]),
			lists:sort(Compare,Acc);
		_ -> dsa(lists:reverse(Acc),1,"TGCA$",Bin_index,Compare,[])
	end.
	 	


dsa(Indices,Shift,[S|Alphabet],Bin_index,Compare,Acc) -> 
	%io:format("Next char:~c~n",[S]),
	Res = get_sa(Indices,Shift,S,Bin_index,Compare,[]),
	dsa(Indices,Shift,Alphabet,Bin_index,Compare,Res++Acc);
dsa(_,_,[],_,_,Acc) -> Acc.



get_sa([{_,_,N}=Rec|Indices],Shift,S,Bin_index,Compare,Acc) ->
	case Bin_index(N+Shift) of
		S -> get_sa(Indices,Shift,S,Bin_index,Compare,[Rec|Acc]);
		_ -> get_sa(Indices,Shift,S,Bin_index,Compare,Acc)
	end;
get_sa([],_,_,_,_,[]) -> [];
get_sa([],Shift,_,Bin_index,Compare,Acc) ->
	%io:format("Chunk length of level ~p: ~p~n",[Shift,length(Acc)]),
	case length(Acc) < ?SORTED_MAX of
		true -> 
			io:format("completed~nadding a sub-index of ~p length... ",[length(Acc)]),
			lists:sort(Compare,Acc);
			%lists:sort(Compare,Acc);
		_ -> dsa(lists:reverse(Acc),Shift+1,"TGCA$",Bin_index,Compare,[])
	end.




