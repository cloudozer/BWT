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
	io:format("~n\tStarting ~c suffixes...~n",[S]),
	dsa(Str,Bin,Alphabet,lists:foldl(fun(X,Acc) -> [X|Acc]
							end,AccFM, lists:reverse(get_branch(Str,Bin,S,$$,0,[]))));
dsa(_,_,[],AccFM) -> AccFM.

get_branch([S|Str],Bin,S,Prev,N,Acc) -> 
	get_branch(Str,Bin,S,S,N+1,[{S,Prev,N}|Acc]);
get_branch([P|Str],Bin,S,_,N,Acc) -> get_branch(Str,Bin,S,P,N+1,Acc);
get_branch([],Bin,S,_,_,Acc) -> 
	Size = size(Bin),
	Bin_index = fun(N) when N < Size-1 -> binary:at(Bin, N); (_) -> $$ end,
	F = fun(F,X1,X2) -> Vx = Bin_index(X1), Vy = Bin_index(X2),
						case Vx =:= Vy of true -> F(F,X1+1,X2+1); _ -> Vx < Vy end
		end,	
	Compare = fun(Shift,Ls) -> 
				lists:sort(fun({_,_,X1},{_,_,X2}) -> F(F,X1+Shift,X2+Shift) end,Ls) 
	   		  end,
	%Compare = fun({_,_,X1},{_,_,X2}) -> F(F,X1,X2) end,	
	
	case length(Acc) < ?SORTED_MAX of
		true -> 
			io:format("done~nadding a ~c-branch of length ~p... ",[S,length(Acc)]),
			Compare(0,Acc);
		_ ->
			io:format("~ngetting to level 1~n"), 
			dsa(lists:reverse(Acc),1,"TGCA$",Bin_index,Compare,[])
	end.
	 	


dsa(Indices,Shift,[S|Alphabet],Bin_index,Compare,Acc) -> 
	dsa(Indices,Shift,Alphabet,Bin_index,Compare,lists:foldl(fun(X,Acc1) -> [X|Acc1] end, 
						Acc, lists:reverse(get_sa(Indices,Shift,S,Bin_index,Compare,[])))
		);
dsa(_,Shift,[],_,_,Acc) -> 
	io:format("~ngetting up to level ~p~n",[Shift-1]),
	erlang:garbage_collect(),
	Acc.



get_sa([{_,_,N}=Rec|Indices],Shift,S,Bin_index,Compare,Acc) ->
	case Bin_index(N+Shift) of
		S -> get_sa(Indices,Shift,S,Bin_index,Compare,[Rec|Acc]);
		_ -> get_sa(Indices,Shift,S,Bin_index,Compare,Acc)
	end;
get_sa([],_,_,_,_,[]) -> [];
get_sa([],Shift,_,Bin_index,Compare,Acc) ->
	%io:format("Chunk length of level ~p: ~p~n",[Shift,length(Acc)]),
	Len = length(Acc), 
	case Len < ?SORTED_MAX of
		true -> 
			io:format("done~nadding a sub-index of length ~p... ",[Len]),
			Compare(Shift,Acc);
			%lists:sort(Compare,Acc);
		_ ->
			io:format("~ngetting down to level ~p, processing string of length ~p~n",[Shift+1,Len]), 
			dsa(lists:reverse(Acc),Shift+1,"TGCA$",Bin_index,Compare,[])
	end.


