% An implementation of b-tree
% every node contains the left branch, the queue, and e thright branch 
% 
% Cloudozer(c), 2015
%


-module(btree).
-export([
		new/0,
		insert/2,
		to_list/1
		]).

-export([t/0]).

t() ->
	Ls = [ Y || {_,Y} <- lists:sort([ {random:uniform(),X} || X <- lists:seq(1,100000) ]) ],
	statistics(runtime),
	Be = lists:foldl(fun(X,Acc) -> insert(X,Acc) end, new(), Ls),
	{_,T1} = statistics(runtime),
	to_list(Be),
	{_,T2} = statistics(runtime),
	io:format("Re-arrangement: ~pms~n",[T1]),
	io:format("Traversing the b-tree: ~p~n",[T2]).

new() -> {none,[],[],none}.


insert(Item,{none,[],[],none}) -> {none,[Item],[],none};
insert(Item,{Lb,[H],[],Rb}) when Item == H-1 -> {Lb,[H-1],[H],Rb};
insert(Item,{Lb,[H|Left],Right,Rb}) when Item == H-1 -> {Lb,[H-1,H|Left],Right,Rb};

insert(Item,{Lb,[H],[],Rb}) when Item == H+1 -> {Lb,[H],[Item],Rb};
insert(Item,{Lb,Left,[H|Right],Rb}) when Item == H+1 -> {Lb,Left,[Item,H|Right],Rb};

insert(Item,{Lb,[H|Left],Right,Rb}) when Item < H-1 -> 
	case Lb of
		none -> {insert(Item,new()),[H|Left],Right,Rb};
		_ -> {insert(Item,Lb),[H|Left],Right,Rb}
	end;
insert(Item,{Lb,Left,[H|Right],Rb}) when Item > H+1 -> 
	case Rb of
		none -> {Lb,Left,[H|Right],insert(Item,new())};
		_ -> {Lb,Left,[H|Right],insert(Item,Rb)}
	end;
insert(Item,{Lb,[H],[],Rb}) when Item > H+1 -> 
	case Rb of
		none -> {Lb,[H],[],insert(Item,new())};
		_ -> {Lb,[H],[],insert(Item,Rb)}
	end.
	

to_list(Be) ->
	to_list(Be,[]).

to_list({Lb,Left,Right,Rb},Ls) -> 
	to_list(Lb,Left++lists:reverse(Right)++to_list(Rb,Ls));
to_list(none,Ls) -> Ls.

