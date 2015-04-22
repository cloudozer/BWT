% distributed Suffix Tree construction
%
% 
% Cloudozer(c), 2015
%

-module(st).
-export([t/0,
		st/3
		]).



t() ->
	Alphabet = "ACGT$",
	Str = <<"TTAGAAGATAT$">>,
	
	Ps = dict:from_list(start_branches(Str,Alphabet)),
	scan(0,Str,Ps),
	io:format("Scanning is complete~n"),
	lists:foreach(fun({_,Proc}) -> Proc ! finish
					end,dict:to_list(Ps)),

	[$$|Str1] = lists:reverse(binary_to_list(Str)),
	SA = bwt:sa(lists:reverse(Str1)),
	io:format("~p~n",[SA]),
	collect(dict:size(Ps),[{$$,[size(Str)-1]}]).
	


scan(N,<<I:8,$$>>,Ps) ->
	%io:format("Msg:~p~n",[N]),
	dict:fetch({I,$$},Ps) ! N;
scan(N,<<I:8,J:8,Str/bits>>,Ps) ->
	%io:format("Msg:~p~n",[N]),
	dict:fetch({I,J},Ps) ! N,
	scan(N+1,<<J:8,Str/bits>>,Ps).
	


collect(0,Ls) ->
	Sorted = lists:keysort(1,Ls),
	merge(lists:reverse(Sorted),[]);
collect(N,Ls) ->
	receive
		{Key,Index} -> 
			collect(N-1,[{Key,Index}|Ls])
	end.	


merge([],Acc) -> Acc;
merge([{_,List}|Ls],Acc) -> merge(Ls,List++Acc).



start_branches(Str,Alphabet) ->
	[ {Key,spawn(?MODULE,st,[self(),Str,Key])} || Key <- [ {I,J} || I<-Alphabet, J<-Alphabet, I=/=$$] ].





st(Master,Str,Key) -> 
	%io:format("~p spawned~n",[Key]),
	st(Master,Str,Key,[]).

st(Master,Str,Key,Acc) ->
	receive
		finish -> Master ! {Key,lists:reverse(Acc)};
		J -> st(Master,Str,Key,[J|Acc])
	end.
