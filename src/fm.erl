-module(fm).
-export([create/1, read_file/1, element/2, size/1, encode_symbol/1]).
-define(ELEMENT_SIZE, 9).

create(T) ->
  Tuples = fm(T),
  encode_tuples(Tuples).

element(N, Index) ->
  << FL, I:32, SA:32 >> = binary:part(Index, {(N-1)*?ELEMENT_SIZE, ?ELEMENT_SIZE}),
%%   io:format("~p ~p ~p~n~n", [N, FL bsr 4, FL band 2#1111]),
  {FL bsr 4, FL band 2#1111, I, SA}.

encode_tuples(Tuples) ->
  lists:map(fun encode_tuple/1, Tuples).

encode_tuple({F,L,I,SA}) ->
  Res = << <<((encode_symbol(F) bsl 4) bor encode_symbol(L)):8>>/binary, <<I:32>>/binary, <<SA:32>>/binary >>,
  << FL, _/binary >> = Res,
%%   io:format("~p ~p~n", [FL bsr 4, FL band 2#1111]),
  true = (FL bsr 4) == encode_symbol(F),
  true = (FL band 2#1111) == encode_symbol(L),

%%       io:format("~p -> ~p~n", [{F,L,I,SA}, Res]),
  Res.

encode_symbol(Symbol) ->
  case Symbol of
    $$ -> 0;
    $A -> 1;
    $C -> 2;
    $G -> 3;
    $T -> 4;
    $N -> 5
  end.

read_file(FileName) ->
  {ok,Bin} = file:read_file(FileName),
  Bin.

size(Index) ->
  Bytes = byte_size(Index),
  0 = Bytes rem ?ELEMENT_SIZE,
  Bytes div ?ELEMENT_SIZE.

%% returns an FM index for a given reference sequence
fm(X) ->
	_ = statistics(runtime),
	Ls = lists:sort(get_suffs(X)),
	{_,T2} = statistics(runtime),
	io:format("Suffix array generation took: ~psec~n",[T2/1000]),
	%io:format("Sufs:~p~n",[Ls]),
	{FM, Dq, Aq, Cq, Gq, Tq} = fm(X,Ls,[],1,[],[],[],[],[]),
	{_,T3} = statistics(runtime),
	io:format("Building the queues took ~p sec~n",[T3/1000]),

	add_indices(FM,[],Dq,Aq,Cq,Gq,Tq).



fm(X,[{[S|_],N,P}|Ls], Acc, K, Dq,Aq,Cq,Gq,Tq) ->
	%case N of
		%%%%%%%%%%%  {F,L,SA}
	%	0 -> Acc1 = [{S,$$,P}|Acc];
	%	J -> Acc1 = [{S,lists:nth(J,X),J}|Acc]
	%end,
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


get_suffs(X) ->
	get_suffs([],0,X++"$",$$).

get_suffs(Acc, N, [H|X],P) ->
	get_suffs([{[H|X],N,P}|Acc], N+1, X, H);
get_suffs(Acc,_,[],_) -> Acc.


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
