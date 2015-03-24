-module(fm).
-export([make_binary_index/3, read_file/1, element/3, size/1, encode_symbol/1, get_st/1]).
-export([loop/1]).
-record(state, {bins, pointers, size=0, st = []}).
-define(ELEMENT_SIZE, 9).

%% api

read_file(FileName) ->
  {ok,Bin} = file:read_file(FileName),
  Bytes = byte_size(Bin),
  %% Check size
  0 = Bytes rem ?ELEMENT_SIZE,

  {4, Pointers} = lists:foldl(fun(I, {Prev, {A,C,G,T}} = Acc) -> 
    F = binary:at(Bin, I*?ELEMENT_SIZE) bsr 4,
    case F of
      Prev -> Acc;
      1 -> {1, {I*?ELEMENT_SIZE, 0, 0, 0}};
      2 -> {2, {A, I*?ELEMENT_SIZE, 0, 0}};
      3 -> {3, {A, C, I*?ELEMENT_SIZE, 0}};
      4 -> {4, {A, C, G, I*?ELEMENT_SIZE}}
    end 
  end, {0, {0,0,0,0}}, lists:seq(0, (Bytes div ?ELEMENT_SIZE)-1)),

  {Ap,Cp,Gp,Tp} = Pointers,
  Bins = {
    binary:part(Bin, Ap, Cp-Ap),
    binary:part(Bin, Cp, Gp-Cp),
    binary:part(Bin, Gp, Tp-Gp),
    binary:part(Bin, Tp, Bytes-Tp)
  },
io:format("Pointers ~p~n", [Pointers]),
  spawn_link(?MODULE, loop, [#state{bins=Bins, pointers=Pointers, size=Bytes}]).

element(N, El, IndexPid) ->
  call(IndexPid, {element, N, El}).

size(IndexPid) ->
  call(IndexPid, size).

get_st(IndexPid) ->
  call(IndexPid, get_st).

%% server's private functions

loop(S=#state{st=St, pointers={PA, PC, PG, PT}, bins={BinA,BinC,BinG,BinT}, size=Size}) ->
  receive    
    {{element, N, El}, F} ->

      P = (N-1)*?ELEMENT_SIZE,
      {Bin, StartP} = 
        if PA =< P, P < PC -> {BinA, PA};
           PC =< P, P < PG -> {BinC, PC};
           PG =< P, P < PT -> {BinG, PG};
           PT =< P, P < Size -> {BinT, PT}
        end,

      << FL, I:32, SA:32 >> = binary:part(Bin, {P - StartP, ?ELEMENT_SIZE}),
      Result = case El of
        f -> FL bsr 4;
        s -> FL band 2#1111;
        i -> I;
        sa -> SA;
        s_i -> {FL band 2#1111, I}
      end,
      F ! Result;
    {size, F} ->
      F ! Size div ?ELEMENT_SIZE, S;
    {get_st, F} ->
      F ! St
  end,
  loop(S).

call(Pid, Msg) ->
  Pid ! {Msg, self()},
  receive Reply -> Reply end.

%% make index

make_binary_index(SourceFileName, Chromo, DestFileName) ->
  {Pos,Len} = msw:get_reference_position(Chromo,SourceFileName),
  Tuples = fm(msw:get_chunk(SourceFileName,Pos+3*(Len div 4),(Len div 4) - 13000)),
  Bin = encode_tuples(Tuples),
  file:write_file(DestFileName,Bin).

encode_tuples(Tuples) ->
  lists:map(fun encode_tuple/1, Tuples).

encode_tuple({F,L,I,SA}) ->
  << <<((encode_symbol(F) bsl 4) bor encode_symbol(L)):8>>/binary, <<I:32>>/binary, <<SA:32>>/binary >>.

encode_symbol(Symbol) ->
  case Symbol of
    $$ -> 0;
    $A -> 1;
    $C -> 2;
    $G -> 3;
    $T -> 4;
    $N -> 5
  end.

%% Ian's code

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
