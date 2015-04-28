% An implementation of FM index 
% as a two level tuples
% 
% Cloudozer(c), 2015
%


-module(fmi).
-export([
		se_down/4,
		se_up/3,
		fm/1
		]).

-define(BLOCK_LEN,64).
-define(BLOCK_SHIFT,6).    %% 2^6=64
-define(BLOCK_ID_MASK,2#111111).



se_down(Sp,Ep,C2,FM) -> se_down(Sp bsr ?BLOCK_SHIFT,Sp band ?BLOCK_ID_MASK,
								Ep bsr ?BLOCK_SHIFT,Ep band ?BLOCK_ID_MASK,
								C2,FM,element(Sp bsr ?BLOCK_SHIFT,FM)).

se_down(E_id,IDe,E_id,IDe,_,_,_) -> not_found;
se_down(B_id,IDc,E_id,IDe,C2,FM,_) when IDc =:= ?BLOCK_LEN+1 -> 
	se_down(B_id+1,1,E_id,IDe,C2,FM,element(B_id+1,FM));
se_down(B_id,IDc,E_id,IDe,C2,FM,Block) ->
	case element(IDc,Block) of
		C2 -> (B_id bsl ?BLOCK_SHIFT) + IDc;
		_ -> se_down(B_id,IDc+1,E_id,IDe,C2,FM,Block)
	end.



se_up(Ep,C2,FM) -> se_up(Ep bsr ?BLOCK_SHIFT,Ep band ?BLOCK_ID_MASK,
						C2,FM,element(Ep bsr ?BLOCK_SHIFT,FM)).

se_up(B_id,0,C2,FM,_) -> se_up(B_id-1,?BLOCK_LEN,C2,FM,element(B_id-1,FM));
se_up(B_id,IDc,C2,FM,Block) ->
	case element(IDc,Block) of
		C2 -> (B_id bsl ?BLOCK_SHIFT) + IDc;
		_ -> se_up(B_id,IDc-1,C2,FM,Block)
	end.



%% returns an FM index for a given reference sequence
fm(X) ->
	_ = statistics(runtime),
	Ls = bwt:sort_chuncks(bwt:get_suffs(X),100000),
	{_,T2} = statistics(runtime),
	io:format("Suffix array generation took: ~psec~n",[T2/1000]),
	%io:format("Sufs:~p~n",[Ls]),
	{FM,Dq,Aq,Cq,Gq,Tq} = fm(X,Ls,[],1,[],[],[],[],[]),
	{_,T3} = statistics(runtime),
	io:format("Building the queues took ~p sec~n",[T3/1000]),

	list_to_tuple(bwt:add_indices(FM,[],Dq,Aq,Cq,Gq,Tq)).
	%{_,T4} = statistics(runtime),
	%io:format("Building the index took ~p sec~n",[T4/1000]).



fm(X,[{[S|_],N,P}|Ls], Acc, K, Dq,Aq,Cq,Gq,Tq) ->
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


