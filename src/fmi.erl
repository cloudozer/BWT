% An implementation of FM index 
% as a two level tuples
% 
% Cloudozer(c), 2015
%


-module(fmi).
-export([
		se_down/4,
		se_up/3,
		assemble_index/9,
		get_3_pointers/1
		]).

-define(BLOCK_LEN,64).
-define(BLOCK_SHIFT,6).    %% 2^6=64
-define(BLOCK_ID_MASK,2#111111).



se_down(Sp,Ep,C2,FM) -> se_down(Sp bsr ?BLOCK_SHIFT,Sp band ?BLOCK_ID_MASK + 1,
								Ep bsr ?BLOCK_SHIFT,Ep band ?BLOCK_ID_MASK + 1,
								C2,FM,element(Sp bsr ?BLOCK_SHIFT + 1,FM)).

se_down(E_id,IDe,E_id,IDe,_,_,_) -> not_found;
se_down(B_id,IDc,E_id,IDe,C2,FM,_) when IDc =:= ?BLOCK_LEN+1 -> 
	se_down(B_id+1,1,E_id,IDe,C2,FM,element(B_id+1,FM));
se_down(B_id,IDc,E_id,IDe,C2,FM,Block) ->
	case element(IDc,Block) of
		{C2,_,_,_} -> (B_id bsl ?BLOCK_SHIFT) + IDc;
		_ -> se_down(B_id,IDc+1,E_id,IDe,C2,FM,Block)
	end.



se_up(Ep,C2,FM) -> se_up(Ep bsr ?BLOCK_SHIFT,Ep band ?BLOCK_ID_MASK + 1,
						C2,FM,element(Ep bsr ?BLOCK_SHIFT + 1,FM)).

se_up(B_id,0,C2,FM,_) -> se_up(B_id-1,?BLOCK_LEN,C2,FM,element(B_id-1,FM));
se_up(B_id,IDc,C2,FM,Block) ->
	case element(IDc,Block) of
		{C2,_,_,_} -> ((B_id-1) bsl ?BLOCK_SHIFT) + IDc - 1;
		_ -> se_up(B_id,IDc-1,C2,FM,Block)
	end.


assemble_index(FM,Acc,?BLOCK_LEN,Result,Dq,Aq,Cq,Gq,Tq) ->
	assemble_index(FM,[],0,[list_to_tuple(lists:reverse(Acc))|Result],Dq,Aq,Cq,Gq,Tq);

assemble_index([{F,L,SA}|FM],Acc,N,Result,Dq,Aq,Cq,Gq,Tq) ->
	case L of
		$A -> 
			[I|Aq1] = Aq, 
			assemble_index(FM,[{F,L,I,SA}|Acc],N+1,Result,Dq,Aq1,Cq,Gq,Tq);
		$C -> 
			[I|Cq1] = Cq, 
			assemble_index(FM,[{F,L,I,SA}|Acc],N+1,Result,Dq,Aq,Cq1,Gq,Tq);
		$G -> 
			[I|Gq1] = Gq, 
			assemble_index(FM,[{F,L,I,SA}|Acc],N+1,Result,Dq,Aq,Cq,Gq1,Tq);
		$T -> 
			[I|Tq1] = Tq, 
			assemble_index(FM,[{F,L,I,SA}|Acc],N+1,Result,Dq,Aq,Cq,Gq,Tq1);
		$$ -> 
			[I|Dq1] = Dq, 
			assemble_index(FM,[{F,L,I,SA}|Acc],N+1,Result,Dq1,Aq,Cq,Gq,Tq)
	end;
assemble_index([],Acc,_,Result,[],[],[],[],[]) -> 
	lists:reverse([list_to_tuple(lists:reverse(Acc))|Result]).



get_3_pointers(FM) ->
	Pc = find_pointer(FM,$C,2),
	io:format("Pc=~p~n",[Pc]),
	Pg = find_pointer(FM,$G,Pc+1),
	io:format("Pg=~p~n",[Pg]),
	Pt = find_pointer(FM,$T,Pg+1),
	io:format("Pt=~p~n",[Pt]),
	{Pc,Pg,Pt}.


find_pointer(FM,Char,P) -> find_pointer(FM,Char,P bsr ?BLOCK_SHIFT + 1,
												P band ?BLOCK_ID_MASK + 1, 
												element(P bsr ?BLOCK_SHIFT + 1,FM)).

find_pointer(FM,Char,B_id,ID,_) when ID =:= ?BLOCK_LEN + 1 ->
	io:format("~nBlock:~p~n",[B_id]),
	find_pointer(FM,Char,B_id+1,1,element(B_id+1,FM));
find_pointer(FM,Char,B_id,ID,Block) ->
	case element(ID,Block) of
		{Char,_,_,_} -> (B_id-1) bsl ?BLOCK_SHIFT + ID - 1;
		_ -> find_pointer(FM,Char,B_id,ID+1,Block)
	end.


