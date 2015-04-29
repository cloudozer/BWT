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
		get_index_pointers/1,
		get_seed_ends/4,
		len/1
		]).

-include("bwt.hrl").

-define(BLOCK_LEN,128).
-define(BLOCK_SHIFT,7).    %% 2^6=64
-define(BLOCK_ID_MASK,2#1111111).



se_down(Sp,Ep,C2,FM) -> 
	%io:format("Seeking: ~p - ",[C2]),
	se_down(Sp bsr ?BLOCK_SHIFT + 1,Sp band ?BLOCK_ID_MASK + 1,
								Ep bsr ?BLOCK_SHIFT + 1,Ep band ?BLOCK_ID_MASK + 1,
								C2,FM,element(Sp bsr ?BLOCK_SHIFT + 1,FM)).

se_down(E_id,IDe,E_id,IDe,_,_,_) -> 
	%io:format("not found~n"),
	not_found;
se_down(B_id,IDc,E_id,IDe,C2,FM,_) when IDc =:= ?BLOCK_LEN+1 -> 
	%io:format("Block_id:~p ID:~p~nE_id:~p IDe:~p~n",[B_id,IDc,E_id,IDe]),
	se_down(B_id+1,1,E_id,IDe,C2,FM,element(B_id+1,FM));
se_down(B_id,IDc,E_id,IDe,C2,FM,Block) ->
	case element(IDc,Block) of
		{_,C2,P,_} -> 
			%io:format(" - found. P=~p~n",[P]),
			P;
		{_,Other,_,_} ->
			%io:format("~p ",[Other]), 
			se_down(B_id,IDc+1,E_id,IDe,C2,FM,Block)
	end.



se_up(Ep,C2,FM) -> se_up(Ep bsr ?BLOCK_SHIFT + 1, Ep band ?BLOCK_ID_MASK + 1,
						C2,FM,element(Ep bsr ?BLOCK_SHIFT + 1,FM)).

se_up(B_id,0,C2,FM,_) -> 
	Block = element(B_id-1,FM),
	se_up(B_id-1,?BLOCK_LEN,C2,FM,Block);
se_up(B_id,IDc,C2,FM,Block) ->
	%io:format("IDc:~p ",[IDc]),
	case element(IDc,Block) of
		{_,C2,P,_} -> P;
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



get_index_pointers(FM) ->
	io:format("Finding starting pointers...~n"),
	Pc = find_pointer(FM,$C,1),
	io:format("Pc=~p~n",[Pc]),
	Pg = find_pointer(FM,$G,Pc+1),
	io:format("Pg=~p~n",[Pg]),
	Pt = find_pointer(FM,$T,Pg+1),
	io:format("Pt=~p~n",[Pt]),
	Last = len(FM)-1,
	io:format("Last=~p~n",[Last]),
	{Pc,Pg,Pt,Last}.


find_pointer(FM,Char,P) -> find_pointer(FM,Char,P bsr ?BLOCK_SHIFT + 1,
												P band ?BLOCK_ID_MASK + 1, 
												element(P bsr ?BLOCK_SHIFT + 1,FM)).

find_pointer(FM,Char,B_id,ID,_) when ID =:= ?BLOCK_LEN + 1 ->
	%io:format("~nBlock:~p~n",[B_id]),
	find_pointer(FM,Char,B_id+1,1,element(B_id+1,FM));
find_pointer(FM,Char,B_id,ID,Block) ->
	case element(ID,Block) of
		{Char,_,_,_} -> (B_id-1) bsl ?BLOCK_SHIFT + ID - 1;
		_ -> find_pointer(FM,Char,B_id,ID+1,Block)
	end.



% returns a list of SA positions where the matches happened
get_seed_ends(_,_,_,N) when N < ?MIN_LEN -> 
	%io:format("no seeds. N=~p~n",[N]),
	no_seeds; 
get_seed_ends(FM,Sp,Ep,N) -> 
	%io:format("Extracting seed ends from range: (~p,~p)~n",[Sp,Ep]),
	Sb = Sp bsr ?BLOCK_SHIFT + 1,
	Eb = Ep bsr ?BLOCK_SHIFT + 1,
	Sid = Sp band ?BLOCK_ID_MASK + 1,
	Eid = Ep band ?BLOCK_ID_MASK + 1,
	Block = element(Sb,FM),
	get_seed_ends(FM,Block,Sb,Sid,Eb,Eid,N,[]).

get_seed_ends(_,Block,Eb,Eid,Eb,Eid,N,Acc) -> 
	{_,_,_,SA} = element(Eid,Block),
	%io:format("~nSeeds: ~p~n",[[SA+N|Acc]]),
	[SA+N|Acc];
get_seed_ends(FM,_,Sb,Sid,Eb,Eid,N,Acc) when Sid =:= ?BLOCK_LEN+1 ->
	Block1 = element(Sb+1,FM),
	get_seed_ends(FM,Block1,Sb+1,1,Eb,Eid,N,Acc); 
get_seed_ends(FM,Block,Sb,Sid,Eb,Eid,N,Acc) -> 
	{_,_,_,SA} = element(Sid,Block),
	get_seed_ends(FM,Block,Sb,Sid+1,Eb,Eid,N,[SA+N|Acc]).



len(FM) -> size(element(size(FM),FM)) + ?BLOCK_LEN*(size(FM)-1).
