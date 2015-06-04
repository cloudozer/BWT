% An implementation of FM index 
% as a two level tuples
% 
% Cloudozer(c), 2015
%


-module(fmi).
-export([
		se_down/4,
		se_up/3,
		assemble_index/8,
		get_index_pointers/1,
		get_seed_ends/4
		]).

-include("bwt.hrl").


-define(BLOCK_LEN,32).
-define(BLOCK_SHIFT,7).
-define(BLOCK_ID_MASK,2#11111).


se_down(Ep,Ep,_,_) -> not_found;

se_down(Sp,Ep,C2,FM) -> 
	case element(Sp,FM) of
		{_,C2,P,_} -> P;
		{_,_,_,_} -> se_down(Sp+1,Ep,C2,FM)
	end.



se_up(Ep,C2,FM) -> 
	case element(Ep,FM) of
		{_,C2,P,_} -> P;
		_ -> se_up(Ep-1,C2,FM)
	end.



assemble_index([{F,L,SA}|FM],Acc,N,Dq,Aq,Cq,Gq,Tq) ->
	case L of
		$A -> 
			[I|Aq1] = Aq, 
			assemble_index(FM,[{F,L,I,SA}|Acc],N+1,Dq,Aq1,Cq,Gq,Tq);
		$C -> 
			[I|Cq1] = Cq, 
			assemble_index(FM,[{F,L,I,SA}|Acc],N+1,Dq,Aq,Cq1,Gq,Tq);
		$G -> 
			[I|Gq1] = Gq, 
			assemble_index(FM,[{F,L,I,SA}|Acc],N+1,Dq,Aq,Cq,Gq1,Tq);
		$T -> 
			[I|Tq1] = Tq, 
			assemble_index(FM,[{F,L,I,SA}|Acc],N+1,Dq,Aq,Cq,Gq,Tq1);
		$$ -> 
			[I|Dq1] = Dq, 
			assemble_index(FM,[{F,L,I,SA}|Acc],N+1,Dq1,Aq,Cq,Gq,Tq)
	end;
assemble_index([],Acc,_,[],[],[],[],[]) -> 
	lists:reverse(Acc).



get_index_pointers(FM) ->
	io:format("Finding starting pointers...~n"),
	Pc = find_pointer(FM,$C,1),
	io:format("Pc=~p~n",[Pc]),
	Pg = find_pointer(FM,$G,Pc+1),
	io:format("Pg=~p~n",[Pg]),
	Pt = find_pointer(FM,$T,Pg+1),
	io:format("Pt=~p~n",[Pt]),
	Last = size(FM),
	io:format("Last=~p~n",[Last]),
	{Pc,Pg,Pt,Last}.



find_pointer(FM,Char,P) -> 
	case element(P,FM) of
		{Char,_,_,_} -> P;
		_ -> find_pointer(FM,Char,P+1)
	end.



% returns a list of SA positions where the matches happened
get_seed_ends(_,_,_,N) when N < ?MIN_LEN -> 
	%io:format("no seeds. N=~p~n",[N]),
	no_seeds; 
get_seed_ends(FM,Sp,Ep,N) -> get_seed_ends(FM,Sp,Ep,N,[]).

get_seed_ends(FM,Ep,Ep,N,Acc) ->
	{_,_,_,SA} = element(Ep,FM),
	[SA+N|Acc];

get_seed_ends(FM,Sp,Ep,N,Acc) ->
	{_,_,_,SA} = element(Sp,FM),
	get_seed_ends(FM,Sp+1,Ep,N,[SA+N|Acc]).

