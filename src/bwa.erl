% An implementation of BWA
% 
% Cloudozer(c), 2015
%

-module(bwa).
-export([find_seeds/2]).

-define(RANGE,20).



%% returns the list of matches of the given Qseq sequence against 
%% the reference sequence represented by FM index
find_seeds(Subseq,FM) ->
	[H|Tail] = lists:reverse(Subseq),
	%initialize_sp_ep(H,1,size(FM),FM)
	{Pc,Pg,Pt} = {3203297,5961109,8724872},
	case H of
		$A -> Sp = 2, Ep = Pc-1;
		$C -> Sp = Pc, Ep = Pg-1;
		$G -> Sp = Pg, Ep = Pt-1;
		$T -> Sp = Pt, Ep = fm:size(FM)
	end,
	io:format("Sp:~p, Ep:~p~n",[Sp,Ep]),
	case find_seeds(H, 1, Sp, Ep, Tail, FM) of
		no_match -> [];
		{Sp1,Ep1,N} -> 
			get_pos(N,[],Sp1,Ep1,FM,Sp)
	end.

find_seeds(C1, N, Sp, Ep, [C2|Qseq], FM) ->
	%io:format("Looking for {~p,~p}~n",[C1,C2]),
	%io:format("Range:[~p - ~p]~n",[Sp,Ep]),
	case se(Sp,not_found,Ep,not_found,C1,C2,FM) of
		not_found -> {Sp,Ep,N};
		{Sp1,Ep1} when Ep1-Sp1 > ?RANGE -> find_seeds(C2, N+1, Sp1, Ep1, Qseq, FM);
		{Sp1,Ep1} -> {Sp1,Ep1,N+1} 
	end;
find_seeds(_, N, Sp, Ep, [], _) -> {Sp,Ep,N}.



se(	Sp,found,Ep,found,_,_,FM) -> 
	{_,_,Sp1,SA1} = fm:element(Sp,FM),
	{_,_,Ep1,SA2} = fm:element(Ep,FM),
	%io:format("SA range: ~p,~p~n",[SA1,SA2]),
	{Sp1,Ep1};
se(Sp,State1,Ep,State2,C1,C2,FM) ->
	case State1 of
		not_found ->
			{State11, Sp1} = get_state(Sp,down,C1,C2,FM);
		found -> State11 = found, Sp1 = Sp
	end,
	case State2 of
		not_found ->
			{State22, Ep1} = get_state(Ep,up,C1,C2,FM);
		found -> State22 = found, Ep1 = Ep
	end,
	%io:format("(~p,~p), (~p,~p)~n",[Sp1,State11,Ep1,State22]),
	
	case Sp1 > Ep1 of
		true -> not_found;
		_ -> se(Sp1,State11,Ep1,State22,C1,C2,FM)
	end. 



get_state(P,Dir,C1,C2,FM) ->
	{F,L,_,_} = fm:element(P,FM),
	case {F,L} of
		{C1,C2} -> {found,P};
		_ ->
			case Dir of
				down -> {not_found,P+1};
				up -> {not_found,P-1}
			end
	end.



% returns a list of SA positions where the matches happened
get_pos(N,Acc,Ep,Ep,FM,Sp) -> 
	{_,_,_,SA} = fm:element(Ep,FM),
	{N,Sp,Ep,[SA|Acc]};
get_pos(N,Acc,Sc,Ep,FM,Sp) ->
	{_,_,_,SA} = fm:element(Sc,FM),
	get_pos(N,[SA|Acc],Sc+1,Ep,FM,Sp).


