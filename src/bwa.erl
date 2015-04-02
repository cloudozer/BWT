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
	{Pc,Pg,Pt} = {163080,275810,390251},
	case H of
		$A -> Sp = 2, Ep = Pc-1;
		$C -> Sp = Pc, Ep = Pg-1;
		$G -> Sp = Pg, Ep = Pt-1;
		$T -> Sp = Pt, Ep = size(FM);
                _ ->  Sp = 2, Ep = Pc-1
	end,
	%io:format("Sp:~p, Ep:~p~n",[Sp,Ep]),
	case find_seeds(1, Sp, Ep, Tail, FM) of
		not_found -> not_found;
		{Sp1,Ep1,N} -> 
			get_pos(N,[],Sp1,Ep1,FM,Sp)
	end.

find_seeds(N, Sp, Ep, [C2|Qseq], FM) ->
	%io:format("Looking for {~p,~p}~n",[C1,C2]),
	%io:format("Range:[~p - ~p]~n",[Sp,Ep]),
	case se_down(Sp,Ep,C2,FM) of
		not_found -> not_found;
		Sp1 ->
			Ep1 = se_up(Ep,C2,FM),
			case Ep1-Sp1 > ?RANGE of
				true -> find_seeds(N+1, Sp1, Ep1, Qseq, FM);
				_ -> {Sp1,Ep1,N+1}
			end
	end;
find_seeds(N, Sp, Ep, [], _) -> {Sp,Ep,N}.


se_down(Ep,Ep,C2,FM) -> 
	{_,L,P,_} = element(Ep,FM),
	case L == C2 of
		true -> P;
		_ -> not_found
	end;
se_down(Sp,Ep,C2,FM) -> 
	{_,L,P,_} = element(Sp,FM),
	case L == C2 of
		true -> P;
		_ -> se_down(Sp+1,Ep,C2,FM)
	end.


se_up(Ep,C2,FM) ->
	{_,L,P,_} = element(Ep,FM),
	case L == C2 of
		true -> P;
		_ -> se_up(Ep-1,C2,FM)
	end.




% returns a list of SA positions where the matches happened
get_pos(N,Acc,Ep,Ep,FM,Sp) -> 
	{_,_,_,SA} = element(Ep,FM),
	{N,Sp,Ep,[SA|Acc]};
get_pos(N,Acc,Sc,Ep,FM,Sp) ->
	{_,_,_,SA} = element(Sc,FM),
	get_pos(N,[SA|Acc],Sc+1,Ep,FM,Sp).


