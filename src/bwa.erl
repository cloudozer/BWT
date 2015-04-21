% An implementation of BWA
% 
% Cloudozer(c), 2015
%

-module(bwa).
-export([find_seeds/5]).

-define(MAX_RANGE,25).
%-define(MIN_LEN,9). % a minimal length of the string that should be matched
-define(MAX_LEN,13). % a maximal length of the string after which the range should be less than MAX_RANGE



%% returns the list of matches of the given Subsequence against 
%% the reference sequence represented by FM index

find_seeds(_,Sp,Ep,_,?MAX_LEN) when Ep-Sp > ?MAX_RANGE -> too_many_seeds;
find_seeds(FM,Sp,Ep,_,?MAX_LEN) -> sga:get_seed_ends(FM,Sp,Ep,?MAX_LEN); 
find_seeds(FM,Sp,Ep,[$N|Qseq],N) when Ep-Sp > ?MAX_RANGE -> too_many_seeds; 
find_seeds(FM,Sp,Ep,[$N|Qseq],N) -> sga:get_seed_ends(FM,Sp,Ep,N);
	
find_seeds(FM,Sp,Ep,[C2|Qseq],N) ->
	%io:format("Looking for {~p,~p}~n",[C1,C2]),
	%io:format("Range:[~p - ~p]~n",[Sp,Ep]),
	case se_down(Sp,Ep,C2,FM) of
		not_found -> sga:get_seed_ends(FM,Sp,Ep,N);
		Sp1 ->
			Ep1 = se_up(Ep,C2,FM),
			find_seeds(FM, Sp1, Ep1, Qseq, N+1)
	end;
find_seeds(_,Sp,Ep,[],_) when Ep-Sp > ?MAX_RANGE -> too_many_seeds;
find_seeds(FM,Sp,Ep,[],N) -> sga:get_seed_ends(FM,Sp,Ep,N);

find_seeds(FM, Pc,Pg,Pt, Subseq) -> %%%%%%%%  starting point  %%%%%%%%%%
	[H|Tail] = lists:reverse(Subseq),
	%initialize_sp_ep(H,1,size(FM),FM)
	case H of
		$A -> Sp = 2, Ep = Pc-1;
		$C -> Sp = Pc, Ep = Pg-1;
		$G -> Sp = Pg, Ep = Pt-1;
		$T -> Sp = Pt, Ep = size(FM);
		%% TODO: handle N symbols
		$N ->
			%io:format("N found in fasq seq~n"),  
			Sp = 2, Ep = Pc-1
	end,
	find_seeds(FM, Sp,Ep, Tail, 1).



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




