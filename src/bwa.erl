% An implementation of BWA
% 
% Cloudozer(c), 2015
%

-module(bwa).
-export([find_seeds/7]).

-include("bwt.hrl").


find_seeds(FM,SavedSeqs,Pc,Pg,Pt,Last, Subseq) -> %%%%%%%%  starting point  %%%%%%%%%%
	{Key,Tail} = lists:split(?SAVED_SEQ_LEN,lists:reverse(Subseq)), 
	case ets:lookup(SavedSeqs,Key) of
		[] -> 
			case add_new_key(FM,SavedSeqs,Pc,Pg,Pt,Last,Key) of
				{Sp,Ep} -> find_seeds(FM, Sp,Ep, Tail, ?SAVED_SEQ_LEN);
				no_seeds -> no_seeds;
				too_many_seeds -> too_many_seeds
			end;
			
		[{Key,Val}] ->
			case Val of
				{Sp,Ep} -> find_seeds(FM, Sp,Ep, Tail, ?SAVED_SEQ_LEN);
				no_seeds -> no_seeds;
				too_many_seeds -> too_many_seeds
			end
	end.


add_new_key(FM,SavedSeqs,Pc,Pg,Pt,Last,Key) ->
	[H|Tail] = Key,
	%initialize_sp_ep(H,1,size(FM),FM)
	case H of
		$A -> Sp = 1,  Ep = Pc-1;
		$C -> Sp = Pc, Ep = Pg-1;
		$G -> Sp = Pg, Ep = Pt-1;
		$T -> Sp = Pt, Ep = Last;
		%% TODO: handle N symbols
		$N ->
%% 			io:format("N found in fasq seq~n"),
			Sp = 1, Ep = Pc-1
	end,
	Interval = find_interval(FM, Sp,Ep, Tail, 1),
	ets:insert(SavedSeqs,{Key,Interval}),
	Interval.



%% returns the list of matches of the given Subsequence against 
%% the reference sequence represented by FM index
find_seeds(_,Sp,Ep,_,?MAX_LEN) when Ep-Sp > ?MAX_RANGE -> too_many_seeds;
find_seeds(FM,Sp,Ep,_,?MAX_LEN) -> fmi:get_seed_ends(FM,Sp,Ep,?MAX_LEN); 
find_seeds(_,Sp,Ep,[$N|_],_) when Ep-Sp > ?MAX_RANGE -> too_many_seeds; 
find_seeds(FM,Sp,Ep,[$N|_],N) -> fmi:get_seed_ends(FM,Sp,Ep,N);
	
find_seeds(FM,Sp,Ep,[C2|Qseq],N) ->
	case fmi:se_down(Sp,Ep,C2,FM) of
		not_found -> fmi:get_seed_ends(FM,Sp,Ep,N);
		Sp1 ->
			Ep1 = fmi:se_up(Ep,C2,FM),
			%io:format("New range: (~p, ~p)~n",[Sp1,Ep1]),
			find_seeds(FM, Sp1, Ep1, Qseq, N+1)
	end;
find_seeds(_,Sp,Ep,[],_) when Ep-Sp > ?MAX_RANGE -> too_many_seeds;
find_seeds(FM,Sp,Ep,[],N) -> fmi:get_seed_ends(FM,Sp,Ep,N).




find_interval(_,_Sp,_Ep,[$N|_],_) -> too_many_seeds; 
find_interval(FM,Sp,Ep,[C2|Qseq],N) ->
	case fmi:se_down(Sp,Ep,C2,FM) of
		not_found -> no_seeds;
		Sp1 ->
			Ep1 = fmi:se_up(Ep,C2,FM),
			%io:format("New range: (~p, ~p)~n",[Sp1,Ep1]),
			find_interval(FM, Sp1, Ep1, Qseq, N+1)
	end;
find_interval(FM,Sp,Ep,[],_) -> 
	Max_range = size(FM)/math:pow(4,?SAVED_SEQ_LEN)/1.2,
	case Ep-Sp > Max_range of
		true -> too_many_seeds;
		_ -> {Sp,Ep}
	end.




