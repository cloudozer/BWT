% An implementation of BWA
% 
% Cloudozer(c), 2015
%

-module(bwa).
-export([find_seeds/7]).

-include("bwt.hrl").
-define(SAVED_SEQ_LEN,6).


find_seeds(FM,SavedSeqs,Pc,Pg,Pt,Last, Subseq) -> %%%%%%%%  starting point  %%%%%%%%%%
	{Key,Tail} = lists:split(?SAVED_SEQ_LEN,lists:reverse(Subseq)), 
	case dict:is_key(Key,SavedSeqs) of
		true -> 
			case dict:fetch(Key,SavedSeqs) of
				{Sp,Ep} -> {find_seeds(FM, Sp,Ep, Tail, ?SAVED_SEQ_LEN),SavedSeqs};
				no_seeds -> {no_seeds,SavedSeqs};
				too_many_seeds ->{ too_many_seeds,SavedSeqs}
			end;
		false->
			SavedSeqs1 = add_new_key(FM,SavedSeqs,Pc,Pg,Pt,Last,Key),
			find_seeds(FM,SavedSeqs1,Pc,Pg,Pt,Last, Subseq)
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
	Max_range = size(FM)/math:pow(4,?SAVED_SEQ_LEN),
	case Interval of
		{Sp,Ep} when Ep-Sp > Max_range -> 
			dict:store(Key,too_many_seeds,SavedSeqs);
		not_found ->
			dict:store(Key,no_seeds,SavedSeqs);
		_ ->
			dict:store(Key,Interval,SavedSeqs)
	end.


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
		not_found -> not_found;
		Sp1 ->
			Ep1 = fmi:se_up(Ep,C2,FM),
			%io:format("New range: (~p, ~p)~n",[Sp1,Ep1]),
			find_interval(FM, Sp1, Ep1, Qseq, N+1)
	end;
find_interval(_,Sp,Ep,[],_) -> {Sp,Ep}.




