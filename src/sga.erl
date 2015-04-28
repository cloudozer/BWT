% An implementation of BWT-SW + heuristics
% 
% Cloudozer(c), 2015
%

-module(sga).
-export([
		sga/5,
		get_seed_ends/4,
		skip_Ns/1
		]).

-define(TOLERANCE,10).
-define(MIN_LEN,8). % a minimal length of the string that should be matched



% performs sw algorithm for a sequence Seq against reference sequence represented by FM-index
sga(FM,Pc,Pg,Pt,Qseq) -> sga(FM,Pc,Pg,Pt,Qseq,[],0,0).
sga(FM,Pc,Pg,Pt,Qseq,Acc,Qty,Shift) -> 
	case skip_Ns(Qseq) of
		no_more_subseqs -> get_similar(Qty,Acc);
		{Skip,Qseq1} ->
			Qlen = length(Qseq1),
			case bwa:find_seeds(FM,Pc,Pg,Pt,Qseq1) of
				no_seeds ->
					%io:format("Seeds not found~n"),
					sga(FM,Pc,Pg,Pt,lists:sublist(Qseq1,Qlen-?MIN_LEN), Acc, Qty,Shift+?MIN_LEN+Skip);
				too_many_seeds ->
					%io:format("Got too many seeds for a subseq: ~p~n",[Qseq]),
					sga(FM,Pc,Pg,Pt,lists:sublist(Qseq1,Qlen-?MIN_LEN),Acc,Qty,Shift+?MIN_LEN+Skip);
				Seed_ends ->
					%io:format("~p seeds found: ~p~n",[length(Seed_ends),[S+Shift||S<-Seed_ends]]),
					sga(FM,Pc,Pg,Pt,lists:sublist(Qseq1,Qlen-?MIN_LEN),add_seeds(Seed_ends,Acc,Shift+Skip),Qty+1,Shift+?MIN_LEN+Skip)					
			end
	end.


add_seeds(Seed_ends, Acc0,Shift) -> 
	lists:foldl(fun(S,Acc) -> [ S+Shift |Acc]
				end,Acc0,Seed_ends).


get_similar(0, _) -> [];
get_similar(1, _) -> [];
get_similar(Qty, Ls) -> 
	[P1|Ls1] = lists:sort(Ls),
	get_similar(2+(Qty div 5), Ls1, P1, 1, 0,[], ?TOLERANCE).

get_similar(N, [P2|Ls], P1, Count, Dist, Acc, Tol) when P2-P1 =< Tol ->
	get_similar(N, Ls, P2, Count+1, Dist+P2-P1, Acc, Tol);
get_similar(N, [P2|Ls], P1, Count, Dist, Acc, Tol) when Count >= N ->
	%io:format("Seed distance:~p~n",[Dist]),
	get_similar(N, Ls, P2, 1, 0, [{P1,Dist}|Acc], Tol);
get_similar(N, [P2|Ls],_,_,_,Acc,Tol) ->
	get_similar(N, Ls, P2, 1, 0, Acc, Tol);
get_similar(N, [], P1, Count, Dist, Acc,_) when Count >= N -> 
	%io:format("Qty:~p~n",[Count]),
%% 	io:format("Similar: ~p~n",[[{P1,Dist}|Acc]]),
	[{P1,Dist}|Acc];
get_similar(_,[],_,_,_,[],_) -> [];
get_similar(_,[],_,_,_,Acc,_) -> 
%% 	io:format("Similar: ~p~n",[Acc]),
	Acc.


% scans from the end. If finds N, cuts the Qseq up to the N and repeats again
% returns new Subseq of len at least MIN_LEN 
skip_Ns(Qseq) when length(Qseq) < ?MIN_LEN -> no_more_subseqs;
skip_Ns(Qseq) -> skip_Ns(Qseq,lists:reverse(Qseq),1,0).

skip_Ns(Qseq,_,?MIN_LEN,0) -> {0,Qseq};
skip_Ns(Qseq,_,?MIN_LEN,Skipped) -> {Skipped,lists:sublist(Qseq,length(Qseq)-Skipped)};
skip_Ns(Qseq,[$N|Ls],J,Skipped) -> skip_Ns(Qseq,Ls,1,Skipped+J);
skip_Ns(Qseq,[_|Ls],J,Skipped) -> skip_Ns(Qseq,Ls,J+1,Skipped);
skip_Ns(_,[],_,_) -> no_more_subseqs.


% returns a position referenced from the end of the query sequence, which is a good pattern for seeds
%get_subseq(Qseq) -> get_subseq(lists:reverse(Qseq), [], 0).

%get_subseq(_,Queue,Pos) when length(Queue) == ?MIN_LEN -> Pos;
%get_subseq([_],_,_) -> no_more;
%get_subseq([X1,X2|Seq], Queue, Pos) when X1=:=$C; X1=:=$G; X2=:=$C; X2=:=$G ->
%	get_subseq([X2|Seq], [{{X1,X2},1}|Queue], Pos);
%get_subseq(_,_,Pos) when Pos >= 7 -> 1;
%get_subseq([X1,X2|Seq], Queue, Pos) ->
	%io:format("{~p,~p}, Q: ~p~n",[X1,X2,Queue]),
%	case lists:keyfind({X1,X2},1,Queue) of
%		false -> get_subseq([X2|Seq], [{{X1,X2},1}|Queue], Pos);
%		_ -> 
%			{Queue1,Pos1} = remove(X1,X2, lists:reverse([{{X1,X2},1}|Queue]), 1 ),
%			get_subseq([X2|Seq], Queue1, Pos+Pos1)
%	end.


%remove(X1,X2, [{{X1,X2},1}|Ls], N) -> {lists:reverse(Ls),N};
%remove(X1,X2, [_|Ls], N) -> remove(X1,X2, Ls, N+1).



% returns a list of SA positions where the matches happened
get_seed_ends(_,_,_,N) when N < ?MIN_LEN -> no_seeds; 
get_seed_ends(FM,Sp,Ep,N) -> get_seed_ends(FM,Sp,Ep,N,[]).

get_seed_ends(FM,Ep,Ep,N,Acc) -> 
	{_,_,_,SA} = element(Ep,FM),
	[SA+N|Acc];
get_seed_ends(FM,Sp,Ep,N,Acc) -> 
	{_,_,_,SA} = element(Sp,FM),
	get_seed_ends(FM,Sp+1,Ep,N,[SA+N|Acc]).






