% An implementation of BWT-SW + heuristics
% 
% Cloudozer(c), 2015
%

-module(sga).
-export([
		sga/6,
		skip_Ns/1
		]).

-include("bwt.hrl").



% performs sw algorithm for a sequence Seq against reference sequence represented by FM-index
sga(FM,Pc,Pg,Pt,Last,Qseq) -> sga(FM,Pc,Pg,Pt,Last,Qseq,[],0,0).
sga(FM,Pc,Pg,Pt,Last,Qseq,Acc,Qty,Shift) -> 
	case skip_Ns(Qseq) of
		no_more_subseqs -> get_similar(Qty,Acc);
		{Skip,Qseq1} ->
			Qlen = length(Qseq1),
			case bwa:find_seeds(FM,Pc,Pg,Pt,Last,Qseq1) of
				no_seeds ->
					%io:format("Seeds not found~n"),
					sga(FM,Pc,Pg,Pt,Last,lists:sublist(Qseq1,Qlen-?MIN_LEN), Acc, Qty,Shift+?MIN_LEN+Skip);
				too_many_seeds ->
					%io:format("Got too many seeds for a subseq: ~p~n",[Qseq]),
					sga(FM,Pc,Pg,Pt,Last,lists:sublist(Qseq1,Qlen-?MIN_LEN),Acc,Qty,Shift+?MIN_LEN+Skip);
				Seed_ends ->
					%io:format("~p seeds found: ~p~n",[length(Seed_ends),[S+Shift||S<-Seed_ends]]),
					sga(FM,Pc,Pg,Pt,Last,lists:sublist(Qseq1,Qlen-?MIN_LEN),add_seeds(Seed_ends,Acc,Shift+Skip),Qty+1,Shift+?MIN_LEN+Skip)					
			end
	end.


add_seeds(Seed_ends, Acc0,Shift) -> 
	lists:foldl(fun(S,Acc) -> [ S+Shift |Acc]
				end,Acc0,Seed_ends).


get_similar(0, _) -> [];
get_similar(1, _) -> [];
get_similar(Qty, Ls) -> 
	[P1|Ls1] = lists:sort(Ls),
	get_similar(2+(Qty div 7), Ls1, P1, 1, 0,[], ?TOLERANCE).

get_similar(N, [P2|Ls], P1, Count, Dist, Acc, Tol) when P2-P1 =< Tol ->
	get_similar(N, Ls, P2, Count+1, Dist+P2-P1, Acc, Tol);
get_similar(N, [P2|Ls], P1, Count, Dist, Acc, Tol) when Count >= N ->
	%io:format("Seed distance:~p~n",[Dist]),
	get_similar(N, Ls, P2, 1, 0, [{P1,Dist}|Acc], Tol);
get_similar(N, [P2|Ls],_,_,_,Acc,Tol) ->
	get_similar(N, Ls, P2, 1, 0, Acc, Tol);
get_similar(N, [], P1, Count, Dist, Acc,_) when Count >= N -> 
	%io:format("Qty:~p~n",[Count]),
 	io:format("Similar: ~p~n",[[{P1,Dist}|Acc]]),
	[{P1,Dist}|Acc];
get_similar(_,[],_,_,_,[],_) -> [];
get_similar(_,[],_,_,_,Acc,_) -> 
 	io:format("Similar: ~p~n",[Acc]),
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

