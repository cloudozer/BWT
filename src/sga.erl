% An implementation of BWT-SW + heuristics
% 
% Cloudozer(c), 2015
%

-module(sga).
-export([sga/2]).

-define(TOLERANCE,10).



% performs sw algorithm for a sequence Seq against reference sequence represented by FM-index
sga(FM,Qseq) -> sga(FM,Qseq,[],0,0).
sga(FM, Qseq, Acc, Qty,End) ->
	case bwt:get_subseq(lists:sublist(Qseq,length(Qseq)-End)) of
		not_found -> 
			get_similar(Qty,Acc);
		Pos ->
			io:format("Pos: ~p~n",[Pos]),
			Subseq = lists:sublist(Qseq,length(Qseq)-Pos-End),
			{N,Sp,Ep,Seed_positons} = bwa:find_seeds(Subseq,FM),
			io:format("Substring lenght: ~p~n, Seeds: ~p~n",[N,Seed_positons]),
			L = length(Subseq),
			sga(FM, Subseq, [ S-L+N || S <- Seed_positons]++Acc, Qty+1, N-5  )
	end.



get_similar(Qty, Ls) -> 
	[P1|Ls1] = lists:sort(Ls),
	get_similar(Qty div 2, Ls1, P1, 1, [], ?TOLERANCE).

get_similar(N, [P2|Ls], P1, Count, Acc, Tol) when P2-P1 =< Tol ->
	get_similar(N, Ls, P2, Count+1, Acc, Tol);
get_similar(N, [P2|Ls], P1, Count, Acc, Tol) when Count >= N ->
	io:format("Qty:~p~n",[Count]),
	get_similar(N, Ls, P2, 1, [P1|Acc], Tol);
get_similar(N, [P2|Ls],_,_,Acc,Tol) ->
	get_similar(N, Ls, P2, 1, Acc, Tol);
get_similar(N, [], P1, Count, Acc,_) when Count >= N -> 
	io:format("Qty:~p~n",[Count]),
	[P1|Acc];
get_similar(_,[],_,_,Acc,_) -> Acc.




