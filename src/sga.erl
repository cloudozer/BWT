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
			%io:format("Pos: ~p~n",[Pos]),
			Subseq = lists:sublist(Qseq,length(Qseq)-Pos-End),
			case length(Subseq) =< 11 of
				true -> get_similar(Qty,Acc);
				_ ->
					case bwa:find_seeds(Subseq,FM) of
						{N,Sp,Ep,Seed_positons} ->
							%io:format("Substring lenght: ~p~nSp:~p,Ep~p~n, Seeds: ~p~n",[N,Sp,Ep,Seed_positons]),
							L = length(Subseq),
							%Seeds = [ S-L+N || S <- Seed_positons],
							sga(FM, Subseq, add_seeds(Seed_positons,Acc,L,N), Qty+1, N-3 );
						not_found ->
							%io:format("Seeds not found~n"),
							Seeds = [],
							sga(FM, Subseq, Seeds++Acc, Qty+1, 10  )
					end	
			end
	end.


add_seeds(Seeds, Acc0,L,N) -> 
	lists:foldl(fun(S,Acc) -> [S-L+N |Acc]
				end,Acc0,Seeds).


get_similar(0, _) -> [];
get_similar(1, _) -> [];
get_similar(Qty, Ls) -> 
	[P1|Ls1] = lists:sort(Ls),
	get_similar(2+Qty div 5, Ls1, P1, 1, [], ?TOLERANCE).

get_similar(N, [P2|Ls], P1, Count, Acc, Tol) when P2-P1 =< Tol ->
	get_similar(N, Ls, P2, Count+1, Acc, Tol);
get_similar(N, [P2|Ls], P1, Count, Acc, Tol) when Count >= N ->
	%io:format("Qty:~p~n",[Count]),
	get_similar(N, Ls, P2, 1, [P1|Acc], Tol);
get_similar(N, [P2|Ls],_,_,Acc,Tol) ->
	get_similar(N, Ls, P2, 1, Acc, Tol);
get_similar(N, [], P1, Count, Acc,_) when Count >= N -> 
	%io:format("Qty:~p~n",[Count]),
	[P1|Acc];
get_similar(_,[],_,_,Acc,_) -> Acc.




