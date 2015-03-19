% An implementation of BWT-SW + heuristics
% 
% Cloudozer(c), 2015
%

-module(sga).
-export([sga/2]).

-define(RANGE,30).



% performs sw algorithm for a sequence Seq against reference sequence represented by FM-index
sga(FM,Qseq) -> sga(FM,Qseq,[],0).
sga(FM, Qseq, Acc, End) ->
	case bwt:get_subseq(lists:sublist(Qseq,length(Qseq)-End)) of
		not_found -> lists:sort(Acc);
		Pos ->
			io:format("Pos: ~p~n",[Pos]),
			Subseq = lists:sublist(Qseq,length(Qseq)-Pos-End),
			{N,Sp,Ep,Seed_positons} = bwa:find_seeds(Subseq,FM),
			io:format("Substring lenght: ~p~n, Seeds: ~p~n",[N,Seed_positons]),
			L = length(Subseq),
			sga(FM, Subseq, [ S-L+N || S <- Seed_positons]++Acc, N div 2)
	end.


