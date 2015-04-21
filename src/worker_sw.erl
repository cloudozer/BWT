% Worker performs smith-waterman alignment and returns CIGAR string
% 
% Cloudozer(c), 2015
%


-module(worker_sw).
-export([
		worker/5
		]).



worker(MasterPID,Ref_bin,Qname,Qsec,Seeds) -> worker(MasterPID,Ref_bin,Qname,Qsec,Seeds,[]).

worker(MasterPID,Ref_bin,Qname,Qsec,[{S,D}|Seeds],Acc) ->
	Ref_len = length(Qsec) + D,
	Start_pos = (S - Ref_len) bsl 3,
	io:format("Start pos: ~p~nRef_len: ~p~n",[Start_pos,Ref_len]),

	<<_:Start_pos,Ref_seq:Ref_len/bytes,_/binary>> = Ref_bin,
	Ref = binary_to_list(Ref_seq),
	io:format("Reference: ~p~n",[Ref]),
	io:format("Query seq: ~p~n",[Qsec]),

	worker(MasterPID,Ref_bin,Qname,Qsec,Seeds,Acc);

worker(MasterPID,_,Qname,_,[],Acc) -> Acc.


	