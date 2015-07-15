% aligning queue module 
% 
% 
% Cloudozer(c), 2015
%

-module(alq).
-export([start_alq/2,
		alq/2
		]).

-define(CIGAR_MAKER_NBR,4).



% starts Alq in each box if there is one or more seed_finder
start_alq(Schedule,Sink) -> start_alq(Schedule,Sink,[]).

start_alq([{Box_id,Chunks}|Schedule],Sink,Acc) ->	
	A = spawn(?MODULE,alq,[Chunks,Sink]),
	start_alq(Schedule,Sink,[{Box_id,A,[ C ||{C,_} <- Chunks]}|Acc]);

start_alq([],_,Acc) -> Acc.




alq(Chunks,Sink) ->
	Refs = lists:zip(Chunks, http:get_refseq(Chunks)),
	%% spawn N cigar_makers
	cm:start_cigar_makers(?CIGAR_MAKER_NBR,Sink),
	alq_in(Refs,?CIGAR_MAKER_NBR,Sink).

alq_in(Refs,?CIGAR_MAKER_NBR,Sink) ->
	receive
		{Read,Chunk,Seeds} -> 
			alq_out(Refs,?CIGAR_MAKER_NBR,Sink,Read,Chunk,Seeds);

		quit ->
			terminate_cm(?CIGAR_MAKER_NBR);

		fastq_done ->
			io:format("Alq: got fastq_done from r_source~n"),
			confirm_fastq_done(?CIGAR_MAKER_NBR,Sink)
	end.



alq_out(Refs,?CIGAR_MAKER_NBR,Sink,Read,Chunk,[{Pos,D}|Seeds]) ->
	Ref = get_ref(Refs,Chunk,Pos,D),
	receive
		{Pid,ready} -> 
			Pid ! {Ref,Read,Chunk,Pos,D},
			io:format("Alq: send seed to aligning~n"),
			alq_out(Refs,?CIGAR_MAKER_NBR,Sink,Read,Chunk,Seeds)
	end;
alq_out(Refs,?CIGAR_MAKER_NBR,Sink,_Read,_Chunk,[]) -> alq_in(Refs,?CIGAR_MAKER_NBR,Sink).




confirm_fastq_done(0,Sink) -> 
	Sink ! fastq_done,
	io:format("Alq confirmed that fastq_done~n"),
	cm:start_cigar_makers(?CIGAR_MAKER_NBR,Sink);
confirm_fastq_done(N,Sink) ->
	receive
		{Pid,ready} -> Pid ! quit, confirm_fastq_done(N-1,Sink)
	end.



terminate_cm(0) -> ok;
terminate_cm(N) ->
	receive
		{Pid,ready} -> Pid ! quit, terminate_cm(N-1)
	end.



% reads the reference sequence from Chunk at position Pos
get_ref(_Refs,_Chunk,_Pos,_D) -> "AACGTAATG".
