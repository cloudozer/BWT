% aligning queue module 
% 
% 
% Cloudozer(c), 2015
%

-module(alq).
-export([start_alq/4,
		alq/3
		]).

-define(CIGAR_MAKER_NBR,4).



% starts Alq in each box if there is one or more seed_finder
start_alq(Schedule,SinkHost,Sink,Lingd) -> start_alq(Schedule,SinkHost,Sink,Lingd,[]).

start_alq([{Box_id,Chunks}|Schedule],SinkHost,Sink,Lingd,Acc) ->
	NodeName = list_to_atom("alq_" ++ Box_id),
	{ok,AlqHost} = lingd:create(Lingd, NodeName, [{memory, 128}]),
	ok = navel:call(NodeName,navel,connect,[SinkHost]),
	ok = navel:call(NodeName,lingd,connect,[]),
	A = navel:call(NodeName,erlang,spawn,[?MODULE,alq,[Chunks,Sink,Lingd]]),
	start_alq(Schedule,SinkHost,Sink,Lingd,[{Box_id,{AlqHost,{NodeName,A}},[ C ||{C,_} <- Chunks]}|Acc]);

start_alq([],_,_,_,Acc) -> Acc.




alq(Chunks,Sink,Lingd) ->
	% make an url
	Refs = lists:zip(Chunks, http:get_refseq(Chunks)),
	%% spawn N cigar_makers
	cm:start_cigar_makers(?CIGAR_MAKER_NBR,Sink,Lingd),
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




confirm_fastq_done(0,Sink={N,P}) ->
	navel:call_no_return(N,erlang,send,[P,fastq_done]),
	io:format("Alq confirmed that fastq_done~n");
%% 	cm:start_cigar_makers(?CIGAR_MAKER_NBR,Sink);
confirm_fastq_done(N,Sink) ->
	receive
		{Pid,ready} ->
			% destroy cm instance
			Pid ! quit,
			confirm_fastq_done(N-1,Sink)
	end.



terminate_cm(0) -> ok;
terminate_cm(N) ->
	receive
		{Pid,ready} -> Pid ! quit, terminate_cm(N-1)
	end.



% reads the reference sequence from Chunk at position Pos
get_ref(_Refs,_Chunk,_Pos,_D) -> "AACGTAATG".
