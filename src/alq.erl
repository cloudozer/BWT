% aligning queue module 
% 
% 
% Cloudozer(c), 2015
%

-module(alq).
-export([start_alq/5,
		alq/5
		]).

-define(CIGAR_MAKER_NBR,4).
-define(REF_EXTENSION_LEN, 200).

-include("bwt.hrl").



% starts Alq in each box if there is one or more seed_finder
start_alq(Schedule,SinkHost,Sink,Lingd,HttpStorage) -> start_alq(Schedule,SinkHost,Sink,Lingd,HttpStorage,[]).

start_alq([{Box_id,Chunks}|Schedule],SinkHost,Sink,Lingd,HttpStorage,Acc) ->
	NodeName = list_to_atom("alq_" ++ Box_id),
	{ok,AlqHost} = lingd:create(Lingd, NodeName, [{memory, 128}]),
	ok = navel:call(NodeName,navel,connect,[SinkHost]),
	ok = navel:call(NodeName,lingd,connect,[]),
	A = navel:call(NodeName,erlang,spawn,[?MODULE,alq,[Chunks,Sink,SinkHost,Lingd,HttpStorage]]),
	start_alq(Schedule,SinkHost,Sink,Lingd,HttpStorage,[{Box_id,{AlqHost,{NodeName,A}},[ C ||{C,_} <- Chunks]}|Acc]);

start_alq([],_,_,_,_,Acc) -> Acc.




alq(Chunks,Sink,SinkHost,Lingd,HttpStorage) ->
  Refs = lists:map(fun({Chunk,_}) ->
    FileName = re:replace(Chunk, ".fm", ".ref", [{return, list}]),
    {ok, Ref} = http:get(HttpStorage ++ "/fm_indices/" ++ FileName),
    Extension = list_to_binary(lists:duplicate(?REF_EXTENSION_LEN, $N)),
    Ref1 = <<Extension/binary, Ref/binary, Extension/binary>>,
    {Chunk,Ref1}
  end, Chunks),
	%% spawn N cigar_makers
	cm:start_cigar_makers(?CIGAR_MAKER_NBR,Sink,SinkHost,Lingd),
	alq_in(Refs,?CIGAR_MAKER_NBR,Sink).

alq_in(Refs,?CIGAR_MAKER_NBR,Sink) ->
	receive
    {shift,Chunk,Shift} ->
      {Chunk, Ref} = lists:keyfind(Chunk, 1, Refs),
      Refs1 = lists:keyreplace(Chunk, 1, Refs, {Chunk, Ref, Shift}),
      alq_in(Refs1,?CIGAR_MAKER_NBR,Sink);

		{Read,Chunk,Seeds} -> 
			alq_out(Refs,?CIGAR_MAKER_NBR,Sink,Read,Chunk,Seeds);

		quit ->
			terminate_cm(?CIGAR_MAKER_NBR);

		fastq_done ->
			io:format("Alq: got fastq_done from r_source~n"),
			confirm_fastq_done(?CIGAR_MAKER_NBR,Sink)
	end.



alq_out(Refs,?CIGAR_MAKER_NBR,Sink,Read,Chunk,[{Pos,D}|Seeds]) ->
  {Ref,Shift} = get_ref(Refs,Chunk,Pos,D),
	receive
		{Pid,ready} -> 
			Pid ! {Ref,Read,Chunk,Pos,D,Shift},
%% 			io:format("Alq: send seed to aligning~n"),
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
get_ref(Refs,Chunk,Pos,D) ->
  {Chunk,Ref,Shift} = lists:keyfind(Chunk,1,Refs),
  Ref_len = ?QSEC_LENGTH + D,
  Start_pos = Pos - Ref_len + ?REF_EXTENSION_LEN,

  <<_:Start_pos/bytes,Ref_seq:Ref_len/bytes,_/binary>> = Ref,
  Ref_seq1 = binary_to_list(Ref_seq),
  {Ref_seq1,Shift}.

