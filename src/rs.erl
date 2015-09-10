% reads source module 
% 
% 
% Cloudozer(c), 2015
%


-module(rs).
-export([
  start/2,
  r_source/8
]).

-define(SINK_CONFIRM_TIMEOUT,10000).
-define(SEQ_FILE_CHUNK_SIZE,100000000).


start(SeqFileName, HttpStorage) ->
	Pid = spawn(fun() ->
		SeqFileNameUrl = HttpStorage ++ "/" ++ SeqFileName,

		{ok,Headers} = http:get_headers(SeqFileNameUrl),
		ContentLength = binary_to_integer(proplists:get_value(<<"Content-Length">>, Headers)),
		DownloadedSize = case ContentLength > ?SEQ_FILE_CHUNK_SIZE of
			true ->
				{ok,Headers1,Reads} = http:get(SeqFileNameUrl, [{"Range", http:range(0, ?SEQ_FILE_CHUNK_SIZE-1)}]),
				http:get_async(SeqFileNameUrl, [{"Range", http:range(?SEQ_FILE_CHUNK_SIZE, 2*?SEQ_FILE_CHUNK_SIZE-1)}]),
				?SEQ_FILE_CHUNK_SIZE;
			false ->
				{ok,Headers1,Reads} = http:get(SeqFileNameUrl),
				ContentLength
		end,

		receive
			{run, Alqs,SFs, SinkRef} ->
StartTime = now(),
				r_source(Reads, SeqFileNameUrl, ContentLength, DownloadedSize, Alqs,SFs,length(SFs),SinkRef),
io:format("Fastq completed within ~p secs.", [timer:now_diff(now(), StartTime) / 1000000])
		end
	end),
	Ref = {navel:get_node(), Pid},
	{ok, Ref}.



produce_workload(N, Fastq) ->
  produce_workload(N, Fastq, []).

produce_workload(0, Bin, Acc) ->
  {Bin, Acc};
produce_workload(Size, Bin, Acc) ->
  case get_next_read(Bin) of
    {false,Bin1} -> {Bin1,Acc};
    {SName,SData,Bin1} -> produce_workload(Size-1,Bin1,[{SName, SData}|Acc])
  end.

get_next_read(Bin) ->
  case binary:split(Bin, <<$\n>>) of
    [<<$@, SName/binary>>, Bin1] ->
      case binary:split(Bin1, <<$\n>>) of
        [SData, Bin2] -> 
          case binary:split(Bin2, <<$\n>>) of
            [<<"+">>,Bin3] -> 
              case binary:split(Bin3, <<$\n>>) of
                [_,Bin4] ->
                  {SName,SData,Bin4};
                [_] ->
                  {false,Bin}
              end;
            [_,_] -> {false,Bin};
            [_] -> {false,Bin}
          end;
        [_] -> {false,Bin}
      end;
    [_, Bin1] -> get_next_read(Bin1);
    [Bin] -> {false,Bin}
  end.


r_source(Reads,SeqUrl,ContentLength,DownloadedSize,Alqs,SFs,0,Sink) ->
  case produce_workload(10000, Reads) of
    {Reads1, []} -> 
      case ContentLength =:= DownloadedSize of
        true ->
          io:format("Tail of fastq: ~p~n",[Reads1]),
          terminate(Alqs,length(SFs)),

          io:format("~n\trs finished fastq distribution and is waiting for confirmation from sink~n"),
          case get_sink_confirmation(Sink) of
            {timeout,Time} -> 
              io:format("No confirmation got from sink during ~psec~n",[Time]),
              shutdown_cluster(Alqs,SFs,Sink);
            Time -> 
              io:format("Source waited for confirmation ~p msec~n",[Time]),
              % request next fastq file
              shutdown_cluster(Alqs,SFs,Sink)
          end;

        false ->
          receive
            {got_async, Headers, ReadsNext} ->
              ContentLength1 = binary_to_integer(proplists:get_value(<<"Content-Length">>, Headers)),
              ContentLength1 = size(ReadsNext),
              if DownloadedSize+ContentLength1 < ContentLength ->
                DownloadedSize1 = DownloadedSize + ContentLength1,
                http:get_async(SeqUrl, [{"Range", http:range(DownloadedSize1, DownloadedSize1+?SEQ_FILE_CHUNK_SIZE-1)}]),
  io:format("Fastq downloaded ~b%~n", [round(100*(DownloadedSize+ContentLength1)/ContentLength)]);
              true ->
                ok
              end
          end,
          r_source(<< Reads1/binary, ReadsNext/binary >> ,SeqUrl,ContentLength,DownloadedSize+ContentLength1,Alqs,SFs,0,Sink)
      end;
    {Reads1, Batch} -> 
      multicast(Batch,SFs),
      r_source(Reads1,SeqUrl,ContentLength,DownloadedSize,Alqs,SFs,length(SFs),Sink)
  end;


r_source(Reads,SeqUrl,ContentLength,DownloadedSize,Alqs,SFs,N,Sink) when N == 1 ->
	receive
		{Pid,ready} -> 
			io:format("ready ~p: ~p~n", [os:timestamp(), Pid]),
			r_source(Reads,SeqUrl,ContentLength,DownloadedSize,Alqs,SFs,N-1,Sink)
	end;

r_source(Reads,SeqUrl,ContentLength,DownloadedSize,Alqs,SFs,N,Sink) ->
	receive
		{_Pid,ready} -> 
			r_source(Reads,SeqUrl,ContentLength,DownloadedSize,Alqs,SFs,N-1,Sink)
	end.


terminate(Alqs,0) ->
	% multicast it
	lists:foreach(  fun({ANode,APid})-> navel:call_no_return(ANode, erlang, send, [APid, fastq_done])
					end,Alqs);
terminate(Alqs,J) ->
	terminate(Alqs,J-1).
	

multicast(Batch,SFs) ->
%% 	lists:foreach(fun({_,SF})-> SF ! {data,Batch} end, SFs).  % {_,SF} = {box,pid}
	lists:foreach(fun({Node,SF})-> navel:call_no_return(Node,erlang,send,[SF,{data,Batch}]) end, SFs).  % {_,SF} = {box,pid}



get_sink_confirmation(Sink) ->
	statistics(wall_clock),
	receive
		{Sink,fastq_done} -> {_,T} = statistics(wall_clock), T;
    Err -> throw({Sink,Err})

	after 
		?SINK_CONFIRM_TIMEOUT -> {timeout,?SINK_CONFIRM_TIMEOUT/1000}
	end.



shutdown_cluster(Alqs,SFs,{SinkN,SinkP}) ->
  navel:call_no_return(SinkN,erlang,send,[SinkP,quit]),
	lists:foreach(fun({N,P})-> navel:call_no_return(N,erlang,send,[P,quit]) end, Alqs),
	lists:foreach(fun({N,P})-> navel:call_no_return(N,erlang,send,[P,quit]) end, SFs).


