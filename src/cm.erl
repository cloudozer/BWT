% CIGAR maker module 
% 
% 
% Cloudozer(c), 2015
%

-module(cm).
-export([start_cigar_makers/4,
		cigar_maker/2
		]).

-include("bwt.hrl").


start_cigar_makers(N,Sink,SinkHost={SinkHost1,_},LingdRef) ->
	lists:foreach(  fun(I) ->
		NodeName = list_to_atom("cm_" ++ integer_to_list(I)),
		{ok,_} = lingd:create(LingdRef, SinkHost1, NodeName, []),
    ok = navel:call(NodeName,navel,connect,[SinkHost]),
		navel:call_no_return(NodeName, erlang, spawn, [?MODULE, cigar_maker,[{navel:get_node(),self()},Sink]])
	end,lists:seq(1,N)).



cigar_maker({AlqN,AlqP}=Alq, Sink={SinkN,SinkP}) ->
	navel:call_no_return(AlqN,erlang,send,[AlqP,{{navel:get_node(),self()},ready}]),
	receive
		{Ref,QsecBin,SeqName,Chunk,Pos} = M ->
%% 			io:format("CM: got read. Aligned: ", []),
			% run SW and send results to Sink
      Qsec = binary_to_list(QsecBin),
      case sw:sw(Qsec,Ref) of
        no_match -> ok;%io:format("no_match~n");
        {Score,CIGAR} ->
          io:format("CM cigar: ~p, ~p~n",[Score,CIGAR]),
          navel:call_no_return(SinkN, erlang, send, [SinkP, {SeqName,Chunk,Pos,Score,CIGAR,Ref}])
      end,

			spawn(?MODULE,cigar_maker,[Alq,Sink]);

		quit -> ok	
	end.
