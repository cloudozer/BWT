% CIGAR maker module 
% 
% 
% Cloudozer(c), 2015
%

-module(cm).
-export([start_cigar_makers/6,
		cigar_maker/2
		]).

-include("bwt.hrl").


start_cigar_makers(N,Sink,SinkHost,AlqHost,BoxName,LingdRef) ->
	lists:foreach(  fun(I) ->
		NodeName = list_to_atom("cm_" ++ integer_to_list(I) ++ "_" ++ atom_to_list(BoxName)),
		{ok,_} = lingd:create(LingdRef, AlqHost, NodeName, []),
                ok = navel:call(NodeName,navel,connect,[SinkHost]),
		navel:call_no_return(NodeName, erlang, spawn, [?MODULE, cigar_maker,[{navel:get_node(),self()},Sink]])
	end,lists:seq(1,N)).


align(Qsec,Ref) ->
	case length(Qsec)=:=length(Ref) of
		true -> sw:simple_match(Qsec,Ref);
		false-> 
			spawn_link( fun() ->
							Res = sw:sw(Qsec,Ref),
							self() ! {result, Res}
						end),
			receive {result, Result} -> Result end
	end.


cigar_maker({AlqN,AlqP}=Alq, Sink={SinkN,SinkP}) ->
	navel:call_no_return(AlqN,erlang,send,[AlqP,{{navel:get_node(),self()},ready}]),
	receive
		{SeqName,Chunk,QsecBin,Ref_seeds} ->
			Qsec = binary_to_list(QsecBin),

			SAM_lines = lists:foldl(
				fun({Pos,Ref},Acc)->
					case align(Qsec,Ref) of
						no_match -> Acc;
						{Score,CIGAR} -> [{SeqName,Chunk,Pos,Score,CIGAR,Ref}|Acc]
					end
				end,[],Ref_seeds),

			case length(SAM_lines) =:= 0 of
				true -> ok;
				_ -> navel:call_no_return(SinkN,erlang,send,[SinkP,SAM_lines])
			end;
			
		quit -> ok	
	end,
	cigar_maker(Alq,Sink).

