% CIGAR maker module 
% 
% 
% Cloudozer(c), 2015
%

-module(cm).
-export([start_cigar_makers/6,
		cigar_maker/2, t/0
		]).

-include("bwt.hrl").


t() -> 
	Ref = sw:rand_seq(101),
	lists:foreach(  fun(_)-> Qsec = sw:rand_seq(100), align(Qsec,Ref) 
					end,lists:seq(1,1000)).

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
			Self = self(),
			spawn_link( fun() ->
							Res = sw:sw(Qsec,Ref),
							Self ! {result, Res}
						end),
			receive {result, Result} -> Result end
	end.


cigar_maker({AlqN,AlqP}=Alq, Sink) ->
	navel:call_no_return(AlqN,erlang,send,[AlqP,{{navel:get_node(),self()},ready}]),
	receive
		Tasks = [_|_] -> align_tasks(Tasks,Sink);
			
		quit -> ok	
	end,
	cigar_maker(Alq,Sink).



align_tasks([{SeqName,Chunk,QsecBin,Ref_seeds}|Tasks],Sink={SinkN,SinkP}) ->
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
	end,
	align_tasks(Tasks,Sink);
align_tasks([],_Sink) -> ok.


