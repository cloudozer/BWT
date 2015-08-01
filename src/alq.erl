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
-define(REF_EXTENSION_LEN, 200).

-include("bwt.hrl").



% starts Alq in each box if there is one or more seed_finder
start_alq(Schedule,SinkHost,Sink,Lingd) -> start_alq(Schedule,SinkHost,Sink,Lingd,[]).

start_alq([{Box_id,Chunks}|Schedule],SinkHost,Sink,Lingd,Acc) ->
	NodeName = list_to_atom("alq_" ++ Box_id),
	{ok,AlqHost} = lingd:create(Lingd, NodeName, [{memory, 128}]),
	ok = navel:call(NodeName,navel,connect,[SinkHost]),
	ok = navel:call(NodeName,lingd,connect,[]),
	A = navel:call(NodeName,erlang,spawn,[?MODULE,alq,[Sink,SinkHost,Lingd]]),
	start_alq(Schedule,SinkHost,Sink,Lingd,[{Box_id,{AlqHost,{NodeName,A}},[ C ||{C,_} <- Chunks]}|Acc]);

start_alq([],_,_,_,Acc) -> Acc.


alq(Sink,SinkHost,Lingd) ->
  	%% spawn N cigar_makers
	cm:start_cigar_makers(?CIGAR_MAKER_NBR,Sink,SinkHost,Lingd),
	cm_balancer(?CIGAR_MAKER_NBR,Sink,[],[]).


cm_balancer(?CIGAR_MAKER_NBR,Sink,[],[]) ->
	receive
		quit -> terminate_cm(?CIGAR_MAKER_NBR);
		{_Read_name,_Chromo,_Read,_Ref_seeds}=Task -> cm_balancer(?CIGAR_MAKER_NBR,Sink,[Task],[]);
		{Pid,ready} -> cm_balancer(?CIGAR_MAKER_NBR,Sink,[],[Pid])		
	end;

cm_balancer(?CIGAR_MAKER_NBR,Sink,[],[Pid|CMs]) ->
	receive
		quit -> terminate_cm(?CIGAR_MAKER_NBR);

		{Read_name,Chromo,Read,[{Pos,Ref}]} -> 
			Pid ! {Ref,Read,Read_name,Chromo,Pos},
			cm_balancer(?CIGAR_MAKER_NBR,Sink,[],[CMs]);

		{Read_name,Chromo,Read,[{Pos,Ref}|Ref_seeds]} -> 
			Pid ! {Ref,Read,Read_name,Chromo,Pos},
			cm_balancer(?CIGAR_MAKER_NBR,Sink,[{Read_name,Chromo,Read,Ref_seeds}],[CMs]);

		{Pid1,ready} -> cm_balancer(?CIGAR_MAKER_NBR,Sink,[],[Pid1,Pid|CMs]);

		fastq_done when length([Pid|CMs]) =:= ?CIGAR_MAKER_NBR ->
			{N,P} = Sink,
			navel:call_no_return(N,erlang,send,[P,fastq_done]),
			io:format("Alq confirmed that fastq_done~n")
	end;

cm_balancer(?CIGAR_MAKER_NBR,Sink,[{Read_name,Chromo,Read,[{Pos,Ref}|Ref_seeds]}|Tasks],[]) ->
	receive
		quit -> terminate_cm(?CIGAR_MAKER_NBR);

		{_Read_name,_Chromo,_Read,_Ref_seeds}=Task -> cm_balancer(?CIGAR_MAKER_NBR,Sink,[Task|Tasks],[]);

		{Pid,ready} -> 
			Pid ! {Ref,Read,Read_name,Chromo,Pos},
			case Ref_seeds of
				[] -> cm_balancer(?CIGAR_MAKER_NBR,Sink,Tasks,[]);
				_ -> cm_balancer(?CIGAR_MAKER_NBR,Sink,[{Read_name,Chromo,Read,Ref_seeds}|Tasks],[])
			end
	end;

cm_balancer(?CIGAR_MAKER_NBR,Sink,[{Read_name,Chromo,Read,[{Pos,Ref}|Ref_seeds]}|Tasks],[Pid|CMs]) ->
	Pid ! {Ref,Read,Read_name,Chromo,Pos},
	case Ref_seeds of
		[] -> cm_balancer(?CIGAR_MAKER_NBR,Sink,Tasks,CMs);
		_ -> cm_balancer(?CIGAR_MAKER_NBR,Sink,[{Read_name,Chromo,Read,Ref_seeds}|Tasks],CMs)
	end.




terminate_cm(0) -> ok;
terminate_cm(N) ->
	receive
		{Pid,ready} -> Pid ! quit, terminate_cm(N-1)
	end.


