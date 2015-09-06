% aligning queue module 
% 
% 
% Cloudozer(c), 2015
%

-module(alq).
-export([
	start/5,
	alq/5
]).

-define(CIGAR_MAKER_NBR,6).

-include("bwt.hrl").


start(SinkRef,SinkHost,Host,BoxName,LingdRef) ->
	Pid = spawn(?MODULE,alq,[SinkRef,SinkHost,Host,BoxName,LingdRef]),
	Ref = {navel:get_node(), Pid},
	{ok, Ref}.



alq(Sink,SinkHost,Host,BoxName,Lingd) ->
  	%% spawn N cigar_makers
	cm:start_cigar_makers(?CIGAR_MAKER_NBR,Sink,SinkHost,Host,BoxName,Lingd),
	cm_balancer(?CIGAR_MAKER_NBR,Sink,[],[]).


cm_balancer(?CIGAR_MAKER_NBR,Sink,[],[]) ->
	receive
		quit -> terminate_cm(?CIGAR_MAKER_NBR);
		{_Read_name,_Chromo,_Read,_Ref_seeds}=Task -> cm_balancer(?CIGAR_MAKER_NBR,Sink,[Task],[]);
		{Pid,ready} -> cm_balancer(?CIGAR_MAKER_NBR,Sink,[],[Pid])
	after 10000 ->
		throw({timeout, cm_balancer, empty_stacks})
	end;

cm_balancer(?CIGAR_MAKER_NBR,Sink,[],[Pid={CmN,CmP}|CMs]) ->
	receive
		quit -> terminate_cm(?CIGAR_MAKER_NBR);

		{Pid1,ready} -> cm_balancer(?CIGAR_MAKER_NBR,Sink,[],[Pid1,Pid|CMs]);

		fastq_done when length([Pid|CMs]) =:= ?CIGAR_MAKER_NBR ->
			{SN,SP} = Sink,
			navel:call_no_return(SN,erlang,send,[SP,fastq_done]),
			io:format("Alq confirmed that fastq_done~n");

		NewTasks ->
			cm_balancer(?CIGAR_MAKER_NBR,Sink,NewTasks,[{CmN,CmP}|CMs])

	end;

cm_balancer(?CIGAR_MAKER_NBR,Sink,[{_Read_name,_Chromo,_Read,[]}|Tasks],CMs) ->
	cm_balancer(?CIGAR_MAKER_NBR,Sink,Tasks,CMs);

cm_balancer(?CIGAR_MAKER_NBR,Sink,[{Read_name,Chromo,Read,[{Pos,Ref}|Ref_seeds]}|Tasks]=OldTasks,[]) ->
	receive
		quit -> terminate_cm(?CIGAR_MAKER_NBR);

		{{CmN,CmP},ready} ->
			navel:call_no_return(CmN,erlang,send,[CmP,{Ref,Read,Read_name,Chromo,Pos}]),
			case Ref_seeds of
				[] -> cm_balancer(?CIGAR_MAKER_NBR,Sink,Tasks,[]);
				_ -> cm_balancer(?CIGAR_MAKER_NBR,Sink,[{Read_name,Chromo,Read,Ref_seeds}|Tasks],[])
			end;

		NewTasks -> cm_balancer(?CIGAR_MAKER_NBR,Sink,OldTasks++NewTasks,[])

	after 10000 ->
		throw({timeout, cm_balancer, tasks_orevflow})
	end;

cm_balancer(?CIGAR_MAKER_NBR,Sink,[{Read_name,Chromo,Read,[{Pos,Ref}|Ref_seeds]}|Tasks],[{CmN,CmP}|CMs]) ->
	navel:call_no_return(CmN,erlang,send,[CmP,{Ref,Read,Read_name,Chromo,Pos}]),
	case Ref_seeds of
		[] -> cm_balancer(?CIGAR_MAKER_NBR,Sink,Tasks,CMs);
		_ -> cm_balancer(?CIGAR_MAKER_NBR,Sink,[{Read_name,Chromo,Read,Ref_seeds}|Tasks],CMs)
	end.




terminate_cm(0) -> ok;
terminate_cm(N) ->
	receive
		{Pid,ready} -> Pid ! quit, terminate_cm(N-1)
	end.


