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
		{Pid,ready} -> cm_balancer(?CIGAR_MAKER_NBR,Sink,[],[Pid]);

		NewTasks=[_|_] -> cm_balancer(?CIGAR_MAKER_NBR,Sink,NewTasks,[]);

		quit -> terminate_cm(?CIGAR_MAKER_NBR)
		
	after 10000 ->
		throw({timeout, cm_balancer, empty_stacks})
	end;

cm_balancer(?CIGAR_MAKER_NBR,Sink,[],CMs) ->
	receive
		{Pid,ready} -> cm_balancer(?CIGAR_MAKER_NBR,Sink,[],[Pid|CMs]);
		
		NewTasks=[_|_] ->
			cm_balancer(?CIGAR_MAKER_NBR,Sink,NewTasks,CMs);

		fastq_done when length(CMs) =:= ?CIGAR_MAKER_NBR ->
			{SN,SP} = Sink,
			navel:call_no_return(SN,erlang,send,[SP,fastq_done]),
			io:format("Alq confirmed that fastq_done~n");

		quit -> terminate_cm(?CIGAR_MAKER_NBR)
	end;

%cm_balancer(?CIGAR_MAKER_NBR,Sink,[{_Read_name,_Chromo,_Read,[]}|Tasks],CMs) ->
%	cm_balancer(?CIGAR_MAKER_NBR,Sink,Tasks,CMs);

cm_balancer(?CIGAR_MAKER_NBR,Sink,[Task|Tasks]=OldTasks,[]) ->
	receive
		{{CmN,CmP},ready} ->
			navel:call_no_return(CmN,erlang,send,[CmP,Task]),
			cm_balancer(?CIGAR_MAKER_NBR,Sink,Tasks,[]);
			
		NewTasks=[_|_] -> cm_balancer(?CIGAR_MAKER_NBR,Sink,OldTasks++NewTasks,[]);

		quit -> terminate_cm(?CIGAR_MAKER_NBR)

	after 10000 ->
		throw({timeout, cm_balancer, tasks_orevflow})
	end;

cm_balancer(?CIGAR_MAKER_NBR,Sink,[Task|Tasks],[{CmN,CmP}|CMs]) ->
	navel:call_no_return(CmN,erlang,send,[CmP,Task]),
	cm_balancer(?CIGAR_MAKER_NBR,Sink,Tasks,CMs).


%cm_balancer(?CIGAR_MAKER_NBR,Sink,[{Read_name,Chromo,Read,[{Pos,Ref}|Ref_seeds]}|Tasks],[{CmN,CmP}|CMs]) ->
%	navel:call_no_return(CmN,erlang,send,[CmP,{Ref,Read,Read_name,Chromo,Pos}]),
%	case Ref_seeds of
%		[] -> cm_balancer(?CIGAR_MAKER_NBR,Sink,Tasks,CMs);
%		_ -> cm_balancer(?CIGAR_MAKER_NBR,Sink,[{Read_name,Chromo,Read,Ref_seeds}|Tasks],CMs)
%	end.




terminate_cm(0) -> ok;
terminate_cm(N) ->
	receive
		{Pid,ready} -> Pid ! quit, terminate_cm(N-1)
	end.


