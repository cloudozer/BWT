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

-define(CIGAR_MAKER_NBR,8).
-define(TASK_BATCH_SIZE,5000).

-include("bwt.hrl").


start(SinkRef,SinkHost,Host,BoxName,LingdRef) ->
	Pid = spawn(?MODULE,alq,[SinkRef,SinkHost,Host,BoxName,LingdRef]),
	Ref = {navel:get_node(), Pid},
	{ok, Ref}.



alq(Sink,SinkHost,Host,BoxName,Lingd) ->
  	%% spawn N cigar_makers
	cm:start_cigar_makers(?CIGAR_MAKER_NBR,Sink,SinkHost,Host,BoxName,Lingd),
	cm_balancer(?CIGAR_MAKER_NBR,Sink,[],[]).


cm_balancer(?CIGAR_MAKER_NBR,Sink,[],CMs) ->
	receive
		quit -> terminate_cm(?CIGAR_MAKER_NBR);

		{Pid,ready} -> cm_balancer(?CIGAR_MAKER_NBR,Sink,[],[Pid|CMs]);
		
		{SFPid, NewTasks} ->
			cm_balancer(?CIGAR_MAKER_NBR,Sink,NewTasks,CMs);

		fastq_done when length(CMs) =:= ?CIGAR_MAKER_NBR ->
			{SN,SP} = Sink,
			navel:call_no_return(SN,erlang,send,[SP,fastq_done]),
			io:format("Alq confirmed that fastq_done~n")
	end;

cm_balancer(?CIGAR_MAKER_NBR,Sink,OldTasks,[]) ->
	receive
		quit -> terminate_cm(?CIGAR_MAKER_NBR);
		
		{Pid,ready} ->
			cm_balancer(?CIGAR_MAKER_NBR,Sink,OldTasks,[Pid]);
			
		{{SFNode,SFPid}=SF, NewTasks} -> 
			if length(OldTasks) > 20000 ->
				io:format("Alq sends wait to ~p~n", [SF]),
				navel:call_no_return(SFNode, erlang, send, [SFPid, wait]);
			true -> ok
			end,
			io:format("Queue ~p: ~p~n", [SFNode, length(OldTasks)]), 
			cm_balancer(?CIGAR_MAKER_NBR,Sink,NewTasks++OldTasks,[])

	after 10000 ->
		throw({timeout, cm_balancer, tasks_orevflow})
	end;

cm_balancer(?CIGAR_MAKER_NBR,Sink,Tasks,[{CmN,CmP}|CMs]) when length(Tasks) < ?TASK_BATCH_SIZE ->
	navel:call_no_return(CmN,erlang,send,[CmP,Tasks]),
	cm_balancer(?CIGAR_MAKER_NBR,Sink,[],CMs);

cm_balancer(?CIGAR_MAKER_NBR,Sink,Tasks,[{CmN,CmP}|CMs]) ->
	{TaskBatch,RestTasks} = lists:split(?TASK_BATCH_SIZE, Tasks),
	navel:call_no_return(CmN,erlang,send,[CmP,TaskBatch]),
	cm_balancer(?CIGAR_MAKER_NBR,Sink,RestTasks,CMs).


terminate_cm(0) -> ok;
terminate_cm(N) ->
	receive
		{{CmN,CmP},ready} -> navel:call_no_return(CmN,erlang,send,[CmP,quit]) , terminate_cm(N-1)
	end.


