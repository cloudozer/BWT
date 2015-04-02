-module(worker_bwt).
-behaviour(gen_server).

-export([start_link/1, execute/3]).
-export([init/1, handle_info/2, handle_call/3, handle_cast/2]).

-include("bwt.hrl").

%% api
start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

execute(Pid, Workload, MasterPid) ->
  gen_server:call(Pid, {execute, Workload, MasterPid}, infinity).

%% callbacks

-record(state, {busy=false, master, slave, fm}).

init(ChromoName) ->
  FM = bwt:get_index(ChromoName),
  lager:info("Worker have read the FM-index"),
  {ok, #state{fm=FM}}.

%% It's ok
handle_info({'DOWN',Ref,process,_Pid,normal}, S=#state{slave=Ref}) ->
  {noreply, S}.

handle_call({execute, Workload, MasterPid}, _From, S=#state{busy=false, fm=FM}) ->
  %{{fmindex, {chromosome, ChromoName}}, {fastq, QseqList}} = Workload,
  {fastq, QseqList} = Workload,
  lager:info("Worker ~p got workload ~p", [self(), length(QseqList)]),
  WorkerPid = self(),
  SlavePid = spawn_link(fun() ->
    Results = lists:foldl(fun(Qseq,Acc) -> case sga:sga(FM,Qseq) of [] -> Acc; R -> [R|Acc] end end, [], QseqList),
    lager:info("Worker ~p done ~p sga:sga", [self(), length(QseqList)]),
    gen_server:cast(WorkerPid, {results, Results})
  end),
  SlaveRef = erlang:monitor(process, SlavePid),
  {reply, ok, S#state{busy=true, master=MasterPid, slave=SlaveRef}}.

handle_cast({results, Results}=Msg, S=#state{busy=true, master=MasterPid}) ->
  ok = gen_server:call(MasterPid, Msg, infinity),
  {noreply, S#state{busy=false}}.
