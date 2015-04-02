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

-record(state, {busy=false, master, slave, fm, workload}).

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
  SlaveRef = spawn_slave(FM, QseqList),
  {reply, ok, S#state{busy=true, master=MasterPid, slave=SlaveRef}}.

handle_cast({results, _Results} = Msg, S = #state{busy=true, master=MasterPid, workload = Workload, fm=FM}) ->
  ok = gen_server:call(MasterPid, Msg, infinity),
  S1 = case Workload of
    undefined ->
      S#state{busy=false};
    {fastq, QseqList} ->
      SlaveRef = spawn_slave(FM, QseqList),
      S#state{busy=true, slave=SlaveRef}
  end,
  {noreply, S1}.

%% private

spawn_slave(FM, QseqList) ->
  WorkerPid = self(),
  SlavePid = spawn_link(fun() ->
    Results = lists:foldl(fun(Qseq,Acc) -> case sga:sga(FM,Qseq) of [] -> Acc; R -> [R|Acc] end end, [], QseqList),
    lager:info("Worker ~p done ~p sga:sga", [self(), length(QseqList)]),
    gen_server:cast(WorkerPid, {results, Results})
  end),
  erlang:monitor(process, SlavePid).