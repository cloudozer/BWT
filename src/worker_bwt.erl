-module(worker_bwt).
-behaviour(gen_server).

-export([start_link/0, start_link/1, execute/3]).
-export([init/1, terminate/2, handle_info/2, handle_call/3, handle_cast/2]).

-include("bwt.hrl").

%% api
start_link() ->
  gen_server:start_link(?MODULE, {}, []).

start_link(MasterPid) ->
  {ok, Pid} = gen_server:start_link(?MODULE, {}, []),
  ok = master:register_workers(MasterPid, [Pid]),
  {ok, Pid}.

execute(Pid, Workload, MasterPid) ->
  gen_server:call(Pid, {execute, Workload, MasterPid}, infinity).

%% callbacks

-record(state, {busy=false, master, workloads=[], fms=[]}).

init(_) ->
  {ok, #state{}}.

terminate(Reason, State) ->
  lager:error("A worker is terminated: ~p~n~p", [Reason, State]).

%% The slave is finished
handle_info({'DOWN',_Ref,process,_Pid,normal}, S=#state{}) ->
  {noreply, S}.

handle_call({execute, Workload, MasterPid}, _From, S=#state{busy=false, fms=FMs, workloads = Workloads}) ->
%%   lager:info("Worker got workload1"),
  FMs1 = run_workload(Workload, FMs),
%%   if (length(Workloads < 10)) ->
    gen_server:cast(MasterPid, {get_workload, self()}),
%%     true -> ok
%%   end,
  {reply, ok, S#state{busy = true, master = MasterPid, fms = FMs1}};
handle_call({execute, Workload, MasterPid}, _From, S=#state{busy = true, workloads = Workloads, master = MasterPid}) ->
%%   lager:info("Worker got workload2 ~p", [length([Workload|Workloads])]),
  {reply, ok, S#state{workloads = [Workload|Workloads]}}.

%% a cast from the slave
handle_cast({results, Results}, S = #state{busy = true, master=MasterPid, workloads = [Workload|Workloads], fms = FMs}) ->
  %% retranslate the results to the master
%%   lager:info("Worker results1"),
  gen_server:cast(MasterPid, {results, Results, self()}),
  %% start buffered workload
  FMs1 = run_workload(Workload, FMs),
  {noreply, S#state{busy = true, fms = FMs1, workloads = Workloads}};
handle_cast({results, Results}, S = #state{busy = true, master=MasterPid, workloads = [], fms = FMs}) ->
  %% retranslate the results to the master
%%   lager:info("Worker results2"),
  gen_server:cast(MasterPid, {results, Results, self()}),
  {noreply, S#state{busy=false}};
handle_cast(get_workload, S = #state{busy=true, master=MasterPid}) ->
  gen_server:cast(MasterPid, {get_workload, self()}),
  {noreply, S}.

%% private

run_workload(Workload, FMs) ->
  {{fmindex, {chromosome, ChromoName}}, {fastq, QseqList}} = Workload,
  {FM, FMs1} =
    case proplists:get_value(ChromoName, FMs) of
      undefined ->
        FM = bwt:get_index(ChromoName),
        {FM, [{ChromoName, FM}]};
      FM ->
        {FM, FMs}
    end,
  _SlaveRef = spawn_slave(FM, QseqList),
  FMs1.

spawn_slave(FM, QseqList) ->
  WorkerPid = self(),
  SlavePid = spawn_link(fun() ->
    Results = lists:foldl(
      fun(Qseq,Acc) ->
        case sga:sga(FM,Qseq) of
          [] -> Acc;
          R -> [R|Acc]
        end
      end, [], QseqList),
    lager:info("Worker ~p done ~p sga:sga", [self(), length(QseqList)]),
    gen_server:cast(WorkerPid, {results, Results})
  end),
  erlang:monitor(process, SlavePid).
