-module(worker_bwt).
-behaviour(gen_server).

-export([start_link/0, start_link/1]).
-export([init/1, terminate/2, handle_info/2, handle_cast/2]).
-export([workload_supplier_loop/3]).

-include("bwt.hrl").

%% api
start_link() ->
  gen_server:start_link(?MODULE, {}, []).

start_link(MasterPid) ->
  {ok, Pid} = gen_server:start_link(?MODULE, {}, []),
  ok = master:register_workers(MasterPid, [Pid]),
  {ok, Pid}.

%% callbacks

-record(state, {busy=false, master, fms=[], supplier, waterline=5}).

init(_) ->
  {ok, #state{}}.

terminate(Reason, State) ->
  lager:error("A worker is terminated: ~p~n~p", [Reason, State]).

%% The slave is finished
handle_info({'DOWN',_Ref,process,_Pid,normal}, S=#state{}) ->
  {noreply, S}.

handle_cast({run, MasterPid}, S=#state{busy=false, fms=FMs, supplier=undefined, waterline=Waterline}) ->
  SuppilerPid = spawn_link(?MODULE, workload_supplier_loop, [MasterPid, Waterline, []]),
  {ok, Workload} = get_workload(SuppilerPid),
  FMs1 = run_workload(Workload, FMs),
  {noreply, S#state{busy = true, master = MasterPid, fms = FMs1, supplier = SuppilerPid}};

%% a cast from the slave
handle_cast({results, Results}, S = #state{busy = true, master=MasterPid, fms = FMs, supplier = SuppilerPid}) ->
  %% retranslate the results to the master
  gen_server:cast(MasterPid, {results, Results, self()}),
  %% start next workload
  {ok, Workload} = get_workload(SuppilerPid),
  FMs1 = run_workload(Workload, FMs),
  {noreply, S#state{fms = FMs1}}.

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
%% TODO several at time
workload_supplier_loop(MasterPid, Waterline, WorkloadList) when Waterline > length(WorkloadList) ->
  {ok, Workload} = gen_server:call(MasterPid, get_workload),
  workload_supplier_loop(MasterPid, Waterline, [Workload | WorkloadList]);
workload_supplier_loop(MasterPid, Waterline, [Workload | WorkloadList]) ->
  receive
    {get_workload, Pid} ->
      Pid ! {ok, Workload},
      workload_supplier_loop(MasterPid, Waterline, WorkloadList)
  end.

get_workload(SupplierPid) ->
  SupplierPid ! {get_workload, self()},
  receive {ok, W} -> {ok, W} end.
