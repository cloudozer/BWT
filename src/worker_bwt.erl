% An implementation of worker that finds seeds using
% BWT (burrows-wheeler transform) 
%
% Cloudozer(c), 2015
%



-module(worker_bwt).
-behaviour(gen_server).

-export([start_link/0, start_link/1]).
-export([init/1, terminate/2, handle_info/2, handle_cast/2]).
-export([workload_supplier_loop/4]).

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
  SuppilerPid = spawn_link(?MODULE, workload_supplier_loop, [MasterPid, Waterline, [], false]),
  {ok, Workload} = get_workload(SuppilerPid),
  FMs1 = run_workload(Workload, FMs),
  {noreply, S#state{busy = true, master = MasterPid, fms = FMs1, supplier = SuppilerPid}};

%% a cast from the slave
handle_cast({results, Results}, S = #state{busy = true, master=MasterPid, fms = FMs, supplier = SuppilerPid}) ->
  %% retranslate the results to the master
  gen_server:cast(MasterPid, {results, Results, self()}),
  %% start next workload
  case get_workload(SuppilerPid) of
    {ok, Workload} ->
      FMs1 = run_workload(Workload, FMs),
      {noreply, S#state{fms = FMs1}};
    eof ->
      {noreply, #state{}}
  end.

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

spawn_slave({Meta,FM}, QseqList) ->
  WorkerPid = self(),
  {Pc,Pg,Pt} = proplists:get_value(pointers, Meta),
  
  SlavePid = spawn_link(fun() ->
    Results = lists:foldl(
      fun(Qseq,Acc) ->
        case sga:sga(FM,Pc,Pg,Pt,Qseq) of
          [] -> Acc;
          R -> [R|Acc]
        end
      end, [], QseqList),
    lager:info("Worker ~p done ~p sga:sga", [self(), length(QseqList)]),
    gen_server:cast(WorkerPid, {results, Results})
  end),
  erlang:monitor(process, SlavePid).

%% TODO several at time
workload_supplier_loop(MasterPid, Waterline, WorkloadList, false) when Waterline > length(WorkloadList) ->
  case gen_server:call(MasterPid, get_workload) of
    {ok, Workload} ->
      workload_supplier_loop(MasterPid, Waterline, [Workload | WorkloadList], false);
    eof ->
      workload_supplier_loop(MasterPid, Waterline, WorkloadList, eof)
  end;
workload_supplier_loop(MasterPid, Waterline, [Workload | WorkloadList], EOF) ->
  receive
    {get_workload, Pid} ->
      Pid ! {ok, Workload},
      workload_supplier_loop(MasterPid, Waterline, WorkloadList, EOF)
  end;
workload_supplier_loop(MasterPid, Waterline, [], eof) ->
  receive
    {get_workload, Pid} ->
      Pid ! eof,
      workload_supplier_loop(MasterPid, Waterline, [], eof)
  end.

get_workload(SupplierPid) ->
  SupplierPid ! {get_workload, self()},
  receive
    {ok, W} -> {ok, W};
    eof -> eof
  end.
