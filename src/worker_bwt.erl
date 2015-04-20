% An implementation of worker that finds seeds using
% BWT (burrows-wheeler transform) 
%
% Cloudozer(c), 2015
%



-module(worker_bwt).
-behaviour(gen_fsm).
%% State names: idle, seeding, aligning

-export([start_link/0, start_link/1, run_seeding/2]).
-export([init/1, terminate/3, handle_info/3, idle/2]).
-export([workload_buffer_loop/4, seeder_loop/5]).

-include("bwt.hrl").

%% api

start_link() ->
  gen_fsm:start_link(?MODULE, {}, []).

start_link(MasterPid) ->
  {ok, Pid} = gen_fms:start_link(?MODULE, {}, []),
  ok = master:register_workers(MasterPid, [Pid]),
  {ok, Pid}.

run_seeding(Pid, MasterPid) ->
  gen_fsm:send_event(Pid, {run_seeding, MasterPid}).

%% callbacks

-record(state, {master, slave}).

init(_) ->
  {ok, idle, #state{}}.

terminate(Reason, State, State) ->
  lager:error("A worker is terminated: ~p ~p~n~p", [State, Reason, State]).

handle_info({'DOWN', _Ref, process, SlavePid, normal}, _StateName, #state{slave = SlavePid}) ->
  {next_state, init, #state{}}.

idle({run_seeding, MasterPid}, S=#state{slave = undefined}) ->
  BufferWaterline = 5,
  WorkloadBufPid = spawn_link(?MODULE, workload_buffer_loop, [MasterPid, BufferWaterline, [], false]),
  {ok, Workload} = get_workload(WorkloadBufPid),
  SlavePid = spawn_link(?MODULE, seeder_loop, [MasterPid, self(), WorkloadBufPid, Workload, []]),
  erlang:monitor(process, SlavePid),
  {next_state, seeding, S#state{master = MasterPid, slave = SlavePid}}.

%% private

seeder_loop(MasterPid, WorkerPid, WorkloadBufPid, Workload, FMs) ->
  {{fmindex, {chromosome, ChromoName}}, {fastq, QseqList}} = Workload,
  {{Meta, FM}, FMs1} =
    case proplists:get_value(ChromoName, FMs) of
      undefined ->
        MetaFM = bwt:get_index(ChromoName),
        {MetaFM, [{ChromoName, MetaFM}]};
      MetaFM ->
        {MetaFM, FMs}
    end,

  {Pc,Pg,Pt} = proplists:get_value(pointers, Meta),

  Results = lists:foldl(
    fun(Qseq,Acc) ->
      case sga:sga(FM,Pc,Pg,Pt,Qseq) of
        [] -> Acc;
        R -> [R|Acc]
      end
    end, [], QseqList),
  lager:info("Worker ~p done ~p sga:sga", [self(), length(QseqList)]),
  gen_server:cast(MasterPid, {results, Results, WorkerPid}),

  case get_workload(WorkloadBufPid) of
    {ok, Workload1} ->
      seeder_loop(MasterPid, WorkerPid, WorkloadBufPid, Workload1, FMs1);
    eof ->
      stop
  end.

workload_buffer_loop(MasterPid, Waterline, WorkloadList, false) when Waterline > length(WorkloadList) ->
  case gen_server:call(MasterPid, get_workload) of
    {ok, Workload} ->
      workload_buffer_loop(MasterPid, Waterline, [Workload | WorkloadList], false);
    eof ->
      workload_buffer_loop(MasterPid, Waterline, WorkloadList, eof)
  end;
workload_buffer_loop(MasterPid, Waterline, [Workload | WorkloadList], EOF) ->
  receive
    {get_workload, Pid} ->
      Pid ! {ok, Workload},
      workload_buffer_loop(MasterPid, Waterline, WorkloadList, EOF)
  end;
workload_buffer_loop(_MasterPid, _Waterline, [], eof) ->
  receive
    {get_workload, Pid} ->
      Pid ! eof,
      stop
  end.

get_workload(SupplierPid) ->
  SupplierPid ! {get_workload, self()},
  receive
    {ok, W} -> {ok, W};
    eof -> eof
  end.
