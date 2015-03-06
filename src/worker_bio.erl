-module(worker_bio).
-behaviour(gen_fsm).
%% API
-export([start_link/0, run/2,
seq_file_reader/1, seq_file_reader_loop/1, worker_loop/6]).
%% Callbacks
-export([init/1, terminate/3, handle_info/3, idle/2]).

-record(state, {current_worker, current_workload = [], workload, master_pid, current_seq, ref_file_abs, seq_reader, seq_limit, seq_counter}).

-include("bwt.hrl").

start_link() ->
  gen_fsm:start_link({local, ?MODULE}, ?MODULE, {}, []).

run(Pid, Args) ->
  gen_fsm:send_event(Pid, {run, Args}).

%% 

init(_Args) ->
  {ok, idle, #state{}}. 

terminate(Reason, StateName, StateData) ->
  lager:info("worker terminated ~p", [Reason]).

handle_info({'DOWN', Ref, process, CurrentPid, normal}, busy, S=#state{
  current_workload = [],
  seq_counter = SeqCounter,
  seq_limit = SeqLimit,
  master_pid = MasterPid,
  current_worker = CurrentPid,
  current_seq = {CurrentSeqName, _}
}) when SeqCounter == SeqLimit ->
  true = demonitor(Ref),
  master:send_done_seq(MasterPid, CurrentSeqName),
  master:send_done(MasterPid),
  {stop, normal, #state{}};

handle_info({'DOWN', Ref, process, CurrentPid, normal}, busy, S=#state{
    current_workload = [],
    current_worker = CurrentPid,
    current_seq = {CurrentSeqName, _},
    workload = [{Pos,ChunkSize}|WorkloadRest],
    master_pid = MasterPid,
    ref_file_abs = RefFileAbs,
    seq_reader = SeqsReaderPid,
    seq_counter = SeqCounter
  }) ->

  true = demonitor(Ref),

  master:send_done_seq(MasterPid, CurrentSeqName),

  {ok, Seq} = get_next_seq(SeqsReaderPid),

  {Pid, _} = spawn_monitor(?MODULE, worker_loop, [self(), MasterPid, Seq, RefFileAbs, Pos, ChunkSize]),
  {next_state, busy, S#state{current_worker = Pid, current_workload = WorkloadRest, current_seq = Seq, seq_counter = SeqCounter + 1}};

handle_info({'DOWN', Ref, process, CurrentPid, normal}, busy, S=#state{
    current_workload = [{Pos,ChunkSize}|WorkloadRest],
    current_worker = CurrentPid,
    master_pid = MasterPid,
    current_seq = Seq,
    ref_file_abs = RefFileAbs
  }) ->

  true = demonitor(Ref),

  {Pid,_} = spawn_monitor(?MODULE, worker_loop, [self(), MasterPid, Seq, RefFileAbs, Pos, ChunkSize]),
  {next_state, busy, S#state{current_worker = Pid, current_workload = WorkloadRest}}.

idle({run, Args}, State = #state{}) ->
  {
    RefFile,IndexFile,SeqFile,WorkerPath,
    Workload,
    MasterPid,
    SeqLimit
  } = Args,

  true = link(MasterPid),

  SeqsReaderPid = seq_file_reader(filename:absname_join(WorkerPath, SeqFile)),
  {ok, Seq} = get_next_seq(SeqsReaderPid),

  [{Pos,ChunkSize}|WorkloadRest] = Workload,

  RefFileAbs = filename:absname_join(WorkerPath, RefFile),
  {Pid, _} = spawn_monitor(?MODULE, worker_loop, [self(), MasterPid, Seq, RefFileAbs, Pos, ChunkSize]),
  {next_state, busy, State#state{
    workload = Workload,
    current_workload = WorkloadRest,
    current_worker = Pid, 
    master_pid = MasterPid,
    current_seq = Seq,
    ref_file_abs = RefFileAbs,
    seq_reader = SeqsReaderPid,
    seq_limit = SeqLimit,
    seq_counter = 1
  }}.


%% Private

seq_file_reader(SeqFile) ->
  {ok, Dev} = file:open(SeqFile, [read]),
skip_seqs(Dev, 50),
  spawn_link(?MODULE, seq_file_reader_loop, [Dev]).

seq_file_reader_loop(Dev) ->
  receive
    {get_seq, Ref, Pid} ->
      case seq_file_reader_loop_loop(Dev) of
        {ok, Seq} ->
          Pid ! {ok, Ref, Seq},
          seq_file_reader_loop(Dev);
        eof ->
          Pid ! eof
      end;
    _ ->
      exit(kill)
  end.

seq_file_reader_loop_loop(Dev) ->
  case file:read_line(Dev) of
    {ok, [$@ | SeqName]} ->
      {ok, SeqData} = file:read_line(Dev),
      Seq = {lists:droplast(SeqName),
             lists:droplast(SeqData)},
      {ok, Seq};
    {ok, E} ->
      seq_file_reader_loop_loop(Dev);
    eof ->
      eof
  end.

skip_seqs(_, 0) -> ok;
skip_seqs(Dev, N) ->
  case file:read_line(Dev) of
    {ok, [$@ | _]} ->
      {ok, _} = file:read_line(Dev),
      skip_seqs(Dev, N-1);
    {ok, _} ->
      skip_seqs(Dev, N);
    eof ->
      eof
  end.

get_next_seq(Pid) ->
  Ref = monitor(process, Pid),
  Pid ! {get_seq, Ref, self()},
  receive
    {ok, Ref, Seq = {_,SeqData}} ->
      true = demonitor(Ref),
      seeds:generate_fs(SeqData,15,2),
      compile:file("fs.erl",[report_errors]),
      code:add_path("."),
      case code:is_loaded(fs) of
        {file, _} -> 
          code:purge(fs);
        false -> ok
      end,
      {module, fs} = code:load_file(fs),
lager:info("Next Seq = ~p", [Seq]),
      {ok, Seq};
    _ ->
      exit(kill)
  end.

worker_loop(WorkerMngrPid, MasterPid, Seq = {SeqName, SeqData}, RefFile, Pos, ChunkSize) ->
  Ref_seq = msw:get_chunk(RefFile, Pos, ChunkSize),
  Seeds = fs:find_seeds(Ref_seq), 
  Matches = lists:foldl(fun(S,Acc)->
      case sw:sw(SeqData,lists:sublist(Ref_seq,S,length(SeqData)+?THRESHOLD)) of
        no_match -> Acc;
        {Match, Quality} ->
          [{Quality,Pos+S*61 div 60,Match}|Acc]
      end
    end,[],Seeds),
  if (Matches =/= []) -> 
    master:send_result(MasterPid, {Seq, Matches});
  true -> ok end.
