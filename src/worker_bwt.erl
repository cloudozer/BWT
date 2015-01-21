-module(worker_bwt).
-behaviour(gen_fsm).
%% API
-export([start_link/0, run/2,
seq_file_reader/1, seq_file_reader_loop/1, worker_loop/0]).
%% Callbacks
-export([init/1, idle/3, busy/3]).

-record(state, {current_worker, tasks_queue, master_pid, seq, ref_file_abs}).

start_link() ->
  gen_fsm:start_link({local, ?MODULE}, ?MODULE, {}, []).

run(Pid, Args) ->
  gen_fsm:sync_send_event(Pid, {run, Args}).


init(_Args) ->
  {ok, idle, #state{}}. 

%handle_info({'DOWN', Ref, process, Pid, normal}, busy, S=#state{current_worker={Pid,Ref}}) ->
%lager:info("exi"),
%  {next_state, idle, S#state{current_worker = undefined}}.

idle({run, Args}, _From, State = #state{}) ->
  {
    RefFile,IndexFile,SeqFile,WorkerPath,
    Workload,
    MasterPid
  } = Args,
  SeqsReaderPid = seq_file_reader(filename:absname_join(WorkerPath, SeqFile)),
  {ok, Seq} = {ok, {test, "ATGTGACACAGATCACTGCGGCCTTGACCTCCCAGGCTCCAGGTGGTTCTT"}}, % get_next_seq(SeqsReaderPid),
lager:info("Seq = ~p", [Seq]),
  {SeqName, SeqData} = Seq,

  [{Pos,ChunkSize}|WorkloadRest] = Workload,

  Pid = spawn_link(?MODULE, worker_loop, []),
  RefFileAbs = filename:absname_join(WorkerPath, RefFile),
  Pid ! {run, self(), MasterPid, SeqData, RefFileAbs, Pos, ChunkSize},
  {reply, ok, busy, State#state{
    current_worker = Pid, 
    tasks_queue = queue:from_list(WorkloadRest), 
    master_pid = MasterPid,
    seq = Seq,
    ref_file_abs = RefFileAbs
  }}.

busy({done, WorkPiece}, {CurrentPid,_}, S) ->
  #state{
    current_worker=CurrentPid, 
    tasks_queue = Tasks,
    master_pid = MasterPid,
    seq = {SeqName, SeqData},
    ref_file_abs = RefFileAbs
  } = S,

  Tasks1 = Tasks, % queue:in(CurrentPid, Tasks),
  case queue:get(Tasks1) of
    {Pos, ChunkSize} -> 
      Pid = spawn_link(?MODULE, worker_loop, []),
      Pid ! {run, self(), MasterPid, SeqData, RefFileAbs, Pos, ChunkSize}
  end,
lager:info("~p in queue", [queue:len(Tasks1)]),
  Tasks2 = queue:drop(Tasks1),
  {reply, ok, busy, S#state{current_worker = Pid, tasks_queue = Tasks2}}; 

busy(stop, _From, #state{current_worker = Pid, tasks_queue = Tasks}) ->
  exit(Pid, kill),
  lists:foreach(fun(P) when is_pid(P) -> exit(P, kill); (_) -> ok end, queue:to_list(Tasks)),
  {reply, ok, idle, #state{}}.

%% Private

seq_file_reader(SeqFile) ->
  {ok, Dev} = file:open(SeqFile, [read]),
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
  [Dot] = "@",
  case file:read_line(Dev) of
    {ok, [Dot | SeqName]} ->
      {ok, SeqData} = file:read_line(Dev),
      Seq = {lists:droplast(SeqName),
             lists:droplast(SeqData)},
      {ok, Seq};
    {ok, E} ->
      seq_file_reader_loop_loop(Dev);
    eof ->
      eof
  end.

get_next_seq(Pid) ->
  Ref = monitor(process, Pid),
  Pid ! {get_seq, Ref, self()},
  receive
    {ok, Ref, Seq} ->
      true = demonitor(Ref),
      {ok, Seq};
    _ ->
      exit(kill)
  end.

worker_loop() -> 
  receive
    {run, WorkerMngrPid, MasterPid, Seq, RefFile, Pos, ChunkSize} ->
      msw:worker(self(), Seq, RefFile, Pos, ChunkSize),
      Results = receive R -> R end,
      master:send_result(MasterPid, Results),
      ok = gen_fsm:sync_send_event(WorkerMngrPid, {done, {Pos, ChunkSize}}),
      worker_loop()
  end.
