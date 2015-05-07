-module(master).
-behaviour(gen_server).

-export([start_link/1, start_link/0, register_workers/2, run/4]).
-export([test/0, test/4]).
-export([init/1, terminate/2, handle_info/2, handle_call/3, handle_cast/2]).

%% test

test() ->
  test("bwt_files/21_not_found.fastq", "21", 1, false).

test(SeqFileName, Chromosome, WorkersNum, Debug) ->
  lager:start(),
  if Debug == true ->
    lager:set_loglevel(lager_console_backend, debug);
  true ->
    lager:set_loglevel(lager_console_backend, error)
  end,

  ok = application:start(bwt),

  %% Create a master process
  {ok, MPid} = ?MODULE:start_link([]),
  %% Create worker processes
  Pids = lists:map(fun(_) -> {ok, WPid} = worker_bwt:start_link(), WPid end, lists:seq(1, WorkersNum)),
  %% Associate them with the master
  ok = master:register_workers(MPid, Pids),
  %% Tell the master to run

  ok = ?MODULE:run(MPid, SeqFileName, Chromosome, WorkersNum).

%% api

start_link() ->
  gen_server:start_link({local, master}, ?MODULE, {}, []).

start_link(Args) ->
  gen_server:start_link({local, master}, ?MODULE, Args, []).

register_workers(MasterPid, Pids) ->
  gen_server:call(MasterPid, {register_workers, Pids}).

run(Pid, SeqFileName, Chromosome, WorkersLimit) ->
  gen_server:call(Pid, {run, SeqFileName, Chromosome, WorkersLimit}, infinity).

%% gen_server callbacks

-record(state, {workers=[], fastq, fastq_eof = false, chromosome, workload_size = 1000, client, stopping = false, start_time}).

init(_Args) ->
  lager:info("Started master"),
  {ok, #state{}}.

terminate(Reason, State) ->
  lager:info("Master terminated: ~p", [{Reason, State}]).

handle_info({'DOWN',Ref,process,Pid,normal}, S=#state{workers = [{Pid,Ref}], start_time = StartTime}) ->
  Microsec = timer:now_diff(now(), StartTime),
  io:format("It's all over. ~.1f sec.~n", [Microsec / 1000000]),
  {stop, normal, S};
handle_info({'DOWN',Ref,process,Pid,normal}, S) ->
lager:info("Worker down: ~p",[node(Pid)]),
  {noreply, S#state{workers = lists:delete({Pid,Ref}, S#state.workers)}}.


handle_call({register_workers, _}, _, S=#state{start_time=T}) when T =/= undefined ->
	{reply, ok, S};
handle_call({register_workers, Pids}, _From, S=#state{workers=Workers}) ->
  %% link and monitor new workers
  NewWorkers = lists:map(fun(Pid)->
                  true = link(Pid),
                  Ref = monitor(process, Pid),
                  {Pid, Ref}
  end, Pids),
  S1 = S#state{workers=NewWorkers++Workers},
  lager:info("The master got ~b workers", [length(S1#state.workers)]),
  {reply, ok, S1};

handle_call({run, FastqFileName, Chromosome, WorkersLimit}, {ClientPid,_}, S=#state{workers=Workers}) when length(Workers) > 0 ->
  {ok, FastqDev} = file:open(FastqFileName, [read, raw, read_ahead]),
  {Workers1, Workers2} = lists:split(WorkersLimit, Workers),
  lists:foreach(fun({Pid,_}) -> worker_bwt:run(Pid, self()) end, Workers1),
  lists:foreach(fun({Pid,Ref}) -> true = unlink(Pid), true = demonitor(Ref) end, Workers2),
  {reply, ok, S#state{fastq={FastqFileName, FastqDev}, chromosome = Chromosome, workers = Workers1, client = ClientPid, start_time = now()}};

handle_call({get_workload, N}, _From, S=#state{stopping = false}) ->
  {Result,S1} = produce_workload(N, S),
  {reply, Result, S1};
handle_call({get_workload, _N}, _From, S=#state{stopping = true}) ->
  {reply, stop, S}.

produce_workload(N, State) ->
  produce_workload(N, State, []).

produce_workload(0, State, Acc) ->
  {Acc, State};

produce_workload(N, S = #state{fastq = {_, FqDev}, fastq_eof = false, chromosome = Chromosome, workload_size = WorkloadSize}, Acc) ->
  case fastq:read_seqs(FqDev, WorkloadSize) of
    {_, SeqList} ->
      Workload = {Chromosome, SeqList},
      produce_workload(N-1, S, [Workload | Acc]);
    eof ->
      {Acc, S#state{fastq_eof = true}}
  end;

produce_workload(_N, S = #state{fastq_eof = true}, []) ->
  {stop, S#state{stopping = true}}.

handle_cast({get_workload, N, Pid}, State) ->
  Self = self(),
  spawn_link(fun() ->
    Resp = gen_server:call(Self, {get_workload, N}, 60000),
    gen_server:cast(Pid, {workload, Resp})
  end),
  {noreply, State};

handle_cast({cigar, _, {CigarRate, _}, _, _}, State) when CigarRate < 270 ->
  {noreply, State};
handle_cast({cigar, SeqName, Cigar = {CigarRate, CigarValue}, Pos, RefSeq}, State = #state{chromosome = Chromosome, client = ClientPid}) ->
  lager:info("Master got cigar: ~p ~p", [SeqName, Cigar]),
  io:format("~s      ~s      ~b      ~s      ~b      ~s~n", [SeqName, Chromosome, Pos, CigarValue, CigarRate, RefSeq]),
  ClientPid ! {cigar, SeqName, Chromosome, Pos, CigarValue, CigarRate, RefSeq},
  {noreply, State}.
