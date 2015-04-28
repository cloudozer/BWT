-module(master).
-behaviour(gen_server).

-export([start_link/1, start_link/0, register_workers/2, run/4]).
-export([test/0, test/2]).
-export([init/1, terminate/2, handle_info/2, handle_call/3, handle_cast/2]).

%% test

test() ->
%%   test("bwt_files/SRR770176_1.fastq", "GL000192.1").
  test("bwt_files/SRR770176_1.fastq", "GL000193.1").

test(SeqFileName, Chromosome) ->
  lager:start(),
%  lager:set_loglevel(lager_console_backend, error),
  ok = application:start(bwt),

  %% Create a master process
  {ok, MPid} = ?MODULE:start_link([]),
  %% Create worker processes
  WorkersNum = 6,
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

-record(state, {workers=[], fastq, chromosome, seeds = [], seed_workload_pkg_size = 1000, result_size=0, client}).

init(_Args) ->
  lager:info("Started master"),
  {ok, #state{}}.

terminate(Reason, State) ->
  lager:info("Master terminated: ~p", [{Reason, State}]).

%% A worker is finished
handle_info({'DOWN',_Ref,process,_Pid,normal}, State) ->
  {noreply, State}.


handle_call({register_workers, Pids}, _From, S=#state{workers=Workers}) ->
  %% monitor new workers
  lists:foreach(fun(Pid)->true = link(Pid) end, Pids),
  S1 = S#state{workers=Pids++Workers},
  lager:info("The master got ~b workers", [length(S1#state.workers)]),
  {reply, ok, S1};

handle_call({run, FastqFileName, Chromosome, WorkersLimit}, {ClientPid,_}, S=#state{workers=Workers}) when length(Workers) > 0 ->
  {ok, FastqDev} = file:open(FastqFileName, [read, raw, read_ahead]),
  {Workers1, _Workers2} = lists:split(WorkersLimit, Workers),
  lists:foreach(fun(Pid) -> worker_bwt:run(Pid, self()) end, Workers1),
  %% TODO: demonitor the rest
  {reply, ok, S#state{fastq={FastqFileName, FastqDev}, chromosome = Chromosome, workers = Workers1, client = ClientPid}};

handle_call(get_workload, _From, S = #state{fastq = {_, done}, seeds = []}) ->
  {reply, stop, S};

handle_call(get_workload, _From, S = #state{fastq = {FqFileName, FqDev}, chromosome = Chromosome, seeds = Seeds, workers = Workers, seed_workload_pkg_size = SeedWorkPkgSize})
  when length(Workers) > length(Seeds) ->
    case fastq:read_seq(FqDev, SeedWorkPkgSize) of
      {_, SeqList} ->
        Workload = {seed, Chromosome, SeqList},
        {reply, {ok, Workload}, S};
      eof ->
        {reply, undefined, S#state{fastq = {FqFileName,done}}}
%        {reply, undefined, S}
    end;

handle_call(get_workload, _From, S = #state{chromosome = Chromosome, seeds = Seeds, workers = Workers, fastq={FileName, _}}) ->
  Seeds1 = lists:map(fun({SeqName, SeqPos, L}) ->
    {ok, Dev} = file:open(FileName, [read, raw, read_ahead]), 
    {ok, SeqPos} = file:position(Dev, SeqPos),
    {ok, {SeqName, SeqData}} = fastq:read_seq(Dev),
    ok = file:close(Dev),
    {{SeqName, SeqData}, L}
  end, Seeds),
  %lager:info("master sent sw workload: ~p", [Seeds1]),
  Workload = {sw, Chromosome, Seeds1},
  {reply, {ok, Workload}, S#state{seeds = []}}.


handle_cast(schedule, State) ->
  %% TODO: do something
  {noreply, State};

handle_cast({seeds, Results}, S=#state{seeds = SeedsList, result_size = ResSize}) ->
  lager:info("Total 'results': ~p", [ResSize + length(Results)]),
  gen_server:cast(self(), schedule),
  {noreply, S#state{seeds = Results ++ SeedsList, result_size = ResSize + length(Results)}};

handle_cast({cigar, _, {CigarRate, _}, _}, State) when CigarRate < 260 ->
  {noreply, State};
handle_cast({cigar, {SeqName, SeqValue}, Cigar = {CigarRate, CigarValue}, Pos}, State = #state{chromosome = Chromosome, client = ClientPid}) ->
  lager:info("Master got cigar: ~p ~p", [SeqName, Cigar]),
  io:format("~s      ~s      ~b      ~s      ~b      ~s~n", [SeqName, Chromosome, Pos, CigarValue, CigarRate, SeqValue]),
  ClientPid ! {cigar, SeqName, Chromosome, Pos, CigarValue, CigarRate, SeqValue},
  {noreply, State};

handle_cast({done, LastPid}, S=#state{workers = [LastPid]}) -> %, seeds = []}) ->
  lager:info("last worker done"),
  {stop, normal, S};
handle_cast({done, Pid}, S = #state{workers = Workers}) -> % , seeds = []}) ->
  lager:info("worker ~p done", [Pid]),
  {noreply, S#state{workers = lists:delete(Pid, Workers)}}.
