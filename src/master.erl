-module(master).
-behaviour(gen_server).

-export([start_link/1, register_workers/2]).
-export([test/0]).
-export([init/1, terminate/2, handle_info/2, handle_call/3, handle_cast/2]).

%% api

start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

register_workers(MasterPid, Pids) ->
  gen_server:call(MasterPid, {register_workers, Pids}).

test() ->
  lager:start(),
  ok = application:start(bwt),

  %% Create a master process
  {ok, MPid} = ?MODULE:start_link([]),
  %% Create worker processes
  WorkersNum = 6,
  Pids = lists:map(fun(_) -> {ok, WPid} = worker_bwt:start_link(), WPid end, lists:seq(1, WorkersNum)),
  %% Associate them with the master
  ok = master:register_workers(MPid, Pids),
  %% Tell the master to run
  SeqFileName = "bwt_files/SRR770176_1.fastq",
  ok = gen_server:call(MPid, {run, SeqFileName}, infinity).

%% gen_server callbacks

-record(state, {running=false, workers=[], tasks=[], fastq, result_size=0}).

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
  lists:foreach(fun(Pid)->monitor(process, Pid) end, Pids),
  S1 = S#state{workers=Pids++Workers},
  lager:info("Worker got ~b workers", [length(S1#state.workers)]),
  %S2 = if S#state.running == true ->
  %  %% assing tasks to the workers
  %  schedule(S1);
  %  true -> S1
  %end,
  S2 = S1,
  {reply, ok, S2};
handle_call({run, FastqFileName}, _From, S=#state{running=false, workers=Workers}) when length(Workers) > 0 ->
  {ok, FastqDev} = file:open(FastqFileName, [read, raw, read_ahead]),
  S1 = schedule(S#state{fastq={FastqFileName, FastqDev}, running=true}),
  {reply, ok, S1};
handle_call(get_workload, _From, S = #state{fastq={_, FqDev}}) ->
  N = 1000,
  {ok, SeqList} = fastq:read_seq(FqDev, N),
  FmIndex = {fmindex, {chromosome, "GL000192.1"}},
  Queries = lists:map(fun({_,Q}) -> Q end, SeqList),
  Workload = {FmIndex, {fastq, Queries}},
  {reply, {ok, Workload}, S}.

handle_cast(schedule, State) ->
  {noreply, schedule(State)};
handle_cast({results, Results, Pid}, S=#state{running=true, result_size = ResSize}) ->
  lager:info("master got results ~p. total: ~p", [Results, ResSize + length(Results)]),
  gen_server:cast(self(), schedule),
  {noreply, S#state{result_size = ResSize + length(Results)}}.

%% private

schedule(S=#state{workers=[]}) ->
  S#state{workers=[]};
schedule(S=#state{workers=Workers}) ->
  lists:foreach(fun(Pid) -> gen_server:cast(Pid, {run, self()}) end, Workers),
  S#state{workers=[]}.

assign([], _Dev) ->
  [];
assign([Pid|Workers], Dev) ->
  N = 1000,
  case fastq:read_seq(Dev, N) of
    {ok, SeqList} when is_list(SeqList) ->
      FmIndex = {fmindex, {chromosome, "GL000192.1"}},
      SeqList1 = lists:map(fun({_,Seq}) -> Seq end, SeqList),
      Workload = {FmIndex, {fastq, SeqList1}},
      ok = worker_bwt:execute(Pid, Workload, self()),
      assign(Workers, Dev);
    eof ->
      lager:info("End of fastq file"),
      [Pid|Workers]
  end.
