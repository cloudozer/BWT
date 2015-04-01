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

%% test

test() ->
  lager:start(),

  %% Create a master process
  {ok, MPid} = ?MODULE:start_link([]),
  %% Create worker processes
  {ok, WPid1} = worker_bwt:start_link(),
  {ok, WPid2} = worker_bwt:start_link(),
  {ok, WPid3} = worker_bwt:start_link(),
  {ok, WPid4} = worker_bwt:start_link(),
  {ok, WPid5} = worker_bwt:start_link(),
  %% Associate them with the master
  ok = master:register_workers(MPid, [WPid1, WPid2, WPid3, WPid4, WPid5]),
  %% Tell the master to run
  SeqFileName = "bwt_files/SRR770176_1.fastq",
  %SeqNum = fastq:size("bwt_files/SRR770176_1.fastq"),
  ok = gen_server:call(MPid, {run, SeqFileName}, infinity).

%% gen_server callbacks

-record(state, {running=false, workers=[], tasks=[], fastq}).

init(_Args) ->
  lager:info("Started master"),
  {ok, #state{}}.

terminate(Reason, State) ->
  lager:info("Master terminated: ~p", [{Reason, State}]).

%% It's ok
handle_info({'DOWN',_Ref,process,_Pid,normal}, State) ->
  {noreply, State}.

handle_call({register_workers, Pids}, _From, S=#state{workers=Workers}) ->
  %% monitor new workers
  lists:foreach(fun(Pid)->monitor(process, Pid) end, Pids),
  S1 = S#state{workers=Pids++Workers},
  S2 = if S#state.running == true ->
    %% assing tasks to the workers
    schedule(S1);
    true -> S1
  end,
  {reply, ok, S2};
handle_call({run, FastqFileName}, _From, S=#state{running=false, workers=Workers}) when length(Workers) > 0 ->
  {ok, FastqDev} = file:open(FastqFileName, [read, raw, read_ahead]),
  S1 = schedule(S#state{fastq={FastqFileName, FastqDev}, running=true}),
  {reply, ok, S1};
  
handle_call({results, Results}, _From={Pid,_}, S=#state{running=true, workers=Workers}) ->
  lager:info("master got results ~p", [Results]),
  gen_server:cast(self(), schedule),
  {reply, ok, S#state{workers=[Pid|Workers]}}.

handle_cast(schedule, State) ->
  {noreply, schedule(State)}.

%% private

schedule(S=#state{workers=Workers, fastq={FqFile, FqDev}}) ->
  Workers1 = assign(Workers, FqDev),
  S#state{workers=Workers1}.

assign([], _Dev) ->
  [];
assign([Pid|Workers], Dev) ->
  N = 777,
  case fastq:read_seq(Dev, N) of
    {ok, SeqList} when is_list(SeqList) ->
      FmIndex = {fmindex, {file, "fm_binary_index"}},
      SeqList1 = lists:map(fun({_,Seq}) -> Seq end, SeqList),
      Workload = {FmIndex, {fastq, SeqList1}},
      ok = worker_bwt:execute(Pid, Workload, self()),
      assign(Workers, Dev)
  end.

split(Number, ChunksNum) ->
  split_inner(lists:seq(1,Number,Number div ChunksNum), []).

split_inner([From, To], Acc) -> 
  [{From, To}|Acc];
split_inner([From | [To | _] = Rest], Acc) ->
  split_inner(Rest, [{From, To}|Acc]).
