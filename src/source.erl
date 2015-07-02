-module(source).
-behaviour(gen_server).

-export([start_link/1, start_link/0, register_workers/2, run/5]).
-export([init/1, terminate/2, handle_info/2, handle_call/3]).

%% api

start_link() ->
  gen_server:start_link({local, source}, ?MODULE, {}, []).

start_link(Args) ->
  gen_server:start_link({local, source}, ?MODULE, Args, []).

register_workers(SourcePid, Pids) ->
  gen_server:call(SourcePid, {register_workers, Pids}).

run(Pid, SeqFileName, Chunks, ClientPid, SinkPid) ->
  gen_server:call(Pid, {run, SeqFileName, Chunks, ClientPid, SinkPid}, infinity).

%% gen_server callbacks

-record(state, {state_name = init, workers = [], fastq, fastq_eof = false, workload_size = 3000, client}).
%% state_name ::= init|running|stopping

init(_Args) ->
  log:info("Start source"),
  {ok, #state{}}.

terminate(normal, _State) ->
  log:info("Stop source");
terminate(Reason, State) ->
  log:info("Source terminated: ~p", [{Reason, State}]).

handle_info(done, S) ->
  {stop, normal, S}.

handle_call({register_workers, _}, _, S=#state{state_name = StateName}) when StateName =/= init ->
	{reply, wait, S};
handle_call({register_workers, Pids}, _From, S=#state{workers=Workers}) ->
  %% monitor new workers
  %TODO lists:foreach(fun(Pid)->true = link(Pid) end, Pids),
  Workers1 = Pids++Workers,
  log:info("The source connected to ~b workers", [length(Workers1)]),
  {reply, ok, S#state{workers = Workers1}};

handle_call({run, FastqFileName, Chunks, ClientPid, SinkPid={SNode,SPid}}, _From, S=#state{workers=Workers}) ->
  {ok, BwtFiles} = application:get_env(bwt_files),
  {ok, FastqDev} = file:open(filename:join(BwtFiles, FastqFileName), [read, raw, read_ahead]),
  MyNode = ?MODULE, %navel:get_node(),
  Self = self(),
  SelfRef = {MyNode,Self},
  lists:foreach(fun({{Node,Pid}, Chunk}) ->
    navel:call_no_return(Node, worker_bwt, run, [Pid,Chunk,SelfRef,SinkPid])
  end, lists:zip(Workers,lists:merge(Chunks))),

  %% Push first workload
  spawn_link(fun() -> ok = gen_server:call(Self, push_workload) end),

  %% Run the Sink
  navel:call_no_return(SNode, gen_server, call, [SPid, {run, SelfRef, Workers}]),

  {reply, ok, S#state{state_name = running, fastq={FastqFileName, FastqDev}, client = ClientPid}};

handle_call(push_workload, _From, S=#state{state_name = stopping}) ->
  lists:foreach(fun({Node,Pid}) ->
    navel:call_no_return(Node, erlang, send, [Pid, stop])
  end, S#state.workers),
  {reply, stopping, S};
handle_call(push_workload, _From, S=#state{state_name = running, workers = Workers}) ->
  {W,S1} = produce_workload(S),
%ResultTest = if Result =/= [] -> [hd(Result)]; true -> [] end,
  lists:foreach(fun({Node,Pid}) ->
    navel:call_no_return(Node, erlang, send, [Pid, {workload, W}])
  end, Workers),
  {reply, ok, S1}.

%% private

produce_workload(S = #state{fastq = {_, FqDev}, fastq_eof = false, workload_size = WorkloadSize}) ->
  case fastq:read_seqs(FqDev, WorkloadSize) of
    {_, SeqList} ->
      {SeqList, S};
    eof ->
      {[], S#state{fastq_eof = true, state_name = stopping}}
  end.
