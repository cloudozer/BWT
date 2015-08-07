-module(source).
-behaviour(gen_fsm).

-export([start_link/1, start_link/0, register_workers/2, run/4, push_workload/1, worker_ready/2]).
-export([init/1, terminate/3, handle_info/3, init/3, running/3, stopping/3]).

%% api

start_link() ->
  gen_fsm:start_link({local, ?MODULE}, ?MODULE, {}, []).

start_link(Args) ->
  gen_fsm:start_link({local, ?MODULE}, ?MODULE, Args, []).

register_workers(Pid, Pids) ->
  gen_fsm:sync_send_event(Pid, {register_workers, Pids}).

run(Pid, SeqFileName, Chunks, SinkPid) ->
  gen_fsm:sync_send_event(Pid, {run, SeqFileName, Chunks, SinkPid}, infinity).

push_workload(Pid) ->
  gen_fsm:sync_send_event(Pid, push_workload, infinity).

worker_ready(Pid, WorkerPid) ->
  gen_fsm:sync_send_event(Pid, {worker_ready, WorkerPid}).

%% gen_fsm callbacks

-record(state, {workers = [], workers_ready = [], fastq, fastq_eof = false, workload_size = 1000, client, chunks, sink, reads_num = 0}).

%% state ::= init|running|stopping

init(Client={_,Pid}) ->
  log:info("Start ~p", [?MODULE]),

  %% Start http service
  http:start_reg_link(),

  {ok, init, #state{client = Client}}.

terminate(normal, _StateName, _StateData) ->
  log:info("Stop ~p", [?MODULE]);
terminate(Reason, StateName, StateData) ->
  log:info("~p terminated: ~p", [?MODULE, {Reason, StateName}]).

handle_info({http_response, Bin}, init, S) ->
  {next_state, init, S#state{fastq = Bin}};

handle_info({sink_done,Sec}, stopping, S=#state{client = {ClientNode,ClientPid}, workers=Workers, reads_num = ReadsNum}) ->
  navel:call(ClientNode, erlang, send, [ClientPid, {source_sink_done, Sec, length(Workers), ReadsNum}]),
  {stop, normal, S}.

init({register_workers, Pids}, _From, S=#state{workers=Workers, client = {ClientNode,ClientPid}}) when Pids =/= [] ->
  %% monitor new workers
  %TODO lists:foreach(fun(Pid)->true = link(Pid) end, Pids),
  Workers1 = Pids++Workers,
  log:info("~p connected to ~b workers", [?MODULE, length(Workers1)]),

navel:call_no_return(ClientNode, erlang, send, [ClientPid, workers_ready]),

  {reply, ok, init, S#state{workers = Workers1}};

init({run, FastqFileUrl, Chunks, SinkPid}, _From, S=#state{}) ->
  %% Async fetching of the fastq file
  http:get_async(FastqFileUrl),
  {reply, ok, init, S#state{chunks = Chunks, sink = SinkPid}};

init({worker_ready, Pid}, _From, S = #state{workers = Workers, workers_ready = WorkersReady, client = Client, sink = {SNode,SPid}}) when length(Workers)-1 == length(WorkersReady) ->

  MyNode = ?MODULE, %navel:get_node(),
  Self = self(),
  SelfRef = {MyNode,Self},

  %% Push first workload
  spawn_link(fun() -> ok = push_workload(Self) end),

  %% Run the Sink
  navel:call_no_return(SNode, gen_server, call, [SPid, {run, SelfRef, Workers, Client}]),
  {reply, ok, running, S#state{workers_ready = [Pid | WorkersReady]}};

init({worker_ready, Pid}, _From, S = #state{workers_ready = WorkersReady}) ->
  {reply, ok, init, S#state{workers_ready = [Pid | WorkersReady]}}.

running(push_workload, _From, S=#state{workers = Workers}) ->
  {W,StateName,S1} = produce_workload(S),
  lists:foreach(fun({Node,Pid}) ->
    navel:call_no_return(Node, erlang, send, [Pid, {workload, W}])
  end, Workers),
  {reply, ok, StateName, S1}.

stopping(push_workload, _From, S) ->
  lists:foreach(fun({Node,Pid}) ->
    navel:call_no_return(Node, erlang, send, [Pid, stop])
  end, S#state.workers),
  {reply, stopping, stopping, S}.

%% private

produce_workload(S = #state{fastq = Fastq, workload_size = WorkloadSize, reads_num = ReadsNum}) ->
  case produce_workload(WorkloadSize, Fastq, []) of
    {<<>>, SeqList} ->
      {SeqList, stopping, S#state{fastq = <<>>}};
    {Fastq1, SeqList} ->
      {SeqList, running, S#state{fastq = Fastq1, reads_num = ReadsNum + length(SeqList)}}
  end.

produce_workload(0, Fastq, Acc) ->
  {Fastq, Acc};
%produce_workload(_Size, [<<>>], Acc) ->
produce_workload(_Size, <<>>, Acc) ->
  {<<>>, Acc};
%produce_workload(Size, [<<$@, SName/binary>>, SData, <<$+>>, _Quality | Fastq], Acc) ->
produce_workload(Size, Bin, Acc) ->
  [<<$@, SName/binary>>, Bin1] = binary:split(Bin, <<$\n>>),
  [SData, Bin2] = binary:split(Bin1, <<$\n>>),
  [<<$+>>, Bin3] = binary:split(Bin2, <<$\n>>),
  [_Quality, Bin4] = binary:split(Bin3, <<$\n>>),
  Seq = {SName, SData},
  produce_workload(Size - 1, Bin4, [Seq | Acc]).
