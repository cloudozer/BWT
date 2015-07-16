-module(source).
-behaviour(gen_fsm).

-export([start_link/1, start_link/0, register_workers/2, run/4, push_workload/1]).
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

%% gen_fsm callbacks

-record(state, {workers = [], fastq, fastq_eof = false, workload_size = 100, client, chunks, sink}).

%% state ::= init|fetching_fastq|running|stopping

init(Client={_,Pid}) ->
  log:info("Start ~p", [?MODULE]),

  %% Start http service
  http:start_reg_link(),

  {ok, init, #state{client = Client}}.

terminate(normal, _StateName, _StateData) ->
  log:info("Stop ~p", [?MODULE]);
terminate(Reason, StateName, StateData) ->
  log:info("~p terminated: ~p", [?MODULE, {Reason, StateName}]).

handle_info({http_response, Bin}, fetching_fastq, S=#state{workers=Workers, chunks = Chunks, sink = SinkPid = {SNode,SPid}}) ->

log:info("handle_info({http_response"),
  MyNode = ?MODULE, %navel:get_node(),
  Self = self(),
  SelfRef = {MyNode,Self},
  lists:foreach(fun({{Node,Pid}, Chunk}) ->
    navel:call_no_return(Node, worker_bwt, run, [Pid,Chunk,SelfRef,SinkPid])
  end, lists:zip(Workers,lists:merge(Chunks))),

  %% Push first workload
  spawn_link(fun() -> ok = push_workload(Self) end),

  %% Run the Sink
  navel:call_no_return(SNode, gen_server, call, [SPid, {run, SelfRef, Workers}]),

  log:info("Splitting... ~p", [size(Bin)]),
  Fastq = binary:split(Bin, <<$\n>>, [global]),
  log:info("Done splitting"),

  {next_state, running, S#state{fastq = Fastq}};

handle_info(sink_done, stopping, S) ->
  {stop, normal, S}.

%% handle_call({register_workers, _}, _, S=#state{state_name = StateName}) when StateName =/= init ->
%% 	{reply, wait, S};
init({register_workers, Pids}, _From, S=#state{workers=Workers, client = {ClientNode,ClientPid}}) when Pids =/= [] ->
  %% monitor new workers
  %TODO lists:foreach(fun(Pid)->true = link(Pid) end, Pids),
  Workers1 = Pids++Workers,
  log:info("~p connected to ~b workers", [?MODULE, length(Workers1)]),
navel:call_no_return(ClientNode, erlang, send, [ClientPid, workers_ready]),
  {reply, ok, init, S#state{workers = Workers1}};

init({run, FastqFileUrl, Chunks, SinkPid}, _From, S=#state{}) ->

log:info("sorce run"),

  %% Async fetching of the fastq file
  http:get_async(FastqFileUrl),

  {reply, ok, fetching_fastq, S#state{chunks = Chunks, sink = SinkPid}}.

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

produce_workload(S = #state{fastq = Fastq, workload_size = WorkloadSize}) ->
  case produce_workload(WorkloadSize, Fastq, []) of
    {<<>>, SeqList} ->
      {SeqList, stopping, S#state{fastq = <<>>}};
    {Fastq1, SeqList} ->
      {SeqList, running, S#state{fastq = Fastq1}}
  end.

produce_workload(0, Fastq, Acc) ->
  {Fastq, Acc};
produce_workload(_Size, [<<>>], Acc) ->
  {<<>>, Acc};
produce_workload(Size, [<<$@, SName/binary>>, SData, <<$+>>, _Quality | Fastq], Acc) ->
  Seq = {SName, SData},
  produce_workload(Size - 1, Fastq, [Seq | Acc]).
