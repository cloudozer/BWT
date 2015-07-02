-module(source).
-behaviour(gen_server).

-export([start_link/1, start_link/0, register_workers/2, run/5]).
-export([test/3]).
-export([init/1, terminate/2, handle_info/2, handle_call/3]).

%% test

test(SeqFileName, ChromosomeList, Debug) ->
  if Debug == true ->
    application:start(sasl);
  true ->
    ok
  end,

  {LocalIP, LingdPort, SourcePort, SinkPort, WorkerStartPort} = {'127.0.0.1', 10, 20, 30, 40},

  %% Start navel
  navel:start(tester),

  %% Start lingd daemon
  {ok, {LingdNode,LingdPid}} = lingd:start_link(LocalIP, LingdPort),
  LingdRef = {LingdNode,LingdPid},

  %% Start source app
  {ok, SourceNode} = lingd:create_link(LingdRef, source, {LocalIP, SourcePort}),
  lingd:connect(LingdRef, SourceNode, LocalIP),
  ok = navel:call(SourceNode, application, start, [source]),

  %% Start sink app
  {ok, SinkNode} = lingd:create_link(LingdRef, sink, {LocalIP, SinkPort}),
  lingd:connect(LingdRef, SinkNode, LocalIP),
  ok = navel:call(SinkNode, application, start, [sink]),
  %% Connect the Sink to the Source
  lingd:connect(LingdRef, SinkNode, {LocalIP,SourcePort}),

BoxNbr = 1,
BoxMem = 4000,
ChunksList = schedule:chunks_to_box(ChromosomeList,BoxNbr,BoxMem),

%% Workaround for latest schedule:chunks_to_box, remove all source, sink.
ChunksList1 = [lists:filter(fun({source,_})->false; ({sink,_})->false; (_)->true end, Chunks) || Chunks <- ChunksList],

	lists:foreach(fun({source,_})->ignore; ({sink,_})->ignore; (Chunks) ->

	  %% Start workers
	  WorkersNum = length(Chunks),
	  Pids = lists:map(fun(N) ->
	    %% Create a node
	    NodeName = list_to_atom("erl" ++ integer_to_list(N)),
	    {ok, Node} = lingd:create_link(LingdRef, NodeName, {LocalIP, N}),

	    %% Start worker app (connect it firstly)
	    lingd:connect(LingdRef, Node, LocalIP),
	    ok = navel:call(Node, application, start, [worker_bwt_app]),

	    %% Connect the node to the Source and to the Sink
	    lingd:connect(LingdRef, Node, {LocalIP, SourcePort}),
	    lingd:connect(LingdRef, Node, {LocalIP, SinkPort}),
	    {NodeName, worker_bwt}
	  end, lists:seq(WorkerStartPort, WorkerStartPort+WorkersNum-1)),

	  %% Associate them with the Source
	  ok = navel:call(SourceNode, ?MODULE, register_workers, [source,Pids])
	end, ChunksList1),

  %% Run everything
  ok = navel:call(SourceNode, ?MODULE, run, [source, SeqFileName, ChunksList1, self(), {SinkNode,sink}]).

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

handle_info({done,Pid}, S=#state{workers = [Pid], start_time = StartTime, client = ClientPid, chromosome = Chromosome, fastq={FastqFileNam,_}, stat_workload_amount = WorkloadAmount}) ->
  Microsec = timer:now_diff(now(), StartTime),
  Sec = Microsec / 1000000,

  StatTemplate = "~nReads: ~p~nReference seq: ~p~nChromosomes: ~p~nReads aligned: ~p~nAlignment completion time: ~.1f sec~nWorkers: ~p~nDate/time: ~4.10.0B-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B~n~n",
  ReferenceFile = "human_g1k_v37_decoy.fasta",
  {{Year,Month,Day},{Hour,Min,Sec1}} = erlang:localtime(),
  Statistics = [
    FastqFileNam,
    ReferenceFile,
    Chromosome,
    WorkloadAmount,
    Sec,
    S#state.workers_num,
    Year, Month, Day, Hour, Min, Sec1
  ],
  io:format(StatTemplate, Statistics),

  ClientPid ! {stop, Sec},
  {stop, normal, S};
handle_info({done,Pid}, S) ->
  {noreply, S#state{workers = lists:delete(Pid, S#state.workers)}};

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
