-module(source).
-behaviour(gen_server).

-export([start_link/1, start_link/0, register_workers/2, run/5]).
-export([test/4]).
-export([init/1, terminate/2, handle_info/2, handle_call/3, handle_cast/2]).

%% test

test(SeqFileName, Chromosome, WorkersNum, Debug) ->
  lager:start(),
  if Debug == true ->
    application:start(sasl),
    lager:set_loglevel(lager_console_backend, debug);
  true ->
    lager:set_loglevel(lager_console_backend, error)
  end,

  %% Start navel
  navel:start(tester),

  {LocalIP, LingdPort, SourcePort, WorkerStartPort} = {'127.0.0.1', 110, 120, 1},

  %% Start lingd daemon
  {ok, {LingdNode,LingdPid}} = lingd:start_link(LocalIP, LingdPort),
  LingdRef = {LingdNode,LingdPid},

  %% Start source app
  {ok, SourceNode} = lingd:create_link(LingdRef, source, {LocalIP, SourcePort}),
  lingd:connect(LingdRef, SourceNode, {'127.0.0.1', 0}),
  navel:connect('127.0.0.1',120),
  timer:sleep(500),
  ok = navel:call(SourceNode, application, start, [source]),

%%   %% Start sink app
%%   {ok, SinkNode} = slave:start_link('127.0.0.1', sink, ["-pa ebin deps/*/ebin apps/sink/ebin"]),
%%   ok = rpc:call(SinkNode, application, start, [sink]),

  %% Create worker processes
  Pids = lists:map(fun(N) ->
    %% Create a node
    NodeName = list_to_atom("erl" ++ integer_to_list(N)),
    {ok, Node} = lingd:create_link(LingdRef, NodeName, {LocalIP, N}),

    %% Start worker app (connect to it firstly)
    navel:connect('127.0.0.1',N),
    timer:sleep(500),
    ok = navel:call(Node, application, start, [worker_bwt_app]),

    %% Connect the node to the Source and to the Sink
    lingd:connect(LingdRef, Node, {LocalIP, SourcePort}),
    {NodeName, worker_bwt}
  end, lists:seq(WorkerStartPort, WorkersNum)),

  %% Associate them with the source
  ok = navel:call(SourceNode, ?MODULE, register_workers, [source,Pids]),

  ok = navel:call(SourceNode, ?MODULE, run, [source, SeqFileName, Chromosome, WorkersNum, self()]).

%% api

start_link() ->
  gen_server:start_link({local, source}, ?MODULE, {}, []).

start_link(Args) ->
  gen_server:start_link({local, source}, ?MODULE, Args, []).

register_workers(SourcePid, Pids) ->
  gen_server:call(SourcePid, {register_workers, Pids}).

run(Pid, SeqFileName, Chromosome, WorkersLimit, ClientPid) ->
  gen_server:call(Pid, {run, SeqFileName, Chromosome, WorkersLimit, ClientPid}, infinity).

%% gen_server callbacks

-record(state, {workers=[], fastq, fastq_eof = false, chromosome, workload_size = 200, client, stopping = false, start_time}).

init(_Args) ->
  lager:info("Start source"),
  {ok, #state{}}.

terminate(normal, _State) ->
  lager:info("Stop source");
terminate(Reason, State) ->
  lager:info("Source terminated: ~p", [{Reason, State}]).

handle_info({done,Pid}, S=#state{workers = [Pid], start_time = StartTime, client = ClientPid}) ->
  Microsec = timer:now_diff(now(), StartTime),
  Sec = Microsec / 1000000,
  lager:info("It's all over. ~.1f sec.", [Sec]),
  ClientPid ! {stop, Sec},
  {stop, normal, S};
handle_info({done,Pid}, S) ->
  {noreply, S#state{workers = lists:delete(Pid, S#state.workers)}}.


handle_call({register_workers, _}, _, S=#state{start_time=T}) when T =/= undefined ->
	{reply, wait, S};
handle_call({register_workers, Pids}, _From, S=#state{workers=Workers}) ->
  %% monitor new workers
  %TODO lists:foreach(fun(Pid)->true = link(Pid) end, Pids),
  S1 = S#state{workers=Pids++Workers},
  lager:info("The source connected to ~b workers", [length(S1#state.workers)]),
  {reply, ok, S1};

handle_call({run, FastqFileName, Chromosome, WorkersLimit, ClientPid}, _From, S=#state{workers=Workers}) when length(Workers) >= WorkersLimit ->
  {ok, BwtFiles} = application:get_env(bwt_files),
  {ok, FastqDev} = file:open(filename:join(BwtFiles, FastqFileName), [read, raw, read_ahead]),
  {Workers1, _Workers2} = lists:split(WorkersLimit, Workers),
  MyNode = ?MODULE, %navel:get_node(),
  lists:foreach(fun({Node,Pid}) -> navel:call_no_return(Node, worker_bwt, run, [Pid,Chromosome,{MyNode,self()}]) end, Workers1),
  %% TODO: demonitor the rest
  {reply, ok, S#state{fastq={FastqFileName, FastqDev}, chromosome = Chromosome, workers = Workers1, client = ClientPid, start_time = now()}};

handle_call({get_workload, N}, _From, S=#state{stopping = false}) ->
  {Result,S1} = produce_workload(N, S),
  {reply, Result, S1};
handle_call({get_workload, _N}, _From, S=#state{stopping = true}) ->
  {reply, stop, S}.


handle_cast({get_workload, N, {Node,Pid}}, State) ->
  Self = self(),
  spawn_link(fun() ->
    Resp = gen_server:call(Self, {get_workload, N}, 60000),
    navel:call_no_return(Node, erlang, send, [Pid,{workload,Resp}])
  end),
  {noreply, State};

handle_cast({cigar, _, {CigarRate, _}, _, _}, State) when CigarRate < 265 ->
  {noreply, State};
handle_cast({cigar, SeqName, Cigar = {CigarRate, CigarValue}, Pos, RefSeq}, State = #state{chromosome = Chromosome, client = ClientPid}) ->
  lager:info("Master got cigar: ~p ~p", [SeqName, Cigar]),
  io:format("~s      ~s      ~b      ~s      ~b      ~s~n", [SeqName, Chromosome, Pos, CigarValue, CigarRate, RefSeq]),
  ClientPid ! {cigar, SeqName, Chromosome, Pos, CigarValue, CigarRate, RefSeq},
  {noreply, State};

handle_cast(no_cigar, State) ->
  {noreply, State}.

%% private

produce_workload(N, State) ->
  produce_workload(N, State, []).

produce_workload(0, State, Acc) ->
  {Acc, State};

produce_workload(N, S = #state{fastq = {_, FqDev}, fastq_eof = false, chromosome = Chromosome, workload_size = WorkloadSize}, Acc) ->
  case fastq:read_seqs(FqDev, WorkloadSize) of
    {_, SeqList} ->
      Workload = SeqList,
      produce_workload(N-1, S, [Workload | Acc]);
    eof ->
      {Acc, S#state{fastq_eof = true}}
  end;

produce_workload(_N, S = #state{fastq_eof = true}, []) ->
  {stop, S#state{stopping = true}}.
