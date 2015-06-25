-module(master).
-behaviour(gen_server).

-export([start_link/1, start_link/0, register_workers/2, run/4]).
-export([test/0, test/4, test_create_worker/2]).
-export([init/1, terminate/2, handle_info/2, handle_call/3, handle_cast/2]).

%% test

test() ->
  test("21_not_found.fastq", "21", 1, false).

test(SeqFileName, Chromosome, WorkersNum, Debug) ->
  lager:start(),
  if Debug == true ->
    lager:set_loglevel(lager_console_backend, debug);
  true ->
    lager:set_loglevel(lager_console_backend, error)
  end,

  %% Start master app
  ok = application:start(master),
  %% Create worker processes
  Pids = lists:map(fun(N) ->
    NodeName = list_to_atom("erl" ++ integer_to_list(N)),
    {ok, Node} = slave:start_link('127.0.0.1', NodeName, ["-pa ebin deps/*/ebin"]),
    {_WNode, _WPid} = rpc:call(Node, ?MODULE, test_create_worker, [NodeName, N])
  end, lists:seq(1, WorkersNum)),
  %% Associate them with the master
  ok = ?MODULE:register_workers(master,Pids),
  %% Tell the master to run
  ok = ?MODULE:run(master, SeqFileName, Chromosome, WorkersNum).

test_create_worker(NodeName, N) ->
  navel:start(NodeName, N),
  MasterIp = {127,0,0,1},
  navel:connect(MasterIp),
  application:set_env(worker_bwt_app,master_ip,MasterIp),
  application:set_env(worker_bwt_app,bwt_files,"bwt_files"),
  {ok, WPid} = worker_bwt:start_link(),
  {navel:get_node(), WPid}.

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

-record(state, {workers=[], fastq, fastq_eof = false, chromosome, workload_size = 200, client, stopping = false, start_time}).

init(_Args) ->
  lager:info("Started master"),
  {ok, #state{}}.

terminate(Reason, State) ->
  lager:info("Master terminated: ~p", [{Reason, State}]).

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
  CurW = length(S1#state.workers),
  case application:get_env(kernel,workers) of
    undefined ->
      lager:info("The master got ~b workers", [CurW]);
    {ok, EnvW} ->
      lager:info("The master got ~b/~b workers", [CurW, EnvW]),
      if 
        CurW == EnvW ->
          timer:apply_after(500,gen_server,call,[master, {run, "SRR770176_1.fastq", "GL000193.1", EnvW}]);
        true ->
          ok
      end
  end,
  {reply, ok, S1};

handle_call({run, FastqFileName, Chromosome, WorkersLimit}, {ClientPid,_}, S=#state{workers=Workers}) when length(Workers) >= WorkersLimit ->
  {ok, BwtFiles} = application:get_env(bwt_files),
  {ok, FastqDev} = file:open(filename:join(BwtFiles, FastqFileName), []),
  {Workers1, _Workers2} = lists:split(WorkersLimit, Workers),
  MyNode = navel:get_node(),
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
    Resp = gen_server:call(Self, {get_workload, N}, 600000),
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
