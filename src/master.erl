-module(master).
-behaviour(gen_fsm).

-export([
  test_cluster/0,
  test_local/0
]).

-export([start_link/0, run/2, send_result/2]).
-export([init/1, idle/3, busy/2]).

-record(state, {client, partititons}).

-include("bwt.hrl").

test_cluster() ->
  lager:start(),
  Nodes = gen_server:call({cluster_manager, 'gc@104.131.46.157'}, get_nodes),
  io:format("Genome sequence matching Demo~n"),
  io:format("Erlang cluster: ~b nodes~n", [length(Nodes)]),
  io:format("Node RAM: 512Mb~n"),
  {ok, Master} = ?MODULE:start_link(),
  RefFile = "human_g1k_v37_decoy.fasta",
  IndexFile = "human_g1k_v37_decoy.fasta.index",
  SeqFile = "SRR770176_1.fastq",
  MasterPath = "bwt_files",
  WorkerPath = "/home/drc/bwt_files",
  ChunkSize = 2000,
  Args = {RefFile,IndexFile,SeqFile, MasterPath,WorkerPath, Nodes, ChunkSize},  
  ?MODULE:run(Master, Args).
%  ok = gen_fsm:send_event(Pid, {run, 20, "ATGTGACACAGATCACTGCGGCCTTGACCTCCCAGGCTCCAGGTGGTTCTT","21","/home/drc/bwt_files/human_g1k_v37_decoy.fasta", Nodes}).

test_local() ->
  lager:start(),
  {ok, WorkerSrv} = worker_bio:start_link(),
  Nodes = [node(WorkerSrv)],
  {ok, Master} = ?MODULE:start_link(),
  RefFile = "human_g1k_v37_decoy.fasta",
  IndexFile = "human_g1k_v37_decoy.fasta.index",
  SeqFile = "SRR770176_1.fastq",
  MasterPath = "bwt_files",
  WorkerPath = MasterPath, % "/home/drc/bwt_files",
  ChunkSize = 2000,
  Args = {RefFile,IndexFile,SeqFile, MasterPath,WorkerPath, Nodes, ChunkSize},  
  ?MODULE:run(Master, Args).

start_link() ->
  gen_fsm:start_link(?MODULE, {}, []).

run(Pid, Args) ->
  ok = gen_fsm:sync_send_event(Pid, {run, Args}, infinity).

send_result(Pid, Matches) ->
  ok = gen_fsm:send_event(Pid, {result, Matches}).

%% Callbacks

init(_Args) ->
  {ok, idle, #state{}}.

idle({run, {RefFile,IndexFile,SeqFile, MasterPath,WorkerPath, Nodes, ChunkSize}}, _From, State) -> 
  NodesNbr = length(Nodes),
  {Workload,Partitions} = schedule:get_workload_and_index(filename:absname_join(MasterPath, IndexFile), ChunkSize, NodesNbr),
  Schedule = schedule:get_schedule(Workload, Partitions, ChunkSize), 
  {Nodes1,_} = lists:split(NodesNbr, Nodes),

  MasterPid = self(),
  lists:foreach(fun({NodeName,Workload}) ->
    Worker = {worker_bio, NodeName},
    %Pid = spawn_link(NodeName, ?MODULE, run_on_worker, [N,Seq,Ref_seq_name,File,ReturnPid,J]),
    Args = {
      RefFile,IndexFile,SeqFile,WorkerPath,
      Workload,
      MasterPid
    },
    spawn_link(fun() ->
      ok = worker_bio:run(Worker, Args)
      %lager:info("started ~p~n", [Worker])
    end)
  end, lists:zip(Nodes1, Schedule)),
  {reply, ok, busy, State#state{partititons=Partitions}}.

busy({result, {{SeqName, SeqData}, Matches}}, S=#state{partititons=Partitions}) when is_list(Matches) ->
  lists:foreach(fun({Quality, Pos, {Up,Lines,Down}}) ->
    io:format("Quality: ~p   Seq: ~p   Genome part: ~p   Pos: ~p~n", [Quality, SeqName, schedule:get_genome_part_name(Partitions, Pos),Pos]),

    Trim = fun(L) ->
      Limit = 50,
      lists:sublist(L, Limit)
    end,

    io:format("~s ...~n", [Trim(Up)]),
    io:format("~s ...~n", [Trim(Lines)]),
    io:format("~s ...~n~n", [Trim(Down)])
  end, Matches),
  {next_state, busy, S}.
