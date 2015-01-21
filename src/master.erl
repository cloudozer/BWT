-module(master).
-behaviour(gen_fsm).

-export([
  test_cluster/0,
  test_local/0,
run_on_worker/6]).

-export([start_link/0, run/2, send_result/2]).
-export([init/1, idle/3, busy/2]).

-record(state, {client, partititons}).

-define(THRESHOLD,5).

test_cluster() ->
  lager:start(),
  Nodes = gen_server:call({cluster_manager, 'gc@104.131.46.157'}, get_nodes),
  lager:info("Genome sequence matching Demo"),
  lager:info("Erlang cluster: ~b nodes", [length(Nodes)]),
  lager:info("Node RAM: 512Mb"),
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
    %spawn_link(fun() ->
      ok = worker_bio:run(Worker, Args),
      lager:info("started ~p~n", [Worker])
    %end)
  end, lists:zip(Nodes1, Schedule)),
  {reply, ok, busy, State#state{partititons=Partitions}}.

busy({result, Matches}, S=#state{partititons=Partitions}) when is_list(Matches) ->
  lists:foreach(fun({Pos, {Up,Lines,Down}}) ->
    lager:info("Genone part: ~p Pos: ~p", [schedule:get_genome_part_name(Partitions, Pos),Pos]),
    lager:info("~p", [Up]),
    lager:info("~p", [Lines]),
    lager:info("~p~n", [Down])
  end, Matches),
  {next_state, busy, S}.

%% 

run_on_worker(N,Seq,Ref_seq_name,File,ReturnPid,J) ->
  seeds:generate_fs(Seq,15,2),
  compile:file("fs.erl",[report_errors]),
  code:add_path("."),
  code:load_file(fs),
  {Pos,Len} = msw:get_reference_position(Ref_seq_name,File),
  Chunk_size = Len div N,
  msw:worker(ReturnPid, Seq,File, Pos+J*Chunk_size, Chunk_size+?THRESHOLD+length(Seq)).