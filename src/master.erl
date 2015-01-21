-module(master).
-behaviour(gen_fsm).

-export([
  %test/0,
  test_local/0,
run_on_worker/6]).

-export([start_link/0, run/2, send_result/2]).
-export([init/1, idle/3, busy/3]).

-record(state, {client}).

-define(THRESHOLD,5).

%test() ->
%  Nodes = gen_server:call({cluster_manager, 'gc@104.131.46.157'}, get_nodes),
%  {ok, Pid} = gen_fsm:start_link(?MODULE, {}, []),
%  ok = gen_fsm:send_event(Pid, {run, 20, "ATGTGACACAGATCACTGCGGCCTTGACCTCCCAGGCTCCAGGTGGTTCTT","21","/home/drc/bwt_files/human_g1k_v37_decoy.fasta", Nodes}).

test_local() ->
  lager:start(),
  {ok, WorkerSrv} = worker_bwt:start_link(),
  Nodes = [node(WorkerSrv)],
  {ok, Master} = ?MODULE:start_link(),
  RefFile = "human_g1k_v37_decoy.fasta",
  IndexFile = "human_g1k_v37_decoy.fasta.index",
  SeqFile = "SRR770176_1.fastq",
  MasterPath = "bwt_files",
  WorkerPath = MasterPath, % "/home/drc/bwt_files",
  NodesNbr = length(Nodes),
  ChunkSize = 5000000,
  Args = {RefFile,IndexFile,SeqFile, MasterPath,WorkerPath, Nodes, NodesNbr, ChunkSize},  
  ?MODULE:run(Master, Args).

start_link() ->
  gen_fsm:start_link(?MODULE, {}, []).

run(Pid, Args) ->
  ok = gen_fsm:sync_send_event(Pid, {run, Args}).

send_result(Pid, Matches) ->
  ok = gen_fsm:sync_send_event(Pid, {result, Matches}).

%% Callbacks

init(_Args) ->
  {ok, idle, #state{}}.

idle({run, {RefFile,IndexFile,SeqFile, MasterPath,WorkerPath, Nodes, NodesNbr, ChunkSize}}, _From, State) when NodesNbr >= length(Nodes) -> 
  Schedule = schedule:get_schedule(filename:absname_join(MasterPath, IndexFile), ChunkSize, NodesNbr), 
  NodesNbr = length(Schedule),

  {Nodes1,_} = lists:split(NodesNbr, Nodes),

  MasterPid = self(),
  lists:foreach(fun({NodeName,Workload}) ->
    Worker = {worker_bwt, NodeName},
    %Pid = spawn_link(NodeName, ?MODULE, run_on_worker, [N,Seq,Ref_seq_name,File,ReturnPid,J]),
    Args = {
      RefFile,IndexFile,SeqFile,WorkerPath,
      Workload,
      MasterPid
    },
    ok = worker:run(Worker, Args),
    io:format("started ~p~n", [Worker])
  end, lists:zip(Nodes1, Schedule)),
  {reply, ok, busy, State}.

busy({result, []}, _From, State) ->
  {reply, ok, busy, State};
busy({result, Matches}, _From, State) when is_list(Matches) ->
  lager:info("Master got matches: ~n~p", [Matches]),
  {reply, ok, busy, State}.

%% 

run_on_worker(N,Seq,Ref_seq_name,File,ReturnPid,J) ->
  seeds:generate_fs(Seq,15,2),
  compile:file("fs.erl",[report_errors]),
  code:add_path("."),
  code:load_file(fs),
  {Pos,Len} = msw:get_reference_position(Ref_seq_name,File),
  Chunk_size = Len div N,
  msw:worker(ReturnPid, Seq,File, Pos+J*Chunk_size, Chunk_size+?THRESHOLD+length(Seq)).
