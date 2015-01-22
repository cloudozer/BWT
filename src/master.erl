-module(master).
-behaviour(gen_fsm).

-export([
  test_cluster/0,
  test_local/0
]).

-export([start_link/0, run/2, send_result/2, send_done_seq/2]).
-export([init/1, idle/3, busy/2]).

-record(state, {client, partititons, seq_match = [], seq_chunk = [], nodes}).

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

send_done_seq(Pid, SeqName) ->
  ok = gen_fsm:send_event(Pid, {done_seq, SeqName}).



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
  {reply, ok, busy, State#state{partititons=Partitions, nodes = Nodes}}.

busy({result, {{SeqName, SeqData}, Matches}}, S=#state{partititons=Partitions, seq_match = SeqMatch}) when is_list(Matches) ->
  lists:foreach(fun({Quality, Pos, {Up,Lines,Down}}) ->
    QualityPers = (Quality / length(SeqData)) * 50,
    io:format("Accuracy: ~.1f%   Seq: ~p   Genome part: ~p   Pos: ~p~n", [QualityPers, SeqName, schedule:get_genome_part_name(Partitions, Pos),Pos]),

    Trim = fun(L) ->
      Limit = 70,
      lists:sublist(L, Limit)
    end,

    io:format("~s ...~n", [Trim(Up)]),
    io:format("~s ...~n", [Trim(Lines)]),
    io:format("~s ...~n~n", [Trim(Down)])
  end, Matches),
  {next_state, busy, S#state{seq_match = [{SeqName, Matches} | SeqMatch]}};

busy({done_seq, SeqName}, S=#state{seq_match = SeqMatch, seq_chunk = SeqChunk, nodes = Nodes}) ->
  case proplists:get_value(SeqName, SeqChunk, 0) of
    NumChunksDone when NumChunksDone == length(Nodes) - 1 ->
      case proplists:append_values(SeqName, SeqMatch) of
        [] ->
          io:format("Sequence ~s no match found.~n~n", [SeqName]);
        MatchesList ->
          io:format("~s sequence: ~b matches found~n~n", [SeqName, length(MatchesList)])
      end,
      {next_state, busy, S};
    0 ->
%%       lager:info("Seq ~s done 1 node", [SeqName]),
      {next_state, busy, S#state{seq_chunk = [{SeqName,1} | SeqChunk]}};
    NumChunksDone ->
      NumNodesDone1 = NumChunksDone + 1,
%%       lager:info("Seq ~s done ~b nodes", [SeqName, NumNodesDone1]),
      SeqNode1 = lists:keyreplace(SeqName, 1, SeqChunk, {SeqName, NumNodesDone1}),
      {next_state, busy, S#state{seq_chunk = SeqNode1}}
  end.
