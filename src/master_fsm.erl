-module(master_fsm).
-behaviour(gen_fsm).

-export([test/0,run_on_worker/6]).
-export([init/1,handle_info/3,idle/2]).

-record(state, {client}).

-define(THRESHOLD,5).

test() ->
  Nodes = gen_server:call({cluster_manager, 'gc@104.131.46.157'}, get_nodes),
  {ok, Pid} = gen_fsm:start_link(?MODULE, {}, []),
  ok = gen_fsm:send_event(Pid, {run, 20, "ATGTGACACAGATCACTGCGGCCTTGACCTCCCAGGCTCCAGGTGGTTCTT","21","/home/drc/bwt_files/human_g1k_v37_decoy.fasta", Nodes}).

init(_Args) ->
  {ok, idle, #state{}}.

handle_info(Matches, busy, State) when is_list(Matches) ->
io:format("master got matches: ~n~p~n", [Matches]),
  {next_state, busy, State}.

idle({run, N,Seq,Ref_seq_name,File,Nodes}, State) -> 
  ReturnPid = self(),
  lists:foldl(fun(NodeName,J) ->
    Pid = spawn_link(NodeName, ?MODULE, run_on_worker, [N,Seq,Ref_seq_name,File,ReturnPid,J]),
    io:format("spawned ~p~n", [Pid]),
    J+1
  end, 0, Nodes),
  {next_state, busy, State}.

run_on_worker(N,Seq,Ref_seq_name,File,ReturnPid,J) ->
  seeds:generate_fs(Seq,15,2),
  compile:file("fs.erl",[report_errors]),
  code:add_path("."),
  code:load_file(fs),
  {Pos,Len} = msw:get_reference_position(Ref_seq_name,File),
  Chunk_size = Len div N,
  msw:worker(ReturnPid, Seq,File, Pos+J*Chunk_size, Chunk_size+?THRESHOLD+length(Seq)).
