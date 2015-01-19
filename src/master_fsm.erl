-module(master_fsm).
-behaviour(gen_fsm).

-export([test/1,run_on_worker/5]).
-export([init/1,idle/2]).

-record(state, {client}).

-define(THRESHOLD,5).

test(Nodes) ->
  {ok, Pid} = gen_fsm:start_link(?MODULE, {}, []),
  ok = gen_fsm:send_event(Pid, {run, 20, "ATGTGACACAGATCACTGCGGCCTTGACCTCCCAGGCTCCAGGTGGTTCTT","21","/home/drc/bwt_files/human_g1k_v37_decoy.fasta", Nodes}).

init(_Args) ->
  {ok, idle, #state{}}.

idle({run, N,Seq,Ref_seq_name,File,Nodes}, State) -> 
  ReturnPid = self(),
  lists:foreach(fun(NodeName) ->
    spawn_link(NodeName, ?MODULE, run_on_worker, [N,Seq,Ref_seq_name,File,ReturnPid])
  end, Nodes),
  {next_state, busy, State}.

run_on_worker(N,Seq,Ref_seq_name,File,ReturnPid) ->
  seeds:generate_fs(Seq,15,2),
  compile:file("fs.erl",[report_errors]),
  code:add_path("."),
  code:load_file(fs),
  {Pos,Len} = msw:get_reference_position(Ref_seq_name,File),
  Chunk_size = Len div N,
  msw:worker(ReturnPid, Seq,File, Pos, Chunk_size+?THRESHOLD+length(Seq)).
