% An implementation of worker that finds seeds using
% BWT (burrows-wheeler transform) 
%
% Cloudozer(c), 2015
%



-module(worker_bwt).
-behaviour(gen_server).

-export([start_link/0, start_link/1, run/2]).
-export([init/1, terminate/2, handle_info/2, handle_cast/2]).
-export([workload_buffer_loop/5, slave_loop/6]).

-include("bwt.hrl").

%% api

start_link() ->
  gen_server:start_link(?MODULE, {}, []).

start_link(MasterPid) ->
  {ok, Pid} = gen_server:start_link(?MODULE, {}, []),
  ok = master:register_workers(MasterPid, [Pid]),
  {ok, Pid}.

run(Pid, MasterPid) ->
  gen_server:cast(Pid, {run, MasterPid}).

%% callbacks

-record(state, {master, slave}).

init(_) ->
  {ok, #state{}}.

terminate(Reason, State) ->
  lager:error("A worker is terminated: ~p~n~p", [Reason, State]).

handle_info({'DOWN', _Ref, process, SlavePid, normal}, #state{slave = SlavePid}) ->
  {noreply, #state{}}.

handle_cast({run, MasterPid}, S=#state{slave = undefined}) ->
  WorkloadBufPid = spawn_link(?MODULE, workload_buffer_loop, [MasterPid, self(), 5, [], beg_forever]),
  {ok, Workload} = get_workload(WorkloadBufPid),
  SlavePid = spawn_link(?MODULE, slave_loop, [MasterPid, self(), WorkloadBufPid, Workload, [], []]),
  erlang:monitor(process, SlavePid),
  {noreply, S#state{master = MasterPid, slave = SlavePid}}.

%% private

slave_loop(MasterPid, WorkerPid, WorkloadBufPid, {{fmindex, {chromosome, ChromoName}}, {fastq, QseqList}}, FMs, Refs) ->
  {{Meta, FM}, FMs1} =
    case proplists:get_value(ChromoName, FMs) of
      undefined ->
        MetaFM = bwt:get_index(ChromoName),
        {MetaFM, [{ChromoName, MetaFM}]};
      MetaFM ->
        {MetaFM, FMs}
    end,

  {Pc,Pg,Pt} = proplists:get_value(pointers, Meta),

  Seeds = lists:foldl(
    fun({QSeqName,Qseq},Acc) ->
      case sga:sga(FM,Pc,Pg,Pt,Qseq) of
        [] -> Acc;
        ResultsList -> [{QSeqName,ResultsList}|Acc]
      end
    end, [], QseqList),
  lager:info("Worker ~p done ~p sga:sga", [self(), length(QseqList)]),
  gen_server:cast(MasterPid, {seeds, Seeds}),

  case get_workload(WorkloadBufPid) of
    {ok, Workload1} ->
      slave_loop(MasterPid, WorkerPid, WorkloadBufPid, Workload1, FMs1, Refs);
    %% TODO: change
    stop ->
      stop
  end;

slave_loop(MasterPid, WorkerPid, WorkloadBufPid, {{ref, {chromosome, Chromosome}}, {fastq, undefined}, {seeds, Seeds}}, FMs, Refs) ->

  {Ref_bin, Refs1} =
    case proplists:get_value(Chromosome, Refs) of
      undefined ->
        {ok, BwtFiles} = application:get_env(bwt,bwt_files),
        {ok, Ref_bin} = file:read_file(filename:join(BwtFiles, Chromosome++".ref")),
        {Ref_bin, [{Chromosome,Ref_bin}|Refs]};
      Ref_bin ->
        {Ref_bin, Refs}
    end,

  Seeds1 = lists:merge(Seeds),

  lists:foreach(fun({SeqName, Seeds2}) ->

%%     Qsec = fastq:get_value(SeqName, "bwt_files/SRR770176_1.fastq"),
    Qsec = "GGAAGAGAGGGAGACACAGGAGAAGAGAGGTCAGTCGTCAGATCGGAAGAGCACACGTCTGAACTCCAGTCACCGATGTATCTCGTATGCCGTCTTCTGCTTGAAAAAAAAAAACAAAACCACACACACACACACTCACCCTTCCCCCTTA",

    lists:foreach(fun({S,D}) ->

      Ref_len = length(Qsec) + D,
      Start_pos = (S - Ref_len) bsl 3,
%%       lager:info("Start pos: ~p~nRef_len: ~p",[Start_pos,Ref_len]),

      <<_:Start_pos,Ref_seq:Ref_len/bytes,_/binary>> = Ref_bin,
      Ref = binary_to_list(Ref_seq),
%%       lager:info("Reference: ~p",[Ref]),
%%       lager:info("Query seq: ~p",[Qsec]),

      Cigar = no_match, % = sw:sw(Qsec,Ref),

%%       lager:info("Cigar: ~p", [Cigar]),

      if Cigar =/= no_match ->
        gen_server:cast(MasterPid, {cigar, SeqName, Cigar});
        true -> ok
      end

    end, Seeds2)

  end, Seeds1),

  case get_workload(WorkloadBufPid) of
    {ok, Workload1} ->
      slave_loop(MasterPid, WorkerPid, WorkloadBufPid, Workload1, FMs, Refs1);
    %% TODO: change
    stop ->
      stop
  end.

workload_buffer_loop(MasterPid, WorkerPid, Waterline, WorkloadList, beg_forever) when Waterline > length(WorkloadList) ->
  case gen_server:call(MasterPid, get_workload) of
    {ok, Workload} ->
      workload_buffer_loop(MasterPid, WorkerPid, Waterline, [Workload | WorkloadList], beg_forever);
    undefined ->
      workload_buffer_loop(MasterPid, WorkerPid, Waterline, WorkloadList, beg_forever)
  end;
workload_buffer_loop(MasterPid, WorkerPid, Waterline, [Workload | WorkloadList], NextAction) ->
  receive
    {get_workload, Pid} ->
      Pid ! {ok, Workload},
      workload_buffer_loop(MasterPid, WorkerPid, Waterline, WorkloadList, NextAction)
  end;
workload_buffer_loop(_MasterPid, _WorkerPid, _Waterline, [], stop) ->
  receive
    {get_workload, Pid} ->
      Pid ! stop,
      %% TODO: wait for stop signal from the master
      stop
  end.

get_workload(SupplierPid) ->
  SupplierPid ! {get_workload, self()},
  receive
    {ok, W} -> {ok, W};
    Error -> Error
  end.
