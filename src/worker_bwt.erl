% An implementation of worker that finds seeds using
% BWT (burrows-wheeler transform) 
%
% Cloudozer(c), 2015
%



-module(worker_bwt).

-export([start_link/3]).
-export([worker_loop/12]).

-include("bwt.hrl").


%% api

start_link(Chunk, SourcePid, SinkPid) ->
  Pid = spawn_opt(?MODULE, worker_loop,
				  [init, [], Chunk, SourcePid, SinkPid, undefined, undefined, undefined,undefined,undefined,undefined, undefined],
				  []),
  {ok, Pid}.

start_link({SoNode, SoPid}) ->
  {ok, Pid} = ?MODULE:start_link(),
  case navel:call(SoNode, master, register_workers, [SoPid,[{navel:get_node(),Pid}]]) of
    ok -> {ok, Pid};
    wait -> timer:sleep(1000), exit(wait)
  end.

run(Pid, Chunk, SourcePid, SinkPid) ->
log:info("run? ~p", [{Pid, Chunk, SourcePid, SinkPid}]),
  Pid ! {run, Chunk, SourcePid, SinkPid}.

%% state_name ::= [init|running|stopping]

worker_loop(init, [], {Chunk, _Mem}, SourcePid, SinkPid, undefined, undefined, undefined,undefined,undefined,undefined, undefined) ->
  log:info("Start ~p", [?MODULE]),
      {ok, BaseUrl} = application:get_env(worker_bwt_app,base_url),
      ChunkBin = list_to_binary(Chunk),
      [ChromosomeBin, ChunkIdFmBin] = binary:split(ChunkBin, <<"_p">>),
      [ChunkIdBin, <<>>] = binary:split(ChunkIdFmBin, <<".fm">>),
      Chromosome = binary_to_list(ChromosomeBin),
      ChunkId = binary_to_integer(ChunkIdBin),
      {ok, FmIndexBin} = http:get(BaseUrl ++ Chunk),
      {Meta,FM} = binary_to_term(FmIndexBin),

      {Pc,Pg,Pt,Last} = proplists:get_value(pointers, Meta),
      Shift = proplists:get_value(shift, Meta),

      {ok, Ref} = http:get(BaseUrl ++ Chromosome ++ "_p" ++ integer_to_list(ChunkId) ++ ".ref"),
      Extension = list_to_binary(lists:duplicate(?REF_EXTENSION_LEN, $N)),
      Ref1 = <<Extension/binary, Ref/binary, Extension/binary>>,
      {SNode,SPid} = SourcePid,
      Pid = spawn(fun() -> 
%	process_flag(suppress_gc, true),
	receive ready -> ok end,
        true = register(?MODULE, self()),
        ok = navel:call(SNode, source, worker_ready, [SPid, {navel:get_node(), ?MODULE}]),
        worker_loop(running, [], Chromosome, SourcePid, SinkPid, FM, Ref1, Pc,Pg,Pt,Last, Shift)
      end),
      Pid ! ready;

worker_loop(running, [], Chromosome, SourcePid, SinkPid, FM, Ref, Pc,Pg,Pt,Last, Shift) ->
  receive
    stop ->
      worker_loop(stopping, [], Chromosome, SourcePid, SinkPid, FM, Ref, Pc,Pg,Pt,Last, Shift);
    {workload, Workload} when is_list(Workload) ->
      log:info("worker got workload ~p", [length(Workload)]),
      log:info("memory ~p", [erlang:memory()]),
      worker_loop(running, Workload, Chromosome, SourcePid, SinkPid, FM, Ref, Pc,Pg,Pt,Last, Shift)
  end;
worker_loop(running, QseqList, Chromosome, SourcePid, SinkPid={SiNode,SiPid}, FM, Ref, Pc,Pg,Pt,Last, Shift) ->
  Seeds = lists:foldl(
    fun({Qname,Qseq},Acc) ->
      case sga:sga(FM,Pc,Pg,Pt,Last,binary_to_list(Qseq)) of
        [] -> Acc;
        ResultsList -> [{{Qname,Qseq},ResultsList}|Acc]
      end
    end, [], QseqList),

  Results = lists:foldl(fun({{SeqName, QsecBin}, Seeds1}, Acc) ->
    Qsec = binary_to_list(QsecBin),
    Cigars = lists:foldl(fun({S,D}, Acc) ->

      Ref_len = length(Qsec) + D,
      Start_pos = S - Ref_len + ?REF_EXTENSION_LEN,

      <<_:Start_pos/bytes,Ref_seq:Ref_len/bytes,_/binary>> = Ref,
      Ref_seq1 = binary_to_list(Ref_seq),

      case sw:sw(Qsec,Ref_seq1) of
        no_match -> Acc;
        Cigar -> [{Cigar,S - Ref_len + Shift,Ref_seq1} | Acc]
      end

    end, [],  Seeds1),

    case Cigars of
      [] -> Acc;
      [{Cigar,P,RefSeq}] ->
        [{cigar, Chromosome, SeqName, Cigar, P, RefSeq} | Acc];
      _ ->
        [{TopCigar,P,RefSeq} | _] = lists:sort(fun({{R1,_},_,_}, {{R2,_},_,_}) -> R1 > R2 end, Cigars),
        [{cigar, Chromosome, SeqName, TopCigar, P, RefSeq} | Acc]
    end
  end, [], Seeds),

  %% Send result to the Sink
  navel:call_no_return(SiNode, gen_server, cast, [SiPid, {result, Results}]),

  log:info("Worker ~p: -~b-> sga:sga -~b-> sw:sw -> done", [self(), length(QseqList), length(Seeds)]),

  worker_loop(running, [], Chromosome, SourcePid, SinkPid, FM, Ref, Pc,Pg,Pt,Last, Shift);

worker_loop(stopping, [], _Chromosome, _SourcePid, {SiNode,SiPid}, _FM, _Ref, _Pc,_Pg,_Pt,_Last, _Shift) ->
  log:info("~p is stopping", [?MODULE]),
  navel:call_no_return(SiNode, erlang, send, [SiPid, {done, {navel:get_node(),?MODULE}}]).
