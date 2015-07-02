% An implementation of worker that finds seeds using
% BWT (burrows-wheeler transform) 
%
% Cloudozer(c), 2015
%



-module(worker_bwt).

-export([start_link/0, start_link/1, run/4]).
-export([worker_loop/12]).

-include("bwt.hrl").

-define(REF_EXTENSION_LEN, 200).

%% api

start_link() ->
  Pid = spawn_opt(?MODULE,
				  worker_loop,
				  [init, [], undefined, undefined, undefined, undefined, undefined, undefined, undefined, 0],
				  []),
  true = is_pid(Pid),
  {ok, Pid}.

start_link({SoNode, SoPid}) ->
  {ok, Pid} = ?MODULE:start_link(),
  case navel:call(SoNode, master, register_workers, [SoPid,[{navel:get_node(),Pid}]]) of
    ok -> {ok, Pid};
    wait -> timer:sleep(1000), exit(wait)
  end.

run(Pid, Chunk, SourcePid, SinkPid) ->
  Pid ! {run, Chunk, SourcePid, SinkPid}.

%% state_name ::= [init|running|stopping]

worker_loop(init, [], undefined, undefined, undefined, undefined, undefined, undefined,undefined,undefined,undefined, undefined) ->
  receive
    {run, {Chunk, _Mem}, SourcePid={SoNode,SoPid}, SinkPid} ->
      ChunkBin = list_to_binary(Chunk),
      [ChromosomeBin, ChunkIdBin, <<>>] = binary:split(ChunkBin, [<<"_p">>,<<".fm">>], [global]),
      Chromosome = binary_to_list(ChromosomeBin),
      ChunkId = binary_to_integer(ChunkIdBin),
      {Meta,FM} = fm_index:get_index(Chromosome, ChunkId),

      {Pc,Pg,Pt,Last} = proplists:get_value(pointers, Meta),
      Shift = proplists:get_value(shift, Meta),

      {ok, FmIndicesPath} = application:get_env(worker_bwt_app,fm_indices),
      {ok, Ref} = file:read_file(filename:join(FmIndicesPath, binary:replace(ChunkBin, <<".fm">>, <<".ref">>))),
      Extension = list_to_binary(lists:duplicate(?REF_EXTENSION_LEN, $N)),
      Ref1 = <<Extension/binary, Ref/binary, Extension/binary>>,

      worker_loop(running, [], Chromosome, SourcePid, SinkPid, FM, Ref1, Pc,Pg,Pt,Last, Shift);

    Err -> throw({unsuitable, Err})
  end;

worker_loop(running, [], Chromosome, SourcePid={SoNode,SoPid}, SinkPid, FM, Ref, Pc,Pg,Pt,Last, Shift) ->
  receive
    stop ->
      worker_loop(stopping, [], Chromosome, SourcePid, SinkPid, FM, Ref, Pc,Pg,Pt,Last, Shift);
    {workload, Workload} when is_list(Workload) ->
      log:info("worker got workload ~p", [length(Workload)]),
      worker_loop(running, Workload, Chromosome, SourcePid, SinkPid, FM, Ref, Pc,Pg,Pt,Last, Shift);
    Err -> 
      throw(Err)
  end;
worker_loop(running, QseqList, Chromosome, SourcePid, SinkPid={SiNode,SiPid}, FM, Ref, Pc,Pg,Pt,Last, Shift) ->
  Seeds = lists:foldl(
    fun({Qname,Qseq},Acc) ->
      case sga:sga(FM,Pc,Pg,Pt,Last,Qseq) of
        [] -> Acc;
        ResultsList -> [{{Qname,Qseq},ResultsList}|Acc]
      end
    end, [], QseqList),

  Results = lists:foldl(fun({{SeqName, Qsec}, Seeds1}, Acc) ->

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

worker_loop(stopping, [], Chromosome, _SourcePid, {SiNode,SiPid}, _FM, _Ref, _Pc,_Pg,_Pt,_Last, _Shift) ->
  lager:info("Worker is stopping"),
  navel:call_no_return(MNode, erlang, send, [MPid, {done, {navel:get_node(),self()}}]).
