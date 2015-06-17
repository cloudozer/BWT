% An implementation of worker that finds seeds using
% BWT (burrows-wheeler transform) 
%
% Cloudozer(c), 2015
%



-module(worker_bwt).

-export([start_link/0, start_link/1, run/4]).
-export([worker_loop/11]).

-include("bwt.hrl").

-define(WATERLINE, 10).
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

run(Pid, Chromosome, SourcePid, SinkPid) ->
  Pid ! {run, Chromosome, SourcePid, SinkPid}.

%% state_name ::= [init|running|stopping]

worker_loop(init, [], undefined, undefined, undefined, undefined, undefined,undefined,undefined,undefined, undefined) ->
  receive
    {run, Chromosome, SourcePid={SoNode,SoPid}, SinkPid} ->

      {Meta,FM} = fm_index:get_index(Chromosome, 1),

      {Pc,Pg,Pt,Last} = proplists:get_value(pointers, Meta),
      Shift = proplists:get_value(shift, Meta),

      {ok, BwtFiles} = application:get_env(worker_bwt_app, bwt_files),
      {ok, Ref} = file:read_file(filename:join(BwtFiles, Chromosome++".ref")),
      Extension = list_to_binary(lists:duplicate(?REF_EXTENSION_LEN, $N)),
      Ref1 = <<Extension/binary, Ref/binary, Extension/binary>>,

      worker_loop(running, [], SourcePid, SinkPid, FM, Ref, Pc,Pg,Pt,Last, Shift);

    Err -> throw({unsuitable, Err})
  end;

worker_loop(running, [], SourcePid={SoNode,SoPid}, SinkPid, FM, Ref, Pc,Pg,Pt,Last, Shift) ->
  receive
    stop ->
      worker_loop(stopping, [], SourcePid, SinkPid, FM, Ref, Pc,Pg,Pt,Last, Shift);
    {workload, Workload} when is_list(Workload) ->
      lager:info("worker got workload ~p", [length(Workload)]),
      worker_loop(running, Workload, SourcePid, SinkPid, FM, Ref, Pc,Pg,Pt,Last, Shift);
    Err -> 
      throw(Err)
  end;
worker_loop(running, QseqList, SourcePid, SinkPid={SiNode,SiPid}, FM, Ref, Pc,Pg,Pt,Last, Shift) ->
  erlang:garbage_collect(),
  Seeds = lists:foldl(
    fun({Qname,Qseq},Acc) ->
      case sga:sga(FM,Pc,Pg,Pt,Last,Qseq) of
        [] -> Acc;
        ResultsList -> [{{Qname,Qseq},ResultsList}|Acc]
      end
    end, [], QseqList),

  Ref_bin_size = byte_size(Ref),

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
        [{cigar, SeqName, Cigar, P, RefSeq} | Acc];
      _ ->
        [{TopCigar,P,RefSeq} | _] = lists:sort(fun({{R1,_},_,_}, {{R2,_},_,_}) -> R1 > R2 end, Cigars),
        [{cigar, SeqName, TopCigar, P, RefSeq} | Acc]
    end
  end, [], Seeds),

  %% Send result to the Sink
  navel:call_no_return(SiNode, gen_server, cast, [SiPid, {result, Results}]),

  lager:info("Worker ~p: -~b-> sga:sga -~b-> sw:sw -> done", [self(), length(QseqList), length(Seeds)]),

  worker_loop(running, [], SourcePid, SinkPid, FM, Ref, Pc,Pg,Pt,Last, Shift);

worker_loop(stopping, [], _SourcePid, {SiNode,SiPid}, _FM, _Ref, _Pc,_Pg,_Pt,_Last, _Shift) ->
  lager:info("Worker is stopping"),
  navel:call_no_return(MNode, erlang, send, [MPid, {done, {navel:get_node(),self()}}]).
