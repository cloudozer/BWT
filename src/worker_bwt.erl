% An implementation of worker that finds seeds using
% BWT (burrows-wheeler transform) 
%
% Cloudozer(c), 2015
%



-module(worker_bwt).

-export([start_link/0, start_link/1, run/3]).
-export([worker_loop/10]).

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

start_link({MNode, MPid}) ->
  {ok, Pid} = ?MODULE:start_link(),
  case navel:call(MNode, master, register_workers, [MPid,[{navel:get_node(),Pid}]]) of
    ok -> {ok, Pid};
    wait -> timer:sleep(1000), exit(wait)
  end.

run(Pid, Chromosome, MasterPid) ->
  Pid ! {run, Chromosome, MasterPid}.

%% state_name ::= [init|running|stopping]

worker_loop(init, [], undefined, undefined, undefined,undefined,undefined,undefined, undefined, 0) ->
  receive
    {run, Chromosome, MasterPid1={MNode,MPid}} ->
      %% erlang:garbage_collect(),

      navel:call_no_return(MNode, gen_server, cast, [MPid, {get_workload, ?WATERLINE, {navel:get_node(),self()}}]),

      {Meta,FM} = fm_index:get_index(Chromosome, 1),
      {Pc,Pg,Pt,Last} = proplists:get_value(pointers, Meta),
      Shift = proplists:get_value(shift, Meta),
      {ok, FmIndices} = application:get_env(worker_bwt_app, fm_indices),
      %% !Hardcoded
      {ok, Ref} = file:read_file(filename:join(FmIndices, Chromosome++"_p1.ref")),
      Extension = list_to_binary(lists:duplicate(?REF_EXTENSION_LEN, $N)),
      Ref1 = <<Extension/binary, Ref/binary, Extension/binary>>,

      worker_loop(running, [], MasterPid1, FM, Ref1, Pc,Pg,Pt,Last, Shift);

    Err -> throw({unsuitable, Err})
  end;

worker_loop(running, [], MasterPid={MNode,MPid}, FM, Ref, Pc,Pg,Pt,Last, Shift) ->
  receive
    {workload, stop} ->
      worker_loop(stopping, [], MasterPid, FM, Ref, Pc,Pg,Pt,Last, Shift);
    {workload, Workload} when is_list(Workload) ->
      lager:info("A worker got workload ~p~n", [length(Workload)]),
      navel:call_no_return(MNode, gen_server, cast, [MPid, {get_workload, ?WATERLINE, {navel:get_node(),self()}}]),
      worker_loop(running, Workload, MasterPid, FM, Ref, Pc,Pg,Pt,Last, Shift);
    Err -> 
      throw(Err)
  end;
worker_loop(running, [QseqList | WorkloadRest], MasterPid={MNode,MPid}, FM, Ref, Pc,Pg,Pt,Last, Shift) ->
  Seeds = lists:foldl(
    fun({Qname,Qseq},Acc) ->
      case sga:sga(FM,Pc,Pg,Pt,Last,Qseq) of
        [] -> Acc;
        ResultsList -> [{{Qname,Qseq},ResultsList}|Acc]
      end
    end, [], QseqList),

  lists:foreach(fun({{SeqName, Qsec}, Seeds1}) ->

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
      [] -> navel:call_no_return(MNode, gen_server, cast, [MPid, no_cigar]);
      [{Cigar,P,RefSeq}] ->
        navel:call_no_return(MNode, gen_server, cast, [MPid, {cigar, SeqName, Cigar, P, RefSeq}]);
      _ ->
        [{TopCigar,P,RefSeq} | _] = lists:sort(fun({{R1,_},_,_}, {{R2,_},_,_}) -> R1 > R2 end, Cigars),
        navel:call_no_return(MNode, gen_server, cast, [MPid, {cigar, SeqName, TopCigar, P, RefSeq}])
    end

  end, Seeds),

  lager:info("Worker ~p: -~b-> sga:sga -~b-> sw:sw -> done", [self(), length(QseqList), length(Seeds)]),

  worker_loop(running, WorkloadRest, MasterPid, FM, Ref, Pc,Pg,Pt,Last, Shift);

worker_loop(stopping, [], {MNode,MPid}, _FM, _Ref, _Pc,_Pg,_Pt,_Last, _Shift) ->
  lager:info("Worker is stopping"),
  navel:call_no_return(MNode, erlang, send, [MPid, {done, {navel:get_node(),self()}}]).

