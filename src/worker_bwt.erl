% An implementation of worker that finds seeds using
% BWT (burrows-wheeler transform) 
%
% Cloudozer(c), 2015
%



-module(worker_bwt).
-behaviour(gen_server).

-export([start_link/0, start_link/1, run/2]).
-export([init/1, terminate/2, handle_cast/2, handle_call/3]).
-export([slave_loop/5]).

-include("bwt.hrl").

%% api

start_link() ->
  gen_server:start_link(?MODULE, {}, []).

start_link({MNode, MPid}) ->
  {ok, Pid} = gen_server:start_link(?MODULE, {}, []),
  ok = navel:call(MNode, master, register_workers, [MPid,[{navel:get_node(),Pid}]]),
  {ok, Pid}.

run(Pid, MasterPid) ->
  gen_server:cast(Pid, {run, MasterPid}).

%% callbacks

-record(state, {master, slave, workloads = [], workload_waterline = 10, stopping = false}).

init(_) ->
  {ok, #state{}}.

terminate(normal, _State) ->
  lager:info("garbage_collection: ~p~nexact_reductions: ~p~nmemory: ~p", [erlang:statistics(garbage_collection), erlang:statistics(exact_reductions), erlang:memory()]);
terminate(Reason, State) ->
  lager:error("A worker is terminated: ~p~n~p", [Reason, State]).

handle_cast({run, MasterPid={MNode,MPid}}, S=#state{slave = undefined, workloads = [], workload_waterline = Waterline}) ->
  erlang:garbage_collect(),

%%   Workload = gen_server:call(MasterPid, {get_workload, Waterline}, 60000),
  Workload = navel:call(MNode, gen_server, call, [MPid, {get_workload, Waterline}, 60000]),
  true = is_list(Workload),
  SlavePid = spawn_link(?MODULE, slave_loop, [MasterPid, self(), Workload, undefined, undefined]),
%%   gen_server:cast(MasterPid, {get_workload, Waterline, self()}),
  navel:call_no_return(MNode, gen_server, cast, [MPid, {get_workload, Waterline, self()}]),
  {noreply, S#state{master = MasterPid, slave = SlavePid}};

handle_cast({workload, stop}, S) ->
  {noreply, S#state{stopping = true}};
handle_cast({workload, Workload}, S = #state{workloads = WorkloadList}) when is_list(Workload) ->
  lager:info("worker got workload ~p", [length(Workload)]),
  {noreply, S#state{workloads = Workload ++ WorkloadList}}.

handle_call(get_workload, {SlavePid, _}, S = #state{slave = SlavePid, workloads = [], stopping = true}) ->
  lager:info("Worker is stopping"),
  {stop, normal, stop, S};
handle_call(get_workload, {SlavePid, _}, S = #state{slave = SlavePid, workloads = [], workload_waterline = Waterline, master = {MNode,MPid}, stopping = false}) ->
%%   gen_server:cast(MasterPid, {get_workload, Waterline, self()}),
  navel:call_no_return(MNode, gen_server, cast, [MPid, {get_workload, Waterline, self()}]),
  {reply, wait, S};

handle_call(get_workload, {SlavePid, _}, S = #state{slave = SlavePid, workloads = Workloads, workload_waterline = Waterline, master = {MNode,MPid}}) ->
  WorkLen = length(Workloads),

  if WorkLen =< Waterline ->
    %% gen_server:cast(MasterPid, {get_workload, Waterline, self()});
    navel:call_no_return(MNode, gen_server, cast, [MPid, {get_workload, Waterline, self()}]);
    true -> ok
  end,
%%   {Workloads1, WorkloadsRest} = lists:split(WorkLen div 2, Workloads),

  lager:info("slave get_workload ~p", [length(Workloads)]),
%%   lager:info("slave get_workload ~p", [{length(Workloads1),length(WorkloadsRest)}]),

  {reply, {ok, Workloads}, S#state{workloads = []}}.
%%   {reply, {ok, Workloads1}, S#state{workloads = WorkloadsRest}}.

%% private

slave_loop(MasterPid, WorkerPid, Workload=[{Chromosome,_}|_], undefined, undefined) ->
  {Meta,FM} = fm_index:get_index(Chromosome, 1),

  {Pc,Pg,Pt,Last} = proplists:get_value(pointers, Meta),
  Shift = proplists:get_value(shift, Meta),

  {ok, BwtFiles} = application:get_env(bwt,bwt_files),
  {ok, Ref} = file:read_file(filename:join(BwtFiles, Chromosome++".ref")),
  slave_loop(MasterPid, WorkerPid, Workload, FM, Ref, Pc,Pg,Pt,Last, Shift).

slave_loop(MasterPid, WorkerPid, [], FM, Ref, Pc,Pg,Pt,Last, Shift) ->
  case gen_server:call(WorkerPid, get_workload) of
    {ok, Workload} ->
      slave_loop(MasterPid, WorkerPid, Workload, FM, Ref, Pc,Pg,Pt,Last, Shift);
    wait ->
      timer:sleep(1000),
      slave_loop(MasterPid, WorkerPid, [], FM, Ref, Pc,Pg,Pt,Last, Shift);
    stop ->
      lager:info("Worker's slave is stopping"),
      bye
  end;

slave_loop(MasterPid={MNode,MPid}, WorkerPid, [{Chromosome, QseqList} | WorkloadRest], FM, Ref, Pc,Pg,Pt,Last, Shift) ->
  erlang:garbage_collect(),

  Seeds = lists:foldl(
    fun({Qname,Qseq},Acc) ->
      case sga:sga(FM,Pc,Pg,Pt,Last,Qseq) of
        [] -> Acc;
        ResultsList -> [{{Qname,Qseq},ResultsList}|Acc]
      end
    end, [], QseqList),

  Ref_bin_size = byte_size(Ref),

  lists:foreach(fun({{SeqName, Qsec}, Seeds1}) ->

    Cigars = lists:foldl(fun({S,D}, Acc) ->

      Ref_len = length(Qsec) + D,
      Start_pos = (S - Ref_len) bsl 3,

      {Ref1, Start_pos1} = if S > Ref_bin_size ->
        Ns = binary:copy(<<"N">>, S - Ref_bin_size),
        {<<Ref/binary, Ns/binary>>, Start_pos};
                                 (S - Ref_len) < 0 ->
                                   Ns = binary:copy(<<"N">>, -(S - Ref_len)),
                                   {<<Ns/binary, Ref/binary>>, 0};
                                 true ->
                                   {Ref, Start_pos}
                               end,

      <<_:Start_pos1,Ref_seq:Ref_len/bytes,_/binary>> = Ref1,
      Ref_seq1 = binary_to_list(Ref_seq),

      case sw:sw(Qsec,Ref_seq1) of
        no_match -> Acc;
        Cigar -> [{Cigar,S - Ref_len + Shift,Ref_seq1} | Acc]
      end

    end, [],  Seeds1),

    case Cigars of
      [] -> ok;
      [{Cigar,P,RefSeq}] ->
        navel:call_no_return(MNode, gen_server, cast, [MPid, {cigar, SeqName, Cigar, P, RefSeq}]);
      _ ->
        [{TopCigar,P,RefSeq} | _] = lists:sort(fun({{R1,_},_,_}, {{R2,_},_,_}) -> R1 > R2 end, Cigars),
        navel:call_no_return(MNode, gen_server, cast, [MPid, {cigar, SeqName, TopCigar, P, RefSeq}])
    end

  end, Seeds),

  lager:info("Worker ~p: -~b-> sga:sga -~b-> sw:sw -> done", [self(), length(QseqList), length(Seeds)]),

  slave_loop(MasterPid, WorkerPid, WorkloadRest, FM, Ref, Pc,Pg,Pt,Last, Shift).
