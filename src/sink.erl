-module(sink).
-behaviour(gen_server).
-export([init/1, terminate/2, handle_info/2, handle_cast/2, handle_call/3]).
-export([start_link/0]).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {}, []).

%% callbacks

-record(state, {state_name = init, workers = [], start_time, source, results_counter, client}).

init(_Args) ->
  log:info("Start ~p", [?MODULE]),
  {ok, #state{}}.

terminate(normal, _State) ->
  log:info("Stop ~p", [?MODULE]).

%handle_info({done,Pid}, S=#state{workers = [Pid], start_time = StartTime, client = ClientPid, chromosome = Chromosome, fastq={FastqFileNam,_}, stat_workload_amount = WorkloadAmount}) ->
%  Microsec = timer:now_diff(now(), StartTime),
%  Sec = Microsec / 1000000,
%
%  StatTemplate = "~nReads: ~p~nReference seq: ~p~nChromosomes: ~p~nReads aligned: ~p~nAlignment completion time: ~.1f sec~nWorkers: ~p~nDate/time: ~4.10.0B-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B~n~n",
%  ReferenceFile = "human_g1k_v37_decoy.fasta",
%  {{Year,Month,Day},{Hour,Min,Sec1}} = erlang:localtime(),
%  Statistics = [
%    FastqFileNam,
%    ReferenceFile,
%    Chromosome,
%    WorkloadAmount,
%    Sec,
%    S#state.workers_num,
%    Year, Month, Day, Hour, Min, Sec1
%  ],
%  io:format(StatTemplate, Statistics),
%
%  ClientPid ! {stop, Sec},
%  {stop, normal, S};
%handle_info({done,Pid}, S) ->
%  {noreply, S#state{workers = lists:delete(Pid, S#state.workers)}};

handle_info({done,Pid}, S=#state{workers = [Pid], source = {SNode,SPid}, start_time = StartTime, client = {CNode,CPid}}) ->
%% handle_info({done,Pid}, S=#state{workers = [Pid], start_time = StartTime, client = ClientPid}) ->
  Microsec = timer:now_diff(now(), StartTime),
  Sec = Microsec / 1000000,
  log:info("It's all over. ~.1f sec.", [Sec]),
io:format("It's all over. ~.1f sec.~n", [Sec]),
%%   ClientPid ! {stop, Sec},
  navel:call_no_return(SNode, erlang, send, [SPid, sink_done]),
  navel:call_no_return(CNode, erlang, send, [CPid, sink_done]),
  {stop, normal, S};
handle_info({done,Pid}, S) ->
  {noreply, S#state{workers = lists:delete(Pid, S#state.workers)}}.

handle_call({run, SourcePid={SNode,SPid}, Workers, Client}, _From, State) ->
  navel:call_no_return(SNode, source, push_workload, [SPid]),
  {reply, ok, State#state{workers = Workers, start_time = now(), source = SourcePid, results_counter = 0, client = Client}}.


handle_cast({result, Result}, S=#state{state_name = stopping}) ->
  process_result(Result,S),
  {noreply, S};
handle_cast({result, Result}, S=#state{results_counter = ResultsCounter, workers = Workers, source = {Node,Pid}}) when ResultsCounter == length(Workers) - 1 ->
  process_result(Result,S), 
  case navel:call(Node, source, push_workload, [Pid]) of
    ok -> 
      {noreply, S#state{results_counter = 0}};
    stopping ->
      {noreply, S#state{results_counter = 0, state_name = stopping}}
  end;
handle_cast({result, Result}, S=#state{results_counter = ResultsCounter}) ->
  process_result(Result,S),
  {noreply, S#state{results_counter = ResultsCounter + 1}}.

process_result([{cigar, _, _, {CigarRate, _}, _, _}|Rest], State) when CigarRate < 280 ->
  process_result(Rest, State);
process_result([{cigar, Chromosome, SeqName, Cigar = {CigarRate, CigarValue}, Pos, RefSeq} | Rest], State) ->
   io:format("~s      ~s      ~b      ~s      ~b      ~s~n", [SeqName, Chromosome, Pos, CigarValue, CigarRate, RefSeq]),
%%   ClientPid ! {cigar, SeqName, Chromosome, Pos, CigarValue, CigarRate, RefSeq},
  process_result(Rest, State);
process_result([], _S) ->
  ok.
