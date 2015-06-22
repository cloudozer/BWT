-module(sink).
-behaviour(gen_server).
-export([init/1, terminate/2, handle_info/2, handle_cast/2, handle_call/3]).
-export([start_link/0]).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {}, []).

%% callbacks

-record(state, {state_name = init, workers = [], start_time, source, results_counter}).

init(_Args) ->
  lager:info("Start Sink"),
  {ok, #state{}}.

terminate(normal, _State) ->
  lager:info("Stop Sink").

handle_info({done,Pid}, S=#state{workers = [Pid], source = {SNode,SPid}, start_time = StartTime}) ->
%% handle_info({done,Pid}, S=#state{workers = [Pid], start_time = StartTime, client = ClientPid}) ->
  Microsec = timer:now_diff(now(), StartTime),
  Sec = Microsec / 1000000,
  lager:info("It's all over. ~.1f sec.", [Sec]),
io:format("It's all over. ~.1f sec.~n", [Sec]),
%%   ClientPid ! {stop, Sec},
  navel:call_no_return(SNode, erlang, send, [SPid, done]),
  {stop, normal, S};
handle_info({done,Pid}, S) ->
  {noreply, S#state{workers = lists:delete(Pid, S#state.workers)}}.

handle_call({run, SourcePid={SNode,SPid}, Workers}, _From, State) ->
  navel:call_no_return(SNode, gen_server, call, [SPid, push_workload]),
  {reply, ok, State#state{workers = Workers, start_time = now(), source = SourcePid, results_counter = 0}}.


handle_cast({result, Result}, S=#state{state_name = stopping}) ->
  process_result(Result,S),
  {noreply, S};
handle_cast({result, Result}, S=#state{results_counter = ResultsCounter, workers = Workers, source = {Node,Pid}}) when ResultsCounter == length(Workers) - 1 ->
  process_result(Result,S), 
  case navel:call(Node, gen_server, call, [Pid, push_workload]) of
    ok -> 
      {noreply, S#state{results_counter = 0}};
    stopping ->
      {noreply, S#state{results_counter = 0, state_name = stopping}}
  end;
handle_cast({result, Result}, S=#state{results_counter = ResultsCounter}) ->
  process_result(Result,S),
  {noreply, S#state{results_counter = ResultsCounter + 1}}.

process_result([{cigar, _, {CigarRate, _}, _, _}|Rest], State) when CigarRate < 280 ->
  process_result(Rest, State);
process_result([{cigar, SeqName, Cigar = {CigarRate, CigarValue}, Pos, RefSeq} | Rest], State) ->
%% handle_cast({cigar, SeqName, Cigar = {CigarRate, CigarValue}, Pos, RefSeq}, State = #state{chromosome = Chromosome, client = ClientPid}) ->
  lager:info("Sink got a cigar: ~p ~p", [SeqName, Cigar]),
  io:format("Sink got a cigar: ~p ~p~n", [SeqName, Cigar]),
%%   io:format("~s      ~s      ~b      ~s      ~b      ~s~n", [SeqName, Chromosome, Pos, CigarValue, CigarRate, RefSeq]),
%%   ClientPid ! {cigar, SeqName, Chromosome, Pos, CigarValue, CigarRate, RefSeq},
  process_result(Rest, State);
process_result([], _S) ->
  ok.
