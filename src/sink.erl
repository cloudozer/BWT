-module(sink).
-behaviour(gen_server).
-export([init/1, terminate/2, handle_info/2, handle_cast/2, handle_call/3]).
-export([start_link/0]).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {}, []).

%% callbacks

-record(state, {workers = [], start_time, source}).

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

handle_call({run, SourcePid, Workers}, _From, State) ->
  {reply, ok, State#state{workers = Workers, start_time = now(), source = SourcePid}}.

handle_cast({cigar, _, {CigarRate, _}, _, _}, State) when CigarRate < 265 ->
  {noreply, State};
handle_cast({cigar, SeqName, Cigar = {CigarRate, CigarValue}, Pos, RefSeq}, State) ->
%% handle_cast({cigar, SeqName, Cigar = {CigarRate, CigarValue}, Pos, RefSeq}, State = #state{chromosome = Chromosome, client = ClientPid}) ->
  lager:info("Sink got a cigar: ~p ~p", [SeqName, Cigar]),
  io:format("Sink got a cigar: ~p ~p~n", [SeqName, Cigar]),
%%   io:format("~s      ~s      ~b      ~s      ~b      ~s~n", [SeqName, Chromosome, Pos, CigarValue, CigarRate, RefSeq]),
%%   ClientPid ! {cigar, SeqName, Chromosome, Pos, CigarValue, CigarRate, RefSeq},
  {noreply, State};

handle_cast(no_cigar, State) ->
  {noreply, State}.