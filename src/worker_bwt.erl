-module(worker_bwt).
-behaviour(gen_server).

-export([start_link/0, execute/3]).
-export([init/1, handle_info/2, handle_call/3, handle_cast/2]).

-include("bwt.hrl").

%% api
start_link() ->
  gen_server:start_link(?MODULE, {}, []).

execute(Pid, Workload, MasterPid) ->
  gen_server:call(Pid, {execute, Workload, MasterPid}, infinity).

%% callbacks

-record(state, {busy=false, master, slave}).

init(_Args) ->
  {ok, #state{}}.

%% It's ok
handle_info({'DOWN',Ref,process,_Pid,normal}, S=#state{slave=Ref}) ->
  {noreply, S}.

handle_call({execute, Workload, MasterPid}, _From, S=#state{busy=false}) ->
  {{fmindex, {file, FmFileName}}, {fastq, QseqList}} = Workload,
  lager:info("Worker ~p got workload ~p", [self(), length(QseqList)]),
  FM = fm:read_file(filename:join(?BWT_FILES, FmFileName)),
  lager:info("Worker have read the FM-index"),
  WorkerPid = self(),
  SlavePid = spawn_link(fun() ->
    Results = lists:map(fun(Qseq) -> sga:sga(FM,Qseq) end, QseqList),
    lager:info("Worker ~p done sga:sga", [self()]),
    gen_server:cast(WorkerPid, {results, Results})
  end),
  SlaveRef = erlang:monitor(process, SlavePid),
  {reply, ok, S#state{busy=true, master=MasterPid, slave=SlaveRef}}.

handle_cast({results, Results}=Msg, S=#state{busy=true, master=MasterPid}) ->
  ok = gen_server:call(MasterPid, Msg, infinity),
  {noreply, S#state{busy=false}}.
