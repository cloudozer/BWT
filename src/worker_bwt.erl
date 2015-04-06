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

-record(state, {busy=false, master, slave, fms=[], workload}).

init(_) ->
  {ok, #state{}}.

%% The slave is finished
handle_info({'DOWN',Ref,process,_Pid,normal}, S=#state{slave=Ref}) ->
  {noreply, S}.

handle_call({execute, Workload, MasterPid}, _From, S=#state{busy=false, fms=FMs}) ->
  {{fmindex, {chromosome, ChromoName}}, {fastq, QseqList}} = Workload,
  {FM, FMs1} =
    case proplists:get_value(ChromoName, FMs) of
      undefined ->
        FM = bwt:get_index(ChromoName),
  	lager:info("Worker is read the FM-index"),
        {FM, [{ChromoName, FM}]};
      FM ->
        {FM, FMs}
    end,
  lager:info("Worker ~p got workload ~p", [self(), length(QseqList)]),
  SlaveRef = spawn_slave(FM, QseqList),
  {reply, ok, S#state{busy = true, master = MasterPid, slave = SlaveRef, fms = FMs1}}.

%% a cast from the slave
handle_cast({results, _Results} = Msg, S = #state{busy=true, master=MasterPid}) ->
  %% retranslate the results to the master
  ok = gen_server:call(MasterPid, Msg, infinity),
  {noreply, S#state{busy=false}}.

%% private

spawn_slave(FM, QseqList) ->
  WorkerPid = self(),
  SlavePid = spawn_link(fun() ->
    Results = lists:foldl(fun(Qseq,Acc) -> case sga:sga(FM,Qseq) of [] -> Acc; R -> [R|Acc] end end, [], QseqList),
    lager:info("Worker ~p done ~p sga:sga", [self(), length(QseqList)]),
    gen_server:cast(WorkerPid, {results, Results})
  end),
  erlang:monitor(process, SlavePid).
