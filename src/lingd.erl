-module(lingd).
-behaviour(gen_fsm).


-export([start_link/0, create/2]).
-export([init/1, slave/3]).

-record(state, {host = '127.0.0.1', port_increment = 10, slave_opts = "-pa ebin deps/*/ebin apps/*/ebin"}).


%% lingd API

start_link() ->
  OsType = erlang:system_info(os_type),
  start_link(OsType).

start_link({xen,ling}) ->
  throw({xen,ling});
start_link(OsType={_,_}) ->
  State = #state{},
  {ok, Node} = slave:start_link(State#state.host, ?MODULE, [State#state.slave_opts]),
  PortInc = State#state.port_increment,
  {ok,_} = rpc:call(Node, navel, start, [?MODULE, PortInc]),
  ok = navel:connect(State#state.host, PortInc),
timer:sleep(1000),
  {ok, Pid} = navel:call(?MODULE, gen_fsm, start_link, [?MODULE,  {OsType,#state{port_increment = PortInc + 1}}, []]),
  NNode = ?MODULE, %%rpc:call(Node, navel, get_node, []),   % returns {badrpc,{'EXIT',{noproc,{gen_server,call,[navel,get_node]}}}
  {ok, {NNode, Pid}}.

create({LNode,LPid},Name) ->
  {ok, {Host,PortInc}} = navel:call(LNode, gen_fsm, sync_send_event, [LPid, {create, Name}]),
  ok = navel:connect(Host,PortInc),
timer:sleep(1000),
  {ok, {Host,PortInc}}.

%% broadcast(NodePids, Msg) ->
%%   lists:foreach(fun({Node,Pid}) ->
%%     navel:call_no_return(Node, erlang, send, [Pid,Msg])
%%   end, NodePids).

%% private

init({{xen,ling}, _State}) ->
  {stop, {not_implemented,xen,ling}};

init({{_,_}, State}) ->
  {ok, slave, State}.

slave({create, Name}, _From, S=#state{host = Host, port_increment = PortInc}) ->
  {ok, Node} = slave:start_link(Host, Name, [S#state.slave_opts]),
  {ok,_} = rpc:call(Node, navel, start, [Name, PortInc]),
  {reply, {ok, {Host,PortInc}}, slave, S#state{port_increment = PortInc + 1}}.

  
