-module(lingd).
-behaviour(gen_server).
-compile([export_all]).

%% lingd API

start_link(Host, PortInc) ->
  {ok, Node} = slave:start_link(Host, ?MODULE, ["-pa ebin deps/*/ebin apps/*/ebin"]),
  true = rpc:call(Node, navel, start, [?MODULE, PortInc]),
  navel:connect(Host, PortInc),
  timer:sleep(500),
  {ok, Pid} = rpc:call(Node, gen_server, start_link, [?MODULE, {}, []]),
  NNode =
    rpc:call(Node, navel, get_node, []),
  {ok, {NNode, Pid}}.

create_link({LNode,LPid},Name, HostPort) ->
  navel:call(LNode, gen_server, call, [LPid, {create, Name, HostPort}]).

connect({LNode,LPid}, Node, IpPort) ->
  navel:call(LNode, gen_server, call, [LPid, {connect, Node, IpPort}]),
  timer:sleep(500).

%% private

-record(state, {nodes = []}).

init(_Args) ->
  {ok, #state{}}.

handle_call({create, Name, {Host, PortInc}}, _From, S=#state{nodes = Nodes}) ->
  {ok, Node} = slave:start_link(Host, Name, ["-pa ebin deps/*/ebin apps/*/ebin"]),
  true = rpc:call(Node, navel, start, [Name, PortInc]),
  NNode = Name,% rpc:call(Node, navel, get_node, []),
  {reply, {ok, NNode}, S#state{nodes = [{NNode,Node}|Nodes]}};

handle_call({connect, NNode, {Ip,Port}}, _From, S=#state{nodes = Nodes}) ->
  case proplists:get_value(NNode, Nodes) of
    undefined ->
      {reply, wrong_nnode, S};
    Node ->
      rpc:call(Node, navel, connect, [Ip,Port]),
      {reply, ok, S}
  end.

  
