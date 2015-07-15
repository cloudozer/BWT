-module(lingd).
-behaviour(gen_fsm).


-export([start_link/0, start_link/1, create/2, ling_up/2]).
-export([init/1, slave/3, ling/3]).
-export([ip/0]).

-record(state, {host = '127.0.0.1', port, port_increment = 10, slave_opts = "-pa ebin deps/*/ebin apps/*/ebin"}).


%% lingd API

start_link() ->
  OsType = erlang:system_info(os_type),
  start_link(OsType).

start_link({xen,ling}) ->
  %{ok, Pid} = gen_fsm:start_link(?MODULE, {{xen,ling},#state{}}, []),
  State = #state{},
  {ok, Node} = slave:start_link(State#state.host, ?MODULE, [State#state.slave_opts]),
  ok = rpc:call(Node, application, start, [sasl]),
  PortInc = State#state.port_increment,
log:info("starting navel ~p", [[?MODULE, PortInc]]),
  {ok,_} = rpc:call(Node, navel, start, [?MODULE, PortInc]),
timer:sleep(3000),
log:info("connecting..."),
  ok = navel:connect(State#state.host, PortInc),
log:info("connected?"),
timer:sleep(1000),
  {ok, Pid} = navel:call(?MODULE, gen_fsm, start_link, [{local, ?MODULE}, ?MODULE, {{xen,ling},#state{port_increment = PortInc + 1, port = PortInc}}, []]),
  Node1 = navel:call(?MODULE, navel, get_node, []),
  {ok, {Node1,Pid}};
start_link(OsType={_,_}) ->
  State = #state{},
  {ok, Node} = slave:start_link(State#state.host, ?MODULE, [State#state.slave_opts]),
  PortInc = State#state.port_increment,
  {ok,_} = rpc:call(Node, navel_sup, start_link, [?MODULE, PortInc]),
  ok = navel:connect(State#state.host, PortInc),
timer:sleep(1000),
  {ok, Pid} = navel:call(?MODULE, gen_fsm, start_link, [?MODULE,  {OsType,#state{port_increment = PortInc + 1}}, []]),
  NNode = ?MODULE, %%rpc:call(Node, navel, get_node, []),   % returns {badrpc,{'EXIT',{noproc,{gen_server,call,[navel,get_node]}}}
  {ok, {NNode, Pid}}.

create({LNode,LPid},Name) ->
log:info("create ~p", [{{LNode,LPid},Name}]),
  {ok, Host} = navel:call(LNode, gen_fsm, sync_send_event, [LPid, {create, Name}, infinity]),
log:info("created ~p", [Host]),
  ok = navel:connect(Host),
timer:sleep(1000),
  {ok, Host}.

ling_up(CallerBin, Host) ->
  log:info("lingd ling_up ~p", [{binary_to_term(CallerBin), Host}]),
  gen_fsm:sync_send_event(?MODULE, {ling_up, CallerBin, Host}).

%% Delete me
ip() ->
    {ok,Ifaddrs} = inet:getifaddrs(),
    case [ X || {If,Props} =X <- Ifaddrs, If =/= "lo", lists:keymember(addr, 1, Props) ] of
	[] -> unassigned;
	[{_,Props}|_] -> proplists:get_value(addr, Props) end.

%% broadcast(NodePids, Msg) ->
%%   lists:foreach(fun({Node,Pid}) ->
%%     navel:call_no_return(Node, erlang, send, [Pid,Msg])
%%   end, NodePids).

%% private

init({{xen,ling}, State}) ->
  {ok, ling, State};

init({{_,_}, State}) ->
  {ok, slave, State}.

slave({create, Name}, _From, S=#state{host = Host, port_increment = PortInc}) ->
  {ok, Node} = slave:start_link(Host, Name, [S#state.slave_opts]),
  {ok,_} = rpc:call(Node, navel, start, [Name, PortInc]),
  {reply, {ok, {Host,PortInc}}, slave, S#state{port_increment = PortInc + 1}}.

ling({create, Name}, From, S) ->
  Host = {192,168,56,200},
  NameBin = list_to_binary(atom_to_list(Name)),
  Extra = list_to_binary(io_lib:format("-dhcp -home /BWT -pz /BWT/ebin -eval 'ok = application:start(sasl), navel:start(~w), ok = navel:connect(~w,~w), timer:sleep(5000), ok = navel:call(~w, lingd, ling_up, [~w,lingd:ip()]).'", [Name, Host, S#state.port, ?MODULE, term_to_binary(From)])),
log:info("ling create ~p", [Extra]),
  egator:create(NameBin, <<"/home/yatagan/BWT/BWT.img">>, [{memory, 512},{extra, Extra}], []),
  {next_state, ling, S};

ling({ling_up, CallerBin, Host}, _From, S) ->
  Caller = binary_to_term(CallerBin),
  gen_fsm:reply(Caller, {ok, Host}),
  {reply, ok, ling, S}. 
