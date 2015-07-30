-module(lingd).
-behaviour(gen_fsm).


-export([start_link/1, create/2, create/3, ling_up/2, destroy/1]).
-export([connect/0]).
-export([init/1, beam/3, ling/3]).
-export([ip/0]).

-record(state, {host = '127.0.0.1', port, port_increment = 10, slave_opts = "-pa ebin deps/*/ebin apps/*/ebin", instances = [], ip_inc = 2}).


%% lingd API

start_link(ling) ->
  %{ok, Pid} = gen_fsm:start_link(?MODULE, {{xen,ling},#state{}}, []),
  State = #state{},
  {ok, Node} = slave:start_link(State#state.host, ?MODULE, [State#state.slave_opts]),
  % ok = rpc:call(Node, application, start, [sasl]),
  PortInc = State#state.port_increment,
  {ok,_} = rpc:call(Node, navel, start, [?MODULE, PortInc]),
timer:sleep(1000),
  ok = navel:connect({State#state.host, PortInc}),
timer:sleep(2000),
  {ok, Pid} = navel:call(?MODULE, gen_fsm, start_link, [{local, ?MODULE}, ?MODULE, {ling,#state{port_increment = PortInc + 1, port = PortInc}}, []]),
  Node1 = navel:call(?MODULE, navel, get_node, []),
  {ok, {Node1,Pid}};
start_link(beam) ->
  State = #state{},
  {ok, Node} = slave:start_link(State#state.host, ?MODULE, [State#state.slave_opts]),
  PortInc = State#state.port_increment,
  {ok,_} = rpc:call(Node, navel, start, [?MODULE, PortInc]),
  ok = navel:connect({State#state.host, PortInc}),
timer:sleep(1000),
  {ok, Pid} = navel:call(?MODULE, gen_fsm, start_link, [?MODULE,  {beam,#state{port_increment = PortInc + 1}}, []]),
  NNode = navel:call(?MODULE, navel, get_node, []),
  {ok, {NNode, Pid}}.

create({LNode,LPid},Name) ->
  create({LNode,LPid},Name,[]).

create({LNode,LPid},Name,Opts) ->
  {ok, Host} = navel:call(LNode, gen_fsm, sync_send_event, [LPid, {create, Name, Opts}, 30000]),
log:info("Instance ~p created.", [Name]),
  ok = navel:connect(Host),
  {ok, Host}.

ling_up(CallerBin, Host) ->
  gen_fsm:sync_send_event(?MODULE, {ling_up, CallerBin, Host}).

destroy({LNode,LPid}) ->
  navel:call(LNode, gen_fsm, sync_send_event, [LPid, destroy, 30000]).

connect() ->
  navel:connect({'127.0.0.1', 10}).

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

init({VM, State}) when ling == VM; beam == VM ->
  {ok, VM, State}.

beam({create, Name, _Opts}, _From, S=#state{host = Host, port_increment = PortInc}) ->
  {ok, Node} = slave:start_link(Host, Name, [S#state.slave_opts]),
  {ok,_} = rpc:call(Node, navel, start, [Name, PortInc]),
  {reply, {ok, {Host,PortInc}}, beam, S#state{port_increment = PortInc + 1}};

beam(destroy, _From, S) ->
  %% TODO implement me
  {reply, ok, beam, S}.

ling({create, Name, Opts}, From, S = #state{instances = Instances, ip_inc = IpInc}) ->
  Host = {10,0,0,1},
  NameBin = list_to_binary(atom_to_list(Name)),
  Extra = list_to_binary(io_lib:format("-ipaddr 10.0.0.~b -netmask 255.255.255.0 -gateway 10.0.0.1 -home /BWT -pz /BWT/ebin -eval 'ok = application:start(sasl), navel:start(~w), ok = navel:connect({~w,~w}), timer:sleep(2000), ok = navel:call(~w, lingd, ling_up, [~w,lingd:ip()]).'", [IpInc, Name, Host, S#state.port, ?MODULE, term_to_binary(From)])),
%log:info("ling create ~p", [Extra]),
  egator:create(NameBin, <<"/home/yatagan/BWT/BWT.img">>, [{memory, proplists:get_value(memory, Opts, 512)},{extra, Extra}], []),
  {next_state, ling, S#state{instances = [NameBin | Instances], ip_inc = IpInc + 1}};

ling({ling_up, CallerBin, Host}, _From, S) ->
  Caller = binary_to_term(CallerBin),
  gen_fsm:reply(Caller, {ok, Host}),
  {reply, ok, ling, S};

ling(destroy, _From, S = #state{instances = Instances}) ->
  lists:foreach(fun(Ins) -> log:info("destroying ~p...", [Ins]), timer:sleep(5000), egator:destroy(Ins, []), log:info("done") end, Instances),
  {reply, ok, ling, S}.
