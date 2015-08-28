-module(lingd).
-behaviour(gen_fsm).


-export([start_link/2, create/2, create/3, create/4, ling_up/2, destroy/1]).
-export([init/1, beam/3, ling/3]).
-export([ip/0, clean_slave_name/1]).

-record(state, {host = 'localhost', port, port_increment = 100, slave_opts = "-pa ebin deps/*/ebin apps/*/ebin -setcookie secret -rsh ssh", instances = [], ip_inc = 2}).


%% lingd API

start_link(ling, Host) ->
  %{ok, Pid} = gen_fsm:start_link(?MODULE, {{xen,ling},#state{}}, []),
  State = #state{host = Host},
  {ok, Node} = slave:start_link(Host, ?MODULE, [State#state.slave_opts]),
  % ok = rpc:call(Node, application, start, [sasl]),
  PortInc = State#state.port_increment,
  {ok,_} = rpc:call(Node, navel, start, [?MODULE, PortInc]),
timer:sleep(1000),
  ok = navel:connect({State#state.host, PortInc}),
timer:sleep(2000),
  {ok, Pid} = navel:call(?MODULE, gen_fsm, start_link, [{local, ?MODULE}, ?MODULE, {ling,#state{port_increment = PortInc + 1, port = PortInc}}, []]),
  Node1 = navel:call(?MODULE, navel, get_node, []),
  {ok, {Node1,Pid}};

start_link(beam, Host) ->
  State = #state{host = Host},
  {ok, Node} = slave:start_link(Host, ?MODULE, [State#state.slave_opts]),
  PortInc = State#state.port_increment,
  {ok,_} = rpc:call(Node, navel, start, [?MODULE, PortInc]),
  ok = navel:connect({State#state.host, PortInc}),
timer:sleep(1000),
  {ok, Pid} = navel:call(?MODULE, gen_fsm, start_link, [?MODULE,  {beam,State#state{port_increment = PortInc + 1}}, []]),
  NNode = navel:call(?MODULE, navel, get_node, []),
  {ok, {NNode, Pid}}.

create({LNode,LPid},Name) ->
  create({LNode,LPid},Name,[]).

create({LNode,LPid},Name,Opts) ->
  Name1 = clean_slave_name(Name),
  {ok, Host} = navel:call(LNode, gen_fsm, sync_send_event, [LPid, {create, Name1, Opts}, 30000]),
log:info("Instance ~p created.", [Name1]),
  ok = navel:connect(Host),
  {ok, Host}.

create({LNode,LPid},Host,Name,Opts) ->
  Name1 = clean_slave_name(Name),
  {ok, Host1} = navel:call(LNode, gen_fsm, sync_send_event, [LPid, {create, Host, Name1, Opts}, 30000]),
log:info("Remote instance ~p created.", [Name1]),
  ok = navel:connect(Host1),
  {ok, Host1}.

ling_up(CallerBin, Host) ->
  gen_fsm:sync_send_event(?MODULE, {ling_up, CallerBin, Host}).

destroy({LNode,LPid}) ->
  navel:call(LNode, gen_fsm, sync_send_event, [LPid, destroy, 30000]).

%% TODO: Delete me
ip() ->
    {ok,Ifaddrs} = inet:getifaddrs(),
    case [ X || {If,Props} =X <- Ifaddrs, If =/= "lo", lists:keymember(addr, 1, Props) ] of
	[] -> unassigned;
	[{_,Props}|_] -> proplists:get_value(addr, Props) end.

clean_slave_name(Name) ->
  list_to_atom(lists:filter(fun($.) -> false;(_) -> true end, atom_to_list(Name))).

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

beam({create, Host, Name, _Opts}, _From, S=#state{port_increment = PortInc}) ->
  %% TODO: move it somewhere
  {ok,Cwd} = file:get_cwd(),
  PA = lists:foldl(fun(Path,Acc) -> filename:join(Cwd,Path) ++ " " ++ Acc end, "", ["ebin", "deps/*/ebin", "apps/*/ebin"]),
  SlaveOpts = ["-pa " ++ PA ++ " -setcookie secret"],
  {ok, Node} = slave:start_link(Host, Name, SlaveOpts),
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

ling({create, Host, Name, Opts}, From, S = #state{instances = Instances, ip_inc = IpInc}) ->
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
