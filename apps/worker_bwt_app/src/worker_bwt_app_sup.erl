-module(worker_bwt_app_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args), {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
	MyDomid = xenstore:domid(),
	NodeName = list_to_atom("erl" ++ integer_to_list(MyDomid)),
	navel:start(NodeName),

	{ok,MasterDomid} = application:get_env(worker_bwt_app, master_domid),
	ok = navel:connect(MasterDomid),

    {ok, { {one_for_one, 5, 10}, [
        {worker_bwt, {worker_bwt, start_link, [{master,master}]}, permanent, 5000, worker, [worker_bwt]}
    ]} }.

ip() ->
    {ok,Ifaddrs} = inet:getifaddrs(),
    case [ X || {If,Props} =X <- Ifaddrs, If =/= "lo", lists:keymember(addr, 1, Props) ] of
	[] -> unassigned;
	[{_,Props}|_] -> proplists:get_value(addr, Props) end.
