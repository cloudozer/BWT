-module(worker_bwt_app_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
	navel:start0(erl1),
	navel:connect({127,0,0,1}),
    {ok, { {one_for_one, 5, 10}, [

        {worker_bwt, {worker_bwt, start_link, [{master,master}]}, permanent, 5000, worker, [worker_bwt]}

    ]} }.

