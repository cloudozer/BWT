-module(navel_sup).

-behaviour(supervisor).

%% API
-export([start_link/1, start_link/2]).

%% Supervisor callbacks
-export([init/1]).


%% ===================================================================
%% API functions
%% ===================================================================

start_link(Node) ->
    start_link(Node, 0).

start_link(Node, PortInc) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Node, PortInc]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(Args) ->
    {ok, { {one_for_one, 5, 10}, [
        {navel, {gen_server,start_link,[{local, navel}, navel, Args, []]}, permanent, 5000, worker, [navel]}
    ]} }.


