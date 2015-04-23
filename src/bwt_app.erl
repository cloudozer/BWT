-module(bwt_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    timer:sleep(5000),
    bwt_sup:start_link().

stop(_State) ->
    ok.
