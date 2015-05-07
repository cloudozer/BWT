-module(worker_bwt_app_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, dev/0]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

dev() ->
    lager:start(),
    application:start(bwt),
    application:start(worker_bwt_app).

start(_StartType, _StartArgs) ->
    timer:sleep(5000),
    worker_bwt_app_sup:start_link().

stop(_State) ->
    ok.
