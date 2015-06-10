-module(master_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, dev/0]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

dev() ->
	lager:start(),
	ok = application:start(master).

start(_StartType, _StartArgs) ->
    master_sup:start_link().

stop(_State) ->
    ok.
