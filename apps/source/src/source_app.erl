-module(source_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, dev/0]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

dev() ->
	ok = application:start(source).

start(_StartType, _StartArgs) ->
    source_sup:start_link().

stop(_State) ->
    ok.
