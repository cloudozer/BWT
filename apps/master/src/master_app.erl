-module(master_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, dev/0]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

-define(SSH_PWD, "1").

dev() ->
	lager:start(),

	case application:get_env(kernel, ssh_port) of
		undefined -> ok;
		{ok,SshPort} ->
			application:ensure_all_started(ssh),
			PrivDir = code:priv_dir(master),
			SshDir = filename:join(PrivDir, "ssh_dir"),
			io:format("Starting ssh daemon on port ~w\n", [SshPort]),
			ssh:daemon(SshPort, [{password,?SSH_PWD},
								 {system_dir,SshDir},
								 {user_dir,SshDir}]) end,

	master:start_link().
	%% ok = application:start(master).

start(_StartType, _StartArgs) ->
    master_sup:start_link().

stop(_State) ->
    ok.
