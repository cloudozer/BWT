-module(source_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, dev/0]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

-define(SSH_PWD, "1").

dev() ->
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
	%% ok = application:start(source).

start(_StartType, _StartArgs) ->
    source_sup:start_link().

stop(_State) ->
    ok.
