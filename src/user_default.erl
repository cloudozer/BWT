-module(user_default).
-compile(export_all).

%s() -> navel:start(master).
%c() -> navel:start0(worker), navel:connect({127,0,0,1}).

s() -> master_app:dev().
c() -> application:start(bwt), application:start(worker_bwt_app).
r() -> gen_server:call(master , {run, "bwt_files/SRR770176_1.fastq", "GL000193.1", 1}).

