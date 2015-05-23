% a module devoted to de novo assembly of genome
%
% (c) Cloudozer, 2015
%


-module(sembly).
-export([t/0]).

-define(WORKERS_NBR,2).
-define(X_TRIALS,1). 	%% number of times a read will be tried to attach to the graph



t() ->
	Chromo = "GL000207.1",
	t(Chromo,100).

t(Chromo, Len) ->
	Coverage = 7,
	{ok,Ref} = file:read_file("bwt_files/"++Chromo++".ref"),
	D = round(2*Len/Coverage),
	Entire_seq = binary_to_list(Ref),
	
	Reads = get_reads(Entire_seq,D,Len,0,[]),
	io:format("Reads: ~p~n",[Reads]),
	assemble(Reads),

	io:format("~p~n",[Reads]).


get_reads(Entire_seq,D,Len,_,Acc) when length(Entire_seq) =< Len+D -> 
	[{length(Entire_seq)-Len,lists:nthtail(length(Entire_seq)-Len,Entire_seq)}|Acc];
get_reads(Entire_seq,D,Len,J,Acc) ->
	D1 = random:uniform(D),
	get_reads(lists:nthtail(D1,Entire_seq),D,Len,J+D1,[{J+D1,lists:sublist(Entire_seq,D1+1,Len)}|Acc]).


assemble([R|Reads]) ->
	Pid = spawn(assembler,start_worker,[self(),R]),
	build_graph(queue:from_list(Reads),[Pid],length(Reads)*?X_TRIALS).
	

build_graph(Q,Workers,0) ->
	io:format("~p reads were not matched~n",[queue:len(Q)]),
	[ Pid ! get_graph || Pid <- Workers ],
	assemble_graph(length(Workers),[]);

build_graph(Q,Workers,N) ->
	case queue:out(Q) of
		{{value,Read},Q1} -> 
			build_graph(Q1,Workers,Read,N);
		{empty,_} -> 
			[ Pid ! {get_graph} || Pid <- Workers ],
			io:format("Assembling graph...~n~p~n",[Workers]),
			assemble_graph(length(Workers),[])
	end.
	
build_graph(Q,Workers,Read,N) -> 
	%% sends Read to all workers which then try to attach it to their sub-graphs
	lists:foreach(fun(Wpid) -> Wpid ! Read end, Workers),
	Res = [ receive Msg -> Msg end || _ <- Workers ],
	%io:format("Got messages from ~p workers: ~p~n",[length(Res),Res]),
	
	%% if Read attached at more than one subgraphs -> merge these sub_graphs
	%% if Read attached at no subgraphs -> spawn a new worker or put the Read back in the queue
	L = length(lists:filter(fun(not_attached)->false; (_)->true end,Res)),
	case L of 
		0 -> 	%% spawn a new assembler or put Read back to the queue
			Q1 = queue:in(Read,Q),
			build_graph(Q1,Workers,N-1);
		1 -> 
			io:format("read attached to the only subgraph~n"),
			build_graph(Q,Workers,N);
		2 -> 
			io:format("Merge subgraphs~n"),
			build_graph(Q,Workers,N)
	end.
	



%% collect all sub-graphs
assemble_graph(0,Acc) -> Acc;
assemble_graph(Workers_nbr,Acc) -> receive Graph -> assemble_graph(Workers_nbr-1, [Graph|Acc]) end.








