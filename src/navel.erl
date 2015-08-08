-module(navel).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-define(CHUNK_SIZE, 4000).

-define(RESTART_AFTER, 1000).

-record(nv, {node,hood =[]}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start/1]).
-export([connect/1]).
-export([get_node/0]).
-export([call/4,call_no_return/4]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start(Node) ->
	spawn(fun() -> print_stats() end),
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Node], []).

print_stats() ->
	%ling:experimental(gc, 1),
	timer:sleep(1000),
	print_stats().

connect(Domid) ->
	Postman = spawn(fun() -> {ok,T} = tube:open(Domid),
							 nice_postman(T) end),
	door_bell(Postman, true).

door_bell(Postman) -> door_bell(Postman, false).
door_bell(Postman, Intro) ->
	gen_server:call(?SERVER, {door_bell,Postman,Intro}).

introduce(Homie, Name) ->
	gen_server:call(?SERVER, {introduce,Homie,Name}).

substitute(A, B) ->
	gen_server:call(?SERVER, {substitute,A,B}).

signoff(Homie) ->
	gen_server:call(?SERVER, {signoff,Homie}).

get_node() -> gen_server:call(?SERVER, get_node).

call(Node, M, F, As) ->
	{ok,Homie} = gen_server:call(?SERVER, {expose,Node}),
	Homie ! {self(),{call,M,F,As}},
	receive {returns,Returns} -> Returns end.

call_no_return(Node, M, F, As) ->
	{ok,Homie} = gen_server:call(?SERVER, {expose,Node}),
	Homie ! {self(),{call_no_return,M,F,As}},
	no_return.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([Node]) ->
	process_flag(trap_exit, true),

	Monitor =
	spawn_link(fun() -> process_flag(trap_exit, true),
						receive X -> io:format("MONITOR: ~p\n", [X]) end end),
	link(Monitor),

	_Acceptor = spawn_link(fun() -> acceptor() end),
	{ok,#nv{node =Node}}.

handle_call(get_node, _From, #nv{node =MyNode} =St) ->
	{reply,MyNode,St};

handle_call({door_bell,Homie,false}, _From, #nv{hood =Hood} =St) ->
	true = link(Homie),
    {reply,ok,St#nv{hood =[{noname,Homie}|Hood]}};

handle_call({door_bell,Homie,true}, From, #nv{hood =Hood} =St) ->
	true = link(Homie),
    {noreply,St#nv{hood =[{noname,Homie,From}|Hood]}};

handle_call({introduce,Homie,Name}, _From, #nv{hood =Hood} =St) ->
	case lists:keytake(Homie, 2, Hood) of
		{value,{noname,_},Hood1} -> Hood1;
		{value,{noname,_,Notify},Hood1} -> gen_server:reply(Notify, ok), Hood1 end,
	{reply,ok,St#nv{hood =[{Name,Homie}|Hood1]}};

handle_call({substitute,A,B}, _From, #nv{hood =Hood} =St) ->
	unlink(A), link(B),
	{value,{Name,_},Hood1} = lists:keytake(A, 2, Hood),
	{reply,ok,St#nv{hood =[{Name,B}|Hood1]}};

handle_call({signoff,Homie}, _From, #nv{hood =Hood} =St) ->
	{value,{Name,_},Hood1} = lists:keytake(Homie, 2, Hood),
	io:format("navel: connection to ~s lost\n", [Name]),
	{reply,ok,St#nv{hood =Hood1}};

handle_call({expose,Name}, _From, #nv{hood =Hood} =St) ->
	case lists:keyfind(Name, 1, Hood) of
		false -> {reply,{error,not_connected},St};
		{_,Homie} -> {reply,{ok,Homie},St} end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
	erlang:display({navel_info,_Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

acceptor() ->
	{ok,S} = tube:accept(),
	Postman = spawn(fun() -> nice_postman(S) end),
	true = port_connect(S, Postman),
	door_bell(Postman),
	acceptor().

nice_postman(S) ->
	MyNode = get_node(),
	ok = tube:can_send_async(S),
	Outgoing = message_chunks({'$iam',MyNode}),
	postman(S, Outgoing).

postman(S, Outgoing) -> postman(S, [], nocall, Outgoing, 0, 0).

rookie_postman(S, Chips, Call, Outgoing, CanSend) ->
	receive take_over -> ok end,
	postman(S, Chips, Call, Outgoing, CanSend, 0).

forward_mail(Addressee) ->
	receive X -> Addressee ! X, forward_mail(Addressee)
			after 0 -> done end.

postman(S, Chips, Call, Outgoing, CanSend, N) when N >= ?RESTART_AFTER ->
	Rookie = spawn(fun() -> rookie_postman(S, Chips, Call, Outgoing, CanSend) end),
	substitute(self(), Rookie),
	true = port_connect(S, Rookie),
	forward_mail(Rookie),
	unlink(S),
	%%io:format("navel: restarted ~w as ~w\n", [self(),Rookie]),
	Rookie ! take_over;

postman(S, Chips, Call, [X|Outgoing], CanSend, N) when CanSend > 0 ->
	true = port_command(S, X),
	if CanSend =:= 1 -> ok = tube:can_send_async(S);
				true -> ok end,
	postman(S, Chips, Call, Outgoing, CanSend-1, N);

postman(S, Chips, Call, Outgoing, CanSend, N) ->
	receive
		{From,{call,M,F,As}} ->
			true = Call =:= nocall,
			Chunks = message_chunks({'$call',M,F,As}),
			postman(S, Chips, From, q(Chunks, Outgoing), CanSend, N+1);
		{_From,{call_no_return,M,F,As}} ->
			Chunks = message_chunks({'$call_no_return',M,F,As}),
			postman(S, Chips, Call, q(Chunks, Outgoing), CanSend, N+1);

		{S,{data,<<0,Chip/binary>>}} ->
			postman(S, [Chip|Chips], Call, Outgoing, CanSend, N);
		{S,{data,<<255,Chip/binary>>}} ->
			X = list_to_binary(lists:reverse([Chip|Chips])),
			action(S, binary_to_term(X), Call, Outgoing, CanSend, N);

		{can_send,S,NumSlots} ->
			postman(S, Chips, Call, Outgoing, NumSlots, N) end.

q(More, Queue) -> lists:append(Queue, More).

action(S, {'$iam',RemoteName}, Call, Outgoing, CanSend, N) ->
	io:format("navel: connected to ~s\n", [RemoteName]),
	introduce(self(), RemoteName),
	postman(S, [], Call, Outgoing, CanSend, N+1);

action(S, {'$call',M,F,As}, Call, Outgoing, CanSend, N) ->
	Returns = (catch apply(M, F, As)),
	Chunks = message_chunks({'$returns',Returns}),
	postman(S, [], Call, q(Chunks, Outgoing), CanSend, N+1);

action(S, {'$call_no_return',M,F,As}, Call, Outgoing, CanSend, N) ->
	case (catch apply(M, F, As)) of
		{'EXIT',Reason} ->
			io:format("navel: exception caught while calling ~w:~w/~w: ~p\n",
					[M,F,length(As),Reason]);
		_ -> ok end,
	postman(S, [], Call, Outgoing, CanSend, N+1);

action(S, {'$returns',Returns}, Call, Outgoing, CanSend, N) when Call =/= nocall ->
	Call ! {returns,Returns},
	postman(S, [], nocall, Outgoing, CanSend, N+1).

%% Message encoding:
%%
%% <<0,Chunk1/binary>>
%% <<0,Chunk2/binary>>
%% ...
%% <<255,ChunkN/binary>>	last chunk
%%

message_chunks(Term) -> message_chunks(term_to_binary(Term), []).
message_chunks(Bin, Acc) when byte_size(Bin) =< ?CHUNK_SIZE ->
	lists:reverse([<<255,Bin/binary>>|Acc]);
message_chunks(<<Chunk:?CHUNK_SIZE/binary,Bin/binary>>, Acc) ->
	message_chunks(Bin, [<<0,Chunk/binary>>|Acc]).

