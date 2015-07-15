-module(navel).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-define(PROXY_TCP_PORT, 9223).
-define(SOCK_OPTS, [{active,false},{reuseaddr,true},binary]).
-define(CHUNK_SIZE, 1440).

-record(nv, {node,hood =[]}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start/1,start0/1,start/2]).
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

start(Node) -> start(Node, 0).

start(Node, PortInc) ->
	spawn(fun() -> print_stats() end),
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Node,PortInc], []).

print_stats() ->
	%ling:experimental(gc, 1),
	timer:sleep(1000),
	print_stats().

start0(Node) -> start(Node).	%%OBSOLETE

connect(Addr) ->
	{ok,S} = gen_tcp:connect(Addr, ?PROXY_TCP_PORT, ?SOCK_OPTS),
	Postman = spawn(fun() -> nice_postman(S) end),
	door_bell(Postman).

door_bell(Postman) ->
	gen_server:call(?SERVER, {door_bell,Postman}).

introduce(Homie, Name) ->
	gen_server:call(?SERVER, {introduce,Homie,Name}).

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

init([Node,PortInc]) ->
	process_flag(trap_exit, true),
	{ok,L} = gen_tcp:listen(?PROXY_TCP_PORT+PortInc, ?SOCK_OPTS),

	%%Monitor =
	%%spawn_link(fun() -> process_flag(trap_exit, true),
	%%					receive X -> io:format("MONITOR: ~p\n", [X]) end end),
	%%link(Monitor),

	_Acceptor = spawn_link(fun() -> acceptor(L) end),
	{ok,#nv{node =Node}}.

handle_call(get_node, _From, #nv{node =MyNode} =St) ->
	{reply,MyNode,St};

handle_call({door_bell,Homie}, _From, #nv{hood =Hood} =St) ->
	true = link(Homie),
    {reply,ok,St#nv{hood =[{noname,Homie}|Hood]}};

handle_call({introduce,Homie,Name}, _From, #nv{hood =Hood} =St) ->
	{value,{noname,_},Hood1} = lists:keytake(Homie, 2, Hood),
	{reply,ok,St#nv{hood =[{Name,Homie}|Hood1]}};

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

acceptor(L) ->
	{ok,S} = gen_tcp:accept(L),
	Postman = spawn(fun() -> nice_postman(S) end),
	door_bell(Postman),
	acceptor(L).

nice_postman(S) ->
	MyNode = get_node(),
	send_message(S, {'$iam',MyNode}),
	postman(S).

postman(S) ->
	case receive_message(S, 0) of
		{ok,{'$iam',RemoteName}} ->
			{ok,{{A,B,C,D},P}} = inet:peername(S),
			io:format("navel: connected to ~w at ~w.~w.~w.~w:~w\n", [RemoteName,A,B,C,D,P]),
			introduce(self(), RemoteName),
			postman(S);
		{ok,{'$call',M,F,As}} ->
			Returns = (catch apply(M, F, As)),
			send_message(S, {'$returns',Returns}),
			postman(S);
		{ok,{'$call_no_return',M,F,As}} ->
			case (catch apply(M, F, As)) of
				{'EXIT',Reason} ->
					io:format("navel: exception caught while calling ~w:~w/~w: ~p\n",
							[M,F,length(As),Reason]);
				_ -> ok end,
			postman(S);
		undefined -> sort_mail(S) end.

sort_mail(S) ->
	receive
		{From,{call,M,F,As}} ->
			ok = send_message(S, {'$call',M,F,As}),
			{ok,{'$returns',Returns}} = receive_message(S),
			From ! {returns,Returns},
			sort_mail(S);
		{_From,{call_no_return,M,F,As}} ->
			ok = send_message(S, {'$call_no_return',M,F,As}),
			sort_mail(S)
		after 0 -> postman(S) end.


%% Message encoding:
%%
%%	<<MsgSz:32,PadSz:16,Padding:PadSz,Payload>>
%%
%% MsgSz 	- the size of the message without the MsgSz field
%% PadSz	- the size of the Padding field
%% Payload	- an Erlang term encoded using term_to_binary()
%%
%% The purpose of the padding is to align the message to CHUNK_SIZE
%%

receive_message(Sock) -> receive_message(Sock, infinity).
receive_message(Sock, Timeout) ->
	case gen_tcp:recv(Sock, 4, Timeout) of
		{ok,<<MsgSz:32>>} ->
			{ok,Chunks} = chunks(Sock, MsgSz),
			<<PadSz:16,_Padding:PadSz/binary,Payload/binary>> = list_to_binary(Chunks),
			Msg = binary_to_term(Payload),
			%%io:format("navel: receive ~P\n", [Msg,12]),
			{ok,Msg};
		{error,timeout} -> undefined end.

chunks(Sock, Sz) -> chunks(Sock, Sz, []).
chunks(_Sock, 0, Acc) -> {ok,lists:reverse(Acc)};
chunks(Sock, Sz, Acc) ->
	Sz1 = if Sz > ?CHUNK_SIZE -> ?CHUNK_SIZE; true -> Sz end,
	{ok,Bin} = gen_tcp:recv(Sock, Sz1),
	chunks(Sock, Sz - Sz1, [Bin|Acc]).

send_message(Sock, Term) ->
	%%io:format("navel: send ~P\n", [Term,12]),
	Payload = term_to_binary(Term),
	NumChunks = (4 + 2 + byte_size(Payload) + ?CHUNK_SIZE-1) div ?CHUNK_SIZE,
	MsgSz = NumChunks * ?CHUNK_SIZE - 4,
	PadSz = MsgSz - 2 - byte_size(Payload),
	Pkt = <<MsgSz:32,PadSz:16,0:PadSz/unit:8,Payload/binary>>,
	ok = gen_tcp:send(Sock, Pkt).

