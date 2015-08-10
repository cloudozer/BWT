-module(navel).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-define(PROXY_TCP_PORT, 9223).
-define(SOCK_OPTS, [{active,true},{reuseaddr,true},binary]).
-define(CHUNK_SIZE, 1440).

-define(RESTART_AFTER, 1000).

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

connect({Addr, PortInc}) ->
	{ok,S} = gen_tcp:connect(Addr, ?PROXY_TCP_PORT+PortInc, ?SOCK_OPTS),
	Postman = spawn(fun() -> nice_postman(S) end),
	ok = gen_tcp:controlling_process(S, Postman),
	door_bell(Postman, true);
connect(Addr) ->
	connect({Addr, 0}).

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

init([Node,PortInc]) ->
	%process_flag(trap_exit, true),
	{ok,L} = gen_tcp:listen(?PROXY_TCP_PORT+PortInc, ?SOCK_OPTS),

	%%Monitor =
	%%spawn_link(fun() -> process_flag(trap_exit, true),
	%%					receive X -> io:format("MONITOR: ~p\n", [X]) end end),
	%%link(Monitor),

	Acceptor = spawn_link(fun() -> acceptor(L) end),
	ok = gen_tcp:controlling_process(L, Acceptor),
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

acceptor(L) ->
	{ok,S} = gen_tcp:accept(L),
	Postman = spawn(fun() -> nice_postman(S) end),
	ok = gen_tcp:controlling_process(S, Postman),
	door_bell(Postman),
	acceptor(L).

nice_postman(S) ->
	MyNode = get_node(),
	send_message(S, {'$iam',MyNode}),
	postman(S).

postman(S) -> postman(S, 0, 0, [], nocall, 0).

rookie_postman(S, ExpSz, IncSz, Chips, Call) ->
	receive take_over -> ok end,
	postman(S, ExpSz, IncSz, Chips, Call, 0).

forward_mail(Addressee) ->
	receive X -> Addressee ! X, forward_mail(Addressee)
			after 0 -> done end.

postman(S, ExpSz, IncSz, Chips, Call, N) when N >= ?RESTART_AFTER ->
	Rookie = spawn(fun() -> rookie_postman(S, ExpSz, IncSz, Chips, Call) end),
	substitute(self(), Rookie),
	ok = gen_tcp:controlling_process(S, Rookie),
	forward_mail(Rookie),
	%%io:format("navel: restarted ~w as ~w\n", [self(),Rookie]),
	Rookie ! take_over;

postman(S, ExpSz, IncSz, Chips, Call, N) ->
	receive
		{From,{call,M,F,As}} ->
			true = Call =:= nocall,
			ok = send_message(S, {'$call',M,F,As}),
			postman(S, ExpSz, IncSz, Chips, From, N+1);
		{_From,{call_no_return,M,F,As}} ->
			ok = send_message(S, {'$call_no_return',M,F,As}),
			postman(S, ExpSz, IncSz, Chips, Call, N+1);
		{tcp,S,Chip} ->
			{ExpSz1,IncSz1,Chips1,Call1} = incoming(S, Chip, ExpSz, IncSz, Chips, Call),
			postman(S, ExpSz1, IncSz1, Chips1, Call1, N+1);
		{tcp_closed,S} -> signoff(self()) end.

incoming(S, Chip, 0, _IncSz, Chips, Call) ->
	case merge(Chip, Chips) of
		<<ExpSz:32,Data/binary>> -> incoming(S, Data, ExpSz, 0, [], Call);
		Bin -> {0,byte_size(Bin),[Bin],Call} end;

incoming(S, Chip, ExpSz, IncSz, Chips, Call) when IncSz+byte_size(Chip) >= ExpSz ->
	<<MsgBin:ExpSz/binary,More/binary>> = merge(Chip, Chips),
	<<PadSz:16,_:PadSz/binary,Payload/binary>> = MsgBin,
	Call1 = action(S, binary_to_term(Payload), Call),
	incoming(S, More, 0, 0, [], Call1);

incoming(_S, Chip, ExpSz, IncSz, Chips, Call) ->
	{ExpSz,IncSz+byte_size(Chip),[Chip|Chips],Call}.

merge(Chip, Chips) -> list_to_binary(lists:reverse([Chip|Chips])).

action(S, {'$iam',RemoteName}, Call) ->
	{ok,{{A,B,C,D},P}} = inet:peername(S),
	io:format("navel: connected to ~s at ~w.~w.~w.~w:~w \n", [RemoteName,A,B,C,D,P]),
	introduce(self(), RemoteName),
	Call;

action(S, {'$call',M,F,As}, Call) ->
	Returns = (catch apply(M, F, As)),
	send_message(S, {'$returns',Returns}),
	Call;

action(_S, {'$call_no_return',M,F,As}, Call) ->
	case (catch apply(M, F, As)) of
		{'EXIT',Reason} ->
			io:format("navel: exception caught while calling ~w:~w/~w: ~p\n",
					[M,F,length(As),Reason]), Call;
		_ -> Call end;

action(_S, {'$returns',Returns}, Call) ->
	true = Call =/= nocall,
	Call ! {returns,Returns},
	nocall.

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

send_message(Sock, Term) ->
	%%io:format("navel: send ~P\n", [Term,12]),
	Payload = term_to_binary(Term),
	NumChunks = (4 + 2 + byte_size(Payload) + ?CHUNK_SIZE-1) div ?CHUNK_SIZE,
	MsgSz = NumChunks * ?CHUNK_SIZE - 4,
	PadSz = MsgSz - 2 - byte_size(Payload),
	Pkt = <<MsgSz:32,PadSz:16,0:PadSz/unit:8,Payload/binary>>,
	ok = gen_tcp:send(Sock, Pkt).

