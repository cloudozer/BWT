-module(navel).
-export([start/1]).
-export([start0/1]).
-export([connect/1]).
-export([call/4]).
-export([get_node/0]).

-define(PROXY_TCP_PORT, 9223).
-define(SOCK_OPTS, [{active,true},{packet,4},{reuseaddr,true},binary]).

start(Node) ->
	spawn(fun() -> {ok,L} = gen_tcp:listen(?PROXY_TCP_PORT, ?SOCK_OPTS),
				   acceptor(L) end),
	start0(Node).

start0(Node) -> register(?MODULE, spawn(fun() -> navel(Node, [], []) end)).

connect(IpAddr) ->
	{ok,S} = gen_tcp:connect(IpAddr, ?PROXY_TCP_PORT, ?SOCK_OPTS),
	ok = gen_tcp:controlling_process(S, whereis(navel)),
	navel ! {connected,S}.

call(Node, M, F, As) ->
	Ref = make_ref(),
	navel ! {self(),Ref,{call,Node,M,F,As}},
	receive {Ref,X} -> X end.

get_node() ->
	Ref = make_ref(),
	navel ! {self(),Ref,get_node},
	receive {Ref,X} -> X end.

acceptor(L) ->
	{ok,S} = gen_tcp:accept(L),
	ok = gen_tcp:controlling_process(S, whereis(navel)),
	navel ! {connected,S},
	acceptor(L).

navel(Node, Peers, Calls) ->
	receive
		{From,Ref,get_node} ->
			From ! {Ref,Node},
			navel(Node, Peers, Calls);

		{connected,S} ->
			dispatch(S, {'$iam',Node}),
			navel(Node, Peers, Calls);

		{From,Ref,{call,RN,M,F,As}}  ->
			case lists:keyfind(RN, 1, Peers) of
				{_,S} ->
					dispatch(S, {Ref,{'$call',M,F,As}}),
					navel(Node, Peers, [{From,Ref}|Calls]);
				false ->
					io:format("navel: ~w not found (call to ~w:~w/~w dropped\n", [RN,M,F,length(As)]),
					navel(Node, Peers, Calls) end;
		
		{tcp,S,<<_:32,Pkt/binary>>} ->
			case binary_to_term(Pkt) of
				{'$iam',RN} ->
					false = lists:keymember(S, 2, Peers),
					{ok,{{A,B,C,D},P}} = inet:peername(S),
					io:format("navel: connected to ~w at ~w.~w.~w.~w:~w\n", [RN,A,B,C,D,P]),
					navel(Node, [{RN,S}|Peers], Calls);
				{Ref,{'$call',M,F,As}} ->
					Returns = (catch apply(M, F, As)),
					%io:format("navel: call to ~w:~w/~w returns ~P\n", [M,F,length(As),Returns,12]),
					dispatch(S, {Ref,{'$returns',Returns}}),
					navel(Node, Peers, Calls);
				{Ref,{'$returns',Returns}} ->
					{value,{From,_},Calls1} = lists:keytake(Ref, 2, Calls),
					From ! {Ref,Returns},
					navel(Node, Peers, Calls1) end;

		X -> throw({todo,X}) end.

dispatch(S, Msg) ->
	Pkt = term_to_binary(Msg),
	Sz = byte_size(Pkt),
	gen_tcp:send(S, <<Sz:32,Pkt/binary>>).

