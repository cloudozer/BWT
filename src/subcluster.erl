-module(subcluster).

%% API
-export([]).
-compile([export_all]).


start(Boxes, Schedule, SeqFileName, HttpStorage, VM, Host) ->

  %% Start local navel
  navel:start(launcher),
  true = register(launcher, self()),

  %% Start lingd daemon
  {ok, {LingdNode, LingdPid}} = lingd:start_link(VM, Host),
  LingdRef = {LingdNode, LingdPid},
  LingdHost = {Host, 100},

  %% Reads source
  {source, SourceMem, {_, SourceHost, _}, Schedule1} = schedule:pop_subcluster_element(source, Schedule),
  {ok,SourceHost1} = lingd:create(LingdRef, SourceHost, source, [{memory,SourceMem}]),
  {ok, SourceRef={SNode, SPid}} = navel:call(source, rs, start, [SeqFileName, HttpStorage]),
  navel:connect(SourceHost1),

  %% Sink
  {sink, SinkMem, {_, SinkHost, _}, Schedule2} = schedule:pop_subcluster_element(sink, Schedule1),
  {ok,SinkHost1} = lingd:create(LingdRef, SinkHost, sink, [{memory,SinkMem}]),
  {ok, SinkRef} = navel:call(sink, sk, start, [SourceRef, length(Schedule)]),
  ok = navel:call(SNode, navel, connect, [SinkHost1]),

  %% Alqs
  AlqsBoxes = lists:map(fun({{BoxName, BoxHost, _BoxMem},_}) ->
    AlqName = list_to_atom("alq_" ++ atom_to_list(BoxName)),
    %% TODO: define it
    AlqMem = 2,
    {ok, _AlqHost} = lingd:create(LingdRef, BoxHost, AlqName, [{memory,AlqMem}]),
    ok = navel:call(AlqName, navel, connect, [LingdHost]),
    ok = navel:call(AlqName, navel, connect, [SourceHost1]),
    ok = navel:call(AlqName, navel, connect, [SinkHost1]),
    {ok,AlqRef} = navel:call(AlqName, alq, start, [SinkRef,SinkHost1,LingdRef]),
    {BoxName, AlqRef}
  end, Schedule2),
  Alqs = lists:map(fun({_Box,Alq}) -> Alq end, AlqsBoxes),

  %% Seed finders
  Schedule3 = lists:foldl(fun({Box, List}, Acc) -> lists:map(fun({Name, Mem}) -> {Box, Name, Mem} end, List) ++ Acc end, [], Schedule2),
  SFs = lists:map(fun({{BoxName, BoxHost, _BoxMem}, Chunk, Mem}) ->
    AlqRef={ANode,_} = proplists:get_value(BoxName, AlqsBoxes),
    SfName = list_to_atom(atom_to_list(Chunk) ++ "_" ++ atom_to_list(BoxName)),
    {ok, SfHost} = lingd:create(LingdRef, BoxHost, SfName, [{memory,Mem}]),
    ok = navel:call(SNode, navel, connect, [SfHost]),
    ok = navel:call(ANode, navel, connect, [SfHost]),
    {ok, SFRef} = navel:call(lingd:clean_slave_name(SfName), sf, start, [Chunk,AlqRef,SourceRef,HttpStorage]),
    SFRef
  end, Schedule3),

  %% Run
  no_return = navel:call_no_return(SNode, erlang, send, [SPid, {run, Alqs, SFs, SinkRef}]),
  ok.

