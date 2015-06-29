-module(io_embedded).
-author("yatagan").

%% API
-export([start_link/1]).
-export([loop/1]).

-record(state, {bucket_file, pos = 0, buffer = [], read_size = 100}).

start_link(BucketFile) ->
spawn_link(?MODULE, loop, [#state{bucket_file = BucketFile}]).

%% Private

loop(S = #state{buffer = [], bucket_file = {Bucket, FileName}, pos = Pos, read_size = ReadSize}) ->
  Bin = binary:embedded_part(Bucket, FileName, Pos, ReadSize),
  Buffer = binary:split(Bin, <<$\n>>, [global]),
  loop(S#state{buffer = Buffer, pos = Pos + ReadSize});
loop(S = #state{buffer = [Line | BufferRest], pos = Pos}) ->
  receive
    {io_request, From, ReplyAs, {get_line, ''}} ->
      reply(From, ReplyAs, binary_to_list(Line)),
      loop(S#state{buffer = BufferRest, pos = Pos + byte_size(Line) + byte_size(<<$\n>>)});
    {file_request,From, ReplyAs,{position,cur}} ->
      reply(From, ReplyAs, {ok, Pos}),
      loop(S);
    NotImpl ->
      throw({not_implemented, NotImpl})
  end.

reply(From, ReplyAs, Reply) ->
  From ! {io_reply, ReplyAs, Reply}.