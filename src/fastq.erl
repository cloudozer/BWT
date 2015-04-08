-module(fastq).
-export([read_seq/1, read_seq/2, fold/3, size/1]).

size(FileName) ->
  fastq:fold(fun(_,Acc) -> 1+Acc end, 0, FileName).

read_seq(Dev) ->
  case file:read_line(Dev) of
    {ok, [$@ | SeqName]} ->
      {ok, SeqData} = file:read_line(Dev),
      _Delimiter = file:read_line(Dev),
      _Quality = file:read_line(Dev),
      Seq = {lists:droplast(SeqName),
             lists:droplast(SeqData)},
      {ok, Seq};
    {ok, _} -> 
      wrong_format;
    eof ->
      eof
  end.

read_seq(Dev, N) ->
  read_seq(Dev, N, []).

read_seq(_Dev, 0, Acc) ->
  {ok, Acc};
read_seq(Dev, N, Acc) ->
  case read_seq(Dev) of
    {ok, Seq} ->
      read_seq(Dev, N-1, [Seq|Acc]);
    eof -> 
      {eof, Acc};
    Err -> Err
  end.


fold(Fun, Acc, FileName) ->
  {ok, Dev} = file:open(FileName, [read, raw, read_ahead]), 
  Result = fold_inner(Dev, Fun, Acc),
  ok = file:close(Dev),
  Result.

fold_inner(Dev, Fun, Acc) ->
  case fastq:read_seq(Dev) of
    {ok, Seq} -> fold_inner(Dev, Fun, Fun(Seq, Acc));
    eof -> Acc
  end.
