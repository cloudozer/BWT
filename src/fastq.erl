-module(fastq).

-export([read_seq/1, read_seqs/2, read_seq_pos/1, fold/3, size/1, get_value/2, reverse_complement/1]).

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
      read_seq_pos(Dev);
    eof ->
      eof
  end.

read_seq_pos(Dev) ->
  {ok, Pos} = file:position(Dev, cur),
  case file:read_line(Dev) of
    {ok, [$@ | _SeqName]} ->
      {ok, SeqData} = file:read_line(Dev),
      _Delimiter = file:read_line(Dev),
      _Quality = file:read_line(Dev),
      {ok, {Pos, lists:droplast(SeqData)}};
    {ok, _} ->
      read_seq_pos(Dev);
    eof ->
      eof
  end.

read_seqs(Dev, N) ->
  read_seqs(Dev, N, []).

read_seqs(_Dev, 0, Acc) ->
  {ok, Acc};
read_seqs(Dev, N, []) ->
  case read_seq(Dev) of
    {ok, Seq} ->
      read_seqs(Dev, N-1, [Seq]);
    eof ->
        eof;
    Err -> Err
  end;
read_seqs(Dev, N, Acc) ->
  case read_seq(Dev) of
    {ok, Seq} ->
      read_seqs(Dev, N-1, [Seq|Acc]);
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

get_value(SeqName, FileName) ->
  {ok, Dev} = file:open(FileName, [read, raw, read_ahead]),
  Result = get_value_inner(SeqName, Dev),
  ok = file:close(Dev),
  Result.

get_value_inner(SeqName, Dev) ->
  case read_seq(Dev) of
    {ok, {SeqName1, SeqValue}} when SeqName1 == SeqName ->
      SeqValue;
    {ok, _} ->
      get_value_inner(SeqName, Dev)
  end.


reverse_complement(Ls) -> rc(Ls,[]).

rc([$A|Ls],Acc) -> rc(Ls,[$T|Acc]);
rc([$T|Ls],Acc) -> rc(Ls,[$A|Acc]);
rc([$C|Ls],Acc) -> rc(Ls,[$G|Acc]);
rc([$G|Ls],Acc) -> rc(Ls,[$C|Acc]);
rc([$N|Ls],Acc) -> rc(Ls,[$N|Acc]);
rc([],Acc) -> Acc.


  
