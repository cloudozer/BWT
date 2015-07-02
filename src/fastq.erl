-module(fastq).

-export([read_seq/1, read_seqs/2, reverse_complement/1]).

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
      read_seq(Dev);
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


reverse_complement(Ls) -> rc(Ls,[]).

rc([$A|Ls],Acc) -> rc(Ls,[$T|Acc]);
rc([$T|Ls],Acc) -> rc(Ls,[$A|Acc]);
rc([$C|Ls],Acc) -> rc(Ls,[$G|Acc]);
rc([$G|Ls],Acc) -> rc(Ls,[$C|Acc]);
rc([$N|Ls],Acc) -> rc(Ls,[$N|Acc]);
rc([],Acc) -> Acc.


  
