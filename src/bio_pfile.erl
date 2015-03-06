%%------------------------------------------------------------------------------
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file exc%% Attach point and data distribution and control hub for
%% GUI clients. Receives data from the backend
%% and pushes the data to the GUI clients.ept in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%%-----------------------------------------------------------------------------
%%
%%
%% (c) Infoblox, 2014
%%

-module(bio_pfile).

-compile(export_all).

-export([read/2,read/3,find_pattern/2,spawn_find_pattern/3,search_chunk/5]).

-define(INIT_READ_CHUNKS,10).
-define(EXT_SIZE,10000000).

t1()->
    parse("data/human_g1k_v37_decoy.fasta").

t2(OutBin)->
    BinSize = byte_size(OutBin),
    NumExts = trunc(BinSize/?EXT_SIZE),
    ExtRem = BinSize rem ?EXT_SIZE,
    ExtArray = make_pread_array(NumExts,0,NumExts,?EXT_SIZE,ExtRem,0,[]),
    {ok,OutFile} = file:open("newtes",[write]),
    write_exts(OutFile,OutBin,ExtArray).

t3()->
    Out = t1(),
    OutBin = term_to_binary(Out),
    t2(OutBin).

t4()->
    bin_term_file_read("data/human_g1k_v37_decoy.foam").
    

bin_term_file_read(FileName)->
    {ok,InFile} = file:open(FileName,[binary,read]),
    Size = file_size(FileName),
    InBin = read_exts(InFile,<<>>,Size),
    binary_to_term(InBin).

read_exts(InFile,Acc,0)->
    file:close(InFile),
    Acc;
read_exts(InFile,Acc,NumToRead)->
    case NumToRead > ?EXT_SIZE of
	true ->
	    {ok, BinData} = file:read(InFile,?EXT_SIZE),
	    NewAcc = <<Acc/binary,BinData/binary>>,
	    read_exts(InFile,NewAcc,NumToRead-?EXT_SIZE);
	false ->
	    {ok, BinData} = file:read(InFile,NumToRead),
	    NewAcc = <<Acc/binary,BinData/binary>>,
	    read_exts(InFile,NewAcc,0)
    end.

write_exts(OutFile,_OutBin,[])->
    file:sync(OutFile),
    file:close(OutFile),
    ok;
write_exts(OutFile,OutBin,[Part|Rest])->
    ExtBin = binary:part(OutBin,Part),
    file:write(OutFile,ExtBin),
    write_exts(OutFile,OutBin,Rest).

file_size(FileName)->
    {ok,{file_info,Size,Type,Access,
	 ATime,
	 MTime,
	 CTime,
	 Mode,Links,MajorDev,MinorDev,INode,UID,GID}} = file:read_file_info(FileName),
    Size.
    
    

parse(FileName)->
    Data = bio_pfile:read(FileName,?INIT_READ_CHUNKS),
    {ok,{file_info,Size,Type,Access,
	 ATime,
	 MTime,
	 CTime,
	 Mode,Links,MajorDev,MinorDev,INode,UID,GID}} = file:read_file_info(FileName),
    A = lists:sort(bio_pfile:spawn_find_pattern(Data,<<">">>,[])),
    N = bio_pfile:make_genome_index_pread(A,Size,[]),
    {ok,FileDev} = file:open("data/human_g1k_v37_decoy.fasta",[read,binary]),
    {ok,Results} = file:pread(FileDev,N),
    %%lists:map(fun(X)->[Index,Seq] = binary:split(X,<<"\n">>),{Index,binary:replace(Seq,<<"\n">>,<<>>,[global])} end,Results),
    MyPid = self(),
    lists:foreach(fun(Result)->
			  spawn(fun()->[Index,Seq] = binary:split(Result,<<"\n">>),
				       MyPid ! [{Index,remove_pattern(Seq,<<>>,<<"\n">>)}] end) end,Results),
    listen_gather(0,length(Results),[]).

listen_gather(CurChunks,NumChunks,Acc) when CurChunks == NumChunks ->
    Acc;
listen_gather(CurChunks,NumChunks,Acc) ->
    receive
	Chunk ->
	    NewAcc = Chunk ++ Acc,
	    listen_gather(CurChunks+1,NumChunks,NewAcc)
    end.
    

			

t3(Try)->
    {ok,FileDev} = file:open("data/human_g1k_v37_decoy.fasta",[read,binary]),
    file:pread(FileDev,[Try]).

remove_pattern(<<>>,Acc,_BinPat) ->
    Acc;
remove_pattern(Bin,Acc,BinPat)->
    <<Byte:1/binary,Rest/binary>> = Bin,
    case Byte == BinPat of
	true -> remove_pattern(Rest,Acc,BinPat);
	false -> remove_pattern(Rest,<<Acc/binary,Byte/binary>>,BinPat)
    end.
	    



quick_read(FileDev,[],Acc)->
    Acc;
quick_read(FileDev,[Head|Rest],Acc) ->
    io:format("Reading: ~p~n",[Head]),
    {ok,Result} = file:pread(FileDev,[Head]),
    io:format("Result: ~p~n",[Result]),
    quick_read(FileDev,Rest,Acc ++ [Result]).

    


read(FileName,NumChunks)->
    read(FileName,NumChunks,0).

read(FileName,NumChunks,Overlap)->
    {ok,{file_info,Size,Type,Access,
	 ATime,
	 MTime,
	 CTime,
	 Mode,Links,MajorDev,MinorDev,INode,UID,GID}} = file:read_file_info(FileName),
    ChunkSize = trunc(Size / NumChunks),
    ChunkRem = Size rem ChunkSize,
    NewSize = (NumChunks * ChunkSize) + ChunkRem,
    NewSize = Size,
    {ok,FileDev} = file:open(FileName,[read,binary]),
    ReadArray = make_pread_array(NumChunks,0,NumChunks,ChunkSize,ChunkRem,Overlap,[]),
    {ok,Data} = file:pread(FileDev,ReadArray),
    file:close(FileDev),
    Chunks = lists:zip(ReadArray,Data).

spawn_find_pattern(Chunks,BinPattern,Options)->
    Results = spawn_find_pattern(Chunks,length(Chunks),BinPattern,Options),
    lists:sort(sets:to_list(sets:from_list(Results))).

spawn_find_pattern([],NumChunks,BinPattern,Options)->
    listen_find_pattern(0,NumChunks,[]);
spawn_find_pattern([ThisChunk|Chunks],NumChunks,BinPattern,Options) ->
    spawn(?MODULE,search_chunk,[self(),NumChunks-length(Chunks),ThisChunk,BinPattern,Options]),
    spawn_find_pattern(Chunks,NumChunks,BinPattern,Options).

listen_find_pattern(CurChunks,NumChunks,Acc) when CurChunks == NumChunks ->
    Acc;
listen_find_pattern(CurChunks,NumChunks,Acc) ->
    receive
	Chunk ->
	    NewAcc = Chunk ++ Acc,
	    listen_find_pattern(CurChunks+1,NumChunks,NewAcc)
    end.


make_genome_index_pread(Index,TotalSize,Acc) when length(Index) < 2 ->
    [Cur] = Index,
    NewAcc = Acc ++ [{Cur,TotalSize-Cur}],
    NewAcc;
make_genome_index_pread(Index,TotalSize,Acc)->
    [Cur|Rest] = Index,
    [Next|Rest2] = Rest,
    NewAcc = Acc ++ [{Cur,Next-Cur}],
    make_genome_index_pread(Rest,TotalSize,NewAcc).

bwt(BinSeq)->
    TBinSeq = case binary:match(BinSeq,<<"$">>) of
		  nomatch ->  <<BinSeq/binary,<<"$">>/binary>>;
		  _ -> BinSeq
	      end,
    Size = byte_size(TBinSeq),
    M = list_tab(TBinSeq,0,Size-1,[TBinSeq]),
    {F,L} = lists:foldl(fun(X,Acc)->
				{First,Last} = bin_get_fl(X),
				{FCol,LCol} = Acc,
				{<<FCol/binary,First/binary>>,<<LCol/binary,Last/binary>>}
			end,{<<>>,<<>>},M),
    L.

inv_bwt(L)->
    List = binary:bin_to_list(L),
    S = lists:sort(List),
    F = binary:list_to_bin(S),
    <<Start:1/binary,Rest/binary>> = L,
    {Inv,L2F} = walk_fl({<<"$">>,1},F,L,{<<>>,array:new()},byte_size(L)),
    {Inv,F,L,L2F}.

bwt_match_naive(BinSub,BinPat)->
    {_Inv,F,L,L2F} = inv_bwt(bwt(BinSub)),
    io:format("F = ~p, L = ~p, L2F = ~p~n",[F,L,L2F]),
    {Top,Bottom} = bwt_match_naive(F,L,BinPat,L2F,0,byte_size(F)-1),
    Bottom-Top+1.

bwt_match_naive(_F,_L,<<>>,_L2F,Top,Bottom) ->
    {Top,Bottom};
bwt_match_naive(F,L,BinPat,L2F,Top,Bottom) ->
    io:format("Top = ~p, Bottom = ~p~n",[Top,Bottom]),
    HeadSize = byte_size(BinPat)-1,
    <<Head:HeadSize/binary,Sym:1/binary>> = BinPat,
    io:format("Sym = ~p~n",[Sym]),
    case find_sym(L,Sym,Top,Top,Bottom,{undef,undef}) of
	{undef,_} -> nomatch;
	{TopIndex,BottomIndex} ->
	    NewTop = lists:nth(TopIndex+1,L2F),
	    NewBottom = lists:nth(BottomIndex+1,L2F),
	    io:format("NewTop = ~p, NewBottom = ~p~n",[NewTop,NewBottom]),
	    bwt_match_naive(F,L,Head,L2F,NewTop,NewBottom)
    end.
						   

find_sym(Col,Sym,Start,Cur,End,Acc) when Cur > End ->
    io:format("Acc = ~p~n",[Acc]),
    Acc;
find_sym(Col,Sym,Start,Cur,End,{Top,Bottom}) ->
    io:format("~p,~p,~p,~p,~p,~p,~p~n",[Col,Sym,Start,Cur,End,Top,Bottom]),
    <<Head:Cur/binary,CurBin:1/binary,Rest/binary>> = Col,
    io:format("CurBin = ~p, Sym = ~p~n",[CurBin,Sym]),
    case CurBin == Sym of
	true ->
	    case Top of
		undef -> find_sym(Col,Sym,Start,Cur+1,End,{Cur,Cur});
		_ -> find_sym(Col,Sym,Start,Cur+1,End,{Top,Cur})
	    end;
	false -> 
	    find_sym(Col,Sym,Start,Cur+1,End,{Top,Bottom})
    end.

		    
	    
    

				      
    
    


    
    


    
walk_fl(_,_,_,{BinAcc,FLAcc} = Acc,Size) when byte_size(BinAcc) == Size ->
    {BinAcc,array:to_list(FLAcc)};
walk_fl({Pat,Num}=Target,F,L,{BinAcc,FLAcc} = Acc,Size)->
    %%io:format("~p,~p,~p,~p,~p~n",[Target,F,L,Acc,Size]),
    Row = find_row(L,Pat,Num),
    %%io:format("Row = ~p~n",[Row]),
    {NewPat,NewNum} = find_pat(F,Row),
    %%io:format("{NewPat,NewNum} = {~p,~p}~n",[NewPat,NewNum]),
    NewAcc = {<<BinAcc/binary,NewPat/binary>>,array:set(Row,find_row(F,Pat,Num),FLAcc)},
    walk_fl({NewPat,NewNum},F,L,NewAcc,Size). 

find_pat(Col,Row)->   
    find_pat(Col,Row,0,<<>>,0).

find_pat(Col,Row,CurRow,CurPat,Count)->
    <<Start:1/binary,Rest/binary>> = Col,
    NewCount = case Start == CurPat of
		   true ->
		       Count+1;
		   false ->
		       1
	       end,
    case Row == CurRow of
	true ->
	    {Start,NewCount};
	false ->
	    find_pat(Rest,Row,CurRow+1,Start,NewCount)
    end.
	


find_row(Col,Pat,Num)->
    find_row(Col,Pat,Num,0,1).

find_row(Col,Pat,Num,Pos,CurNum)->
    CurPat = binary:part(Col,{Pos,1}),
    case {Pat,Num} == {CurPat,CurNum} of
	true ->
	    Pos;
	false ->	
	    case Pat == CurPat of
		true ->
		    find_row(Col,Pat,Num,Pos+1,CurNum+1);
		false -> 
		    find_row(Col,Pat,Num,Pos+1,CurNum)
	    end
    end.
    
    

 
    
list_tab(TBinSeq,Count,NewSize,Acc) when Count > NewSize-1 ->
    lists:sort(Acc);
list_tab(TBinSeq,Count,NewSize,Acc) when is_list(Acc) ->
    NewSeq = one_byte_ror(TBinSeq,NewSize),
    NewAcc = Acc ++ [NewSeq],
    list_tab(NewSeq,Count+1,NewSize,NewAcc).

bin_tab(TBinSeq,Count,NewSize,Acc) when is_binary(Acc) ->
    NewSeq = one_byte_ror(TBinSeq,NewSize),
    NewAcc = <<Acc/binary,TBinSeq/binary>>,
    bin_tab(NewSeq,Count+1,NewSize,NewAcc).

bin_get_fl(Bin) ->
    Size = byte_size(Bin),
    MSize = Size - 2,
    <<First:1/binary,Middle:MSize/binary,Last:1/binary>> = Bin,
    {First,Last}.




tab(_Bin,Acc,0)->
    Acc;
tab(Bin,Acc,Size)->
    FullSize = byte_size(Bin),
    NewBin = one_byte_rol(Bin),
    NewAcc = <<Acc/binary,NewBin/binary>>,
    tab( NewBin, NewAcc, Size-1 ).
    
    
    


bit_rol(Bin,Shift) -> 
    <<U:Shift/bits,Rest/bits>> = Bin, <<Rest/bits,U:Shift/bits>>.    


one_byte_rol( <<U:1/binary,Rest/binary>> = Bin) -> 
    <<Rest/binary,U:1/binary>>.

one_byte_ror( Bin, NewSize) ->
    <<Rest:NewSize/binary,U:1/binary>> = Bin,
    <<U:1/binary,Rest/binary>>.
    

    
    

search_chunk(Pid,ChunkNum,{{Offset,_Size},BinInput},BinPattern,Options)->
    case Options of
	[overlaping] ->
	    {Num,Positions} = find_pattern(BinInput,BinPattern),
	    Results = lists:map(fun({Start,Length})->
			      {ChunkNum,Start,Length} end,Positions),
	    Pid ! Results;
	[{replace,BinReplacement}] ->
	    NewBin = binary:replace(BinInput,BinPattern,BinReplacement,[global]),
	    {{ChunkNum,byte_size(NewBin)},NewBin};
	_ ->  Positions = binary:matches(BinInput,BinPattern),
	      Results = lists:map(fun({Start,Length})->
					  Start+Offset end,Positions),
	      Pid ! Results
    end.

							   
    
    

make_pread_array(0,ThisOffset,NumChunks,ChunkSize,0,Overlap,Acc) ->
    lists:reverse(Acc);
make_pread_array(0,ThisOffset,NumChunks,ChunkSize,ChunkRem,Overlap,Acc) ->
    lists:reverse([{ThisOffset-Overlap,ChunkRem+Overlap}|Acc]);
make_pread_array(ThisChunk,0,NumChunks,ChunkSize,ChunkRem,Overlap,Acc)->
    make_pread_array(ThisChunk-1,ChunkSize,NumChunks,ChunkSize,ChunkRem,Overlap,[{0,ChunkSize}|Acc]);
make_pread_array(ThisChunk,ThisOffset,NumChunks,ChunkSize,ChunkRem,Overlap,Acc)->
    make_pread_array(ThisChunk-1,ThisOffset+ChunkSize,NumChunks,ChunkSize,ChunkRem,Overlap,[{ThisOffset-Overlap,ChunkSize+Overlap}|Acc]).


find_pattern(BinInput,BinPattern) when byte_size(BinInput) >= byte_size(BinPattern) ->
    InLen = byte_size(BinInput),
    PatLen = byte_size(BinPattern),
    StopPos = InLen - PatLen,
    StartPos = 0,
    bin_pfind(BinInput,BinPattern,PatLen,StartPos,StopPos - StartPos,0,[]).

bin_pfind(BinIn,BinPat,PatLen,CurPos,-1,Count,Acc)->
    {Count,Acc};
bin_pfind(BinIn,BinPat,PatLen,CurPos,Left,Count,Acc) ->
    InPart = binary:part(BinIn,CurPos,PatLen),
    case binary:match(InPart,BinPat) of
	nomatch ->
	    bin_pfind(BinIn,BinPat,PatLen,CurPos+1,Left-1,Count,Acc);
	{MPos,MLen} -> 	    
	    bin_pfind(BinIn,BinPat,PatLen,CurPos+1,Left-1,Count+1,Acc ++ [{MPos+CurPos,MLen}])
    end.



find_header(Chunk,Pos)->
    binary:match(Chunk,<<">">>,[{Pos,byte_size(Chunk)}]).


bin_to_ets(Bin)->
    E = ets:new('genes',[{read_concurrency,true}]),
    bin_to_ets(Bin,E,0).
    
bin_to_ets(<<>>,Ets,_Count)->
    Ets;
bin_to_ets(Bin,Ets,Count)->
    <<Byte:1/binary,Rest/binary>> = Bin,
    ets:insert(Ets,{Count,Byte}),
    bin_to_ets(Rest,Ets,Count+1).

count_ets(Bin,BinChar,Ets)->
    count_ets(Bin,BinChar,0,0,Ets,byte_size(Bin)).
    
count_ets(_Bin,_BinChar,Pos,_Count,Ets,Size) when Pos >= Size ->
    Ets;
count_ets(Bin,BinChar,Pos,Count,Ets,Size) when Pos < Size ->
    <<Cur:1/binary,Rest/binary>> = Bin,
    case Cur == BinChar of
	true ->
	    ets:insert(Ets,{{Cur,Count},Pos}),
	    count_ets(Rest,BinChar,Pos+1,Count+1,Ets,Size);
	false ->
	    count_ets(Rest,BinChar,Pos+1,Count,Ets,Size)
    end.

count_array(Bin,BinChar,Array)->
    count_array(Bin,BinChar,0,0,Array,byte_size(Bin)).
    
count_array(_Bin,_BinChar,Pos,_Count,Array,Size) when Pos >= Size ->
    Array;
count_array(Bin,BinChar,Pos,Count,Array,Size) when Count < Size ->
    <<Cur:1/binary,Rest/binary>> = Bin,
    case Cur == BinChar of
	true ->
	    NewArray = array:set(Count,Pos,Array),
	    count_array(Bin,BinChar,Pos+1,Count+1,NewArray,Size);
	false ->
	    count_array(Bin,BinChar,Pos+1,Count,Array,Size)
    end.
	    

