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
    {ok,InFile} = file:open("newtes",[binary,read]),
    Size = file_size("newtes"),
    InBin = read_exts(InFile,<<>>,Size).

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
