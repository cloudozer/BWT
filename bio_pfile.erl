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

-export([read/2,read/3,find_pattern/2,spawn_find_pattern/2,search_chunk/3]).


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
    NewSize == Size,
    {ok,FileDev} = file:open(FileName,[read,binary]),
    ReadArray = make_pread_array(NumChunks,0,NumChunks,ChunkSize,ChunkRem,Overlap,[]),
    {ok,Data} = file:pread(FileDev,ReadArray),
    Chunks = lists:zip(ReadArray,Data).

spawn_find_pattern(Chunks,BinPattern)->
    spawn_find_pattern(Chunks,length(Chunks),BinPattern).

spawn_find_pattern([],NumChunks,BinPattern)->
    listen_find_pattern(0,NumChunks,[]);
spawn_find_pattern([ThisChunk|Chunks],NumChunks,BinPattern) ->
    spawn(?MODULE,search_chunk,[self(),ThisChunk,BinPattern]),
    spawn_find_pattern(Chunks,NumChunks,BinPattern).

listen_find_pattern(CurChunks,NumChunks,Acc) when CurChunks == NumChunks ->
    Acc;
listen_find_pattern(CurChunks,NumChunks,Acc) ->
    receive
	Chunk ->
	    NewAcc = Acc ++ Chunk,
	    listen_find_pattern(CurChunks+1,NumChunks,NewAcc)
    end.

    
    

search_chunk(Pid,{{Offset,_Size},BinInput},BinPattern)->
    {Num,Positions} = find_pattern(BinInput,BinPattern),
    Results = lists:map(fun({Start,Length})->
				{Start+Offset,Length} end,Positions),
    Pid ! Results.
							   
    

    

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
