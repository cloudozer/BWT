%%------------------------------------------------------------------------------
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
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

-export([read/2]).


read(FileName,NumChunks)->
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
    ReadArray = make_pread_array(NumChunks,0,NumChunks,ChunkSize,ChunkRem,[]),
    {ok,Data} = file:pread(FileDev,ReadArray),
    Data.


make_pread_array(0,ThisOffset,NumChunks,ChunkSize,ChunkRem,Acc) ->
    lists:reverse([{ThisOffset,ChunkRem}|Acc]);
make_pread_array(ThisChunk,ThisOffset,NumChunks,ChunkSize,ChunkRem,Acc)->
    make_pread_array(ThisChunk-1,ThisOffset+ChunkSize,NumChunks,ChunkSize,ChunkRem,[{ThisOffset,ChunkSize}|Acc]).
