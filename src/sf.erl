% seed finder module 
% 
% 
% Cloudozer(c), 2015
%


-module(sf).
-export([start_SF/1,
		seed_finder/3
		]).



% returns {Alqs, SFs} - list of Alq processes and list of SF processes
start_SF(Schedule) -> start_SF(Schedule,[],[]).

start_SF([{Box_id,Alq,Chunk_files}|Schedule],Alqs,SFs) -> 
	SFs1 = lists:foldl( fun(Chunk,Acc)-> 
			SF = spawn(?MODULE,seed_finder,[Chunk,Alq,self()]),
			[SF|Acc]
						end, SFs,Chunk_files),
	start_SF(Schedule,[Alq|Alqs],SFs1);

start_SF([],Alqs,SFs) -> {Alqs, SFs}.




seed_finder(Chunk,Alq,R_source) ->
	FM = http:get_fmindex(Chunk),
	seed_finder(Chunk,Alq,R_source,FM).

seed_finder(Chunk,Alq,R_source,FM) ->
	R_source ! {self(),ready},
	receive
		quit -> ok;
		{data,Batch} ->
			% find seeds functions
			Alq ! {Chunk,[1,2,3,4,5]}, % sends seeds
			seed_finder(Chunk,Alq,R_source,FM)
	end.


	