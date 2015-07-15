% CIGAR maker module 
% 
% 
% Cloudozer(c), 2015
%

-module(cm).
-export([start_cigar_makers/2,
		cigar_maker/2
		]).


start_cigar_makers(N,Sink) ->
	lists:foreach(  fun(_) -> spawn(?MODULE, cigar_maker,[self(),Sink])
					end,lists:seq(1,N)).



cigar_maker(Alq, Sink) ->
	Alq ! {self(),ready},
	receive
		quit -> ok;
		{_Ref,Read,Chunk,Pos,_D} -> 
			io:format("CM: got read. Aligned. Sent to Sink~n"),
			% run SW and send results to Sink
			{Score,CIGAR} = {250, "151M"},

			Sink ! {Read,Chunk,Pos,Score,CIGAR},
			spawn(?MODULE,cigar_maker,[Alq,Sink])
	end.
