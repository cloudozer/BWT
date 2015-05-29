% a module devoted to returning unique ids
%
% (c) Cloudozer, 2015
%


-module(uid).
-export([init/0, stop/1, next/1, uid/1]).
		

init() -> 
	io:format("Uid initiated~n"),	
	spawn(?MODULE,uid,[0]).


uid(J) ->
	receive
		{Pid,next_id} -> 
			%io:format("Got 'next' message from ~p~n",[Pid]), 
			Pid ! {next_id,J}, uid(J+1);
		stop -> 
			io:format("uid terminated normally~n")
	end. 


stop(Uid) -> Uid ! stop.


next(Uid) -> 
	Uid ! {self(),next_id},
	receive 
		{next_id,J} -> J	
	end.
