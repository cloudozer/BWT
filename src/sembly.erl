% a module devoted to de novo assembly of genome
%
% (c) Cloudozer, 2015
%


-module(sembly).
-export([t/0]).


t() ->
	Chromo = "GL000207.1",
	t(Chromo,100).

t(Chromo, Len) ->
	Coverage = 7,
	{ok,Ref} = file:read_file("bwt_files/"++Chromo++".ref"),
	Size = size(Ref),
	Entire_seq = binary_to_list(Ref),
	Reads_nbr = round(Size/Len*Coverage),
	Sembly = assemble([ {lists:sublist(Entire_seq,J,100),J} || 
		J <-[ random:uniform(Size-Len+1) || _ <- lists:seq(1,Reads_nbr)]]),
	io:format("~p~n",[Sembly]).


assemble(Reads) ->
	queue(Reads).


queue(Reads) ->
	queue(Reads,[]).

queue([{Read,J}|Reads],Acc) -> queue(Reads,[{Read,J,extract_G(Read)}|Acc]);
queue([],Acc) -> Acc.


extract_G(Read) -> extract_G(Read,1,[]).

extract_G([$G|Read],Count,Acc) -> extract_G(Read,1,[Count|Acc]);
extract_G([_|Read],Count,Acc) -> extract_G(Read,Count+1,Acc);
extract_G([],Count,Acc) -> lists:reverse([Count|Acc]).








