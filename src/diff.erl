% A test-comparison of outputs of bwa-mem ans gen-da
% 
% Cloudozer(c), 2015
%

-module(diff).
-export([diff/3,t/0]).


t() -> diff:diff("tests/21.mem.out","tests/21.out",9411193).


diff(File1,File2,Shift) ->
	Data1 = read_data_sam(File1),
	Data2 = read_data(File2,Shift),
	Set1 = sets:from_list(dict:fetch_keys(Data1)),
	Set2 = sets:from_list(dict:fetch_keys(Data2)),
	SetU = sets:union([Set1,Set2]),
	Absent_in_1 = sets:subtract(Set2,Set1),
	Absent_in_2 = sets:subtract(Set1,Set2),
	io:format("There are ~p unique reads found by bwa-mem and gen-da~n",[sets:size(SetU)]),
	io:format("bwa-mem matched ~p reads which gen-da did not~n",[sets:size(Absent_in_2)]),
	io:format("gen-da matched ~p reads which bwa-mem did not~n",[sets:size(Absent_in_1)]),
	Ls = sets:to_list(Absent_in_2),
	io:format("The reads not found by gen-da:~n"),
	[io:format("~p\t~p~n",[Name,dict:fetch(Name,Data1)]) || Name <- Ls],
	{Data1,Data2}.
	





read_data_sam(File) ->
	{ok,Dev} = file:open(File,read),
	read_data_sam(Dev,dict:new()).

read_data_sam(Dev,Dict) ->
	case file:read_line(Dev) of
		{ok, Line} -> 
			[Name,_,_,Pos,_,CIGAR,_,_,_,Ref|_] = string:tokens(Line," \t"),
			read_data_sam(Dev,dict:store(Name,{list_to_integer(Pos),CIGAR,Ref},Dict) ); 
		eof ->
			file:close(Dev), 
			Dict
	end.

read_data(File,Shift) ->
	{ok,Dev} = file:open(File,read),
	read_data(Dev,Shift,dict:new()).

read_data(Dev,Shift,Dict) ->
	case file:read_line(Dev) of
		{ok, Line} -> 
			[Name,_,_,Pos,CIGAR,Score,Ref] = string:tokens(Line," \t"),
			%[_,Q_id] = string:tokens(Name,"."),
			read_data(Dev,Shift,dict:store(Name,{list_to_integer(Pos)+Shift,CIGAR,Score,Ref},Dict)); 
		eof ->
			file:close(Dev), 
			Dict
	end.

