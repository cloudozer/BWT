% A test-comparison of outputs of bwa-mem ans gen-da
% 
% Cloudozer(c), 2015
%

-module(diff).
-export([diff/3,t/0]).

-define(PRINT_LEN,80).



t() -> diff:diff("tests/21.mem.out","tests/21.out",9411193).


diff(File1,File2,Shift) ->
	Fastq = "bwt_files/SRR770176_1.fastq",

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
	%Ls = sets:to_list(Absent_in_2),
	%io:format("The reads not found by gen-da:~n"),
	%[io:format("~p\t~p~n",[Name,dict:fetch(Name,Data1)]) || Name <- Ls],
	Ls = sets:to_list(Absent_in_1),
	%io:format("The reads not found by bwa-mem:~n~p~n",[[{Name,dict:fetch(Name,Data2)} || Name<-Ls]]),
	
	%% find TOP-10 that were not found
	Sorted = lists:sort(fun({_,{_,_,A,_}},{_,{_,_,B,_}}) -> A>B 
						end,[ {Name,dict:fetch(Name,Data2)} || Name <- Ls]),

	io:format("~n  TOP 10 unmatched reads:~n"),
	print_top_N(Sorted,10,Fastq).




print_top_N(_,0,_) -> ok;
print_top_N([],_,_)-> ok;
print_top_N([{Name,{Pos,CIGAR,Score,Ref}}|Ls],N,Fastq) ->
	io:format("Read:~p, Score:~p, CIGAR:~p~n",[Name,Score,CIGAR]),
	Qseq = get_read(Name,Fastq),
	{Qcomb,Mcomb,Rcomb} = sw_pp:pp(Qseq,Ref),
	io:format("~p...~n",[lists:sublist(Qcomb,?PRINT_LEN)]),
	io:format("~p...~n",[lists:sublist(Mcomb,?PRINT_LEN)]),
	io:format("~p...~n~n",[lists:sublist(Rcomb,?PRINT_LEN)]),

	print_top_N(Ls,N-1,Fastq).



get_read(Name,File) ->
	{ok,Dev} = file:open(File,read),
	Qseq = find_read(Name,Dev),
	file:close(Dev),
	Qseq.
find_read(Name,Dev) ->
	case file:read_line(Dev) of
		{ok, [$@|Line]} -> 
			[Q_name|_] = string:tokens(Line," \t"),
			case Q_name == Name of
				true -> 
					{ok,Qseq} = file:read_line(Dev),
					lists:droplast(Qseq);
				_ -> find_read(Name,Dev)
			end;
		eof -> not_found;
		{ok,_} -> find_read(Name,Dev)
	end.


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
			read_data(Dev,Shift,dict:store(Name,{list_to_integer(Pos),
												CIGAR,
												list_to_integer(Score),
												Ref},Dict)); 
		eof ->
			file:close(Dev), 
			Dict
	end.

