% FM inde construction for the whole genome
% 
% Cloudozer(c), 2015
%

-module(fm_index).
-export([t/0,t/1,
		make_indices/1, make_indices/2,
		get_index/2]).

-define(OVERLAP,2). % an overlap of neibouring chunks of a chromosome
-define(FM_FOLDER,"fm_indices/").


t() ->
	Genome = "bwt_files/test_genome.fasta",
	make_indices(Genome,["2","4"]).

t(Ls) ->
	Genome = "bwt_files/test_genome.fasta",
	make_indices(Genome,Ls).

make_indices(ChromoLs) ->
	Genome = "bwt_files/human_g1k_v37_decoy.fasta",
	make_indices(Genome,ChromoLs).


make_indices(Genome_file,ChromoLs) ->
	%% open Genome file
	%% read Chromosome by chromosome, check if it is in the given ChromoLs.
	%% if it is in the list, chunk it and make index for each chunk
	{ok,Dev} = file:open(Genome_file,[read,raw,read_ahead,binary]),

	case find_chromosome(Dev,0) of
		eof -> 
			io:format("No chromosomes found~n"),
			file:close(Dev);

		{Chromo_name,N} -> 
			make_indices(Genome_file,ChromoLs,Dev,N,Chromo_name)
	end.

make_indices(Genome_file,ChromoLs,Dev,N,Chromo_name) ->
	case in_list(Chromo_name,ChromoLs) of
		true ->
			io:format("Making fm-index for chromosome ~s ...~n",[Chromo_name]),
			%% read lines until eof or next chromosome found
			case read_chromosome(Dev,0) of
				{eof,Chromo_len} -> 
					build_fm(Genome_file,N,Chromo_len,Chromo_name);
				{Next_chromo,Chromo_len,L} ->
					build_fm(Genome_file,N,Chromo_len,Chromo_name),
					make_indices(Genome_file,ChromoLs,Dev,N+Chromo_len+L,Next_chromo)
			end;
		false ->
			case find_chromosome(Dev,N) of
				eof -> 
					io:format("No more chromosomes found~n"),
					file:close(Dev);

				{Next_chromo_name,N1} -> 
					make_indices(Genome_file,ChromoLs,Dev,N1,Next_chromo_name)
			end
	end.


in_list(_,all) -> true;
in_list(Name,Ls) -> lists:member(Name,Ls).


find_chromosome(Dev,N) ->
	case file:read_line(Dev) of
		eof -> eof;
		{ok,<<$>,Data/binary>>} -> 
			L = size(Data)+1, 
			[Name|_] = string:tokens(binary_to_list(Data)," \n"), 
			{Name,N+L};
		{ok,Data} -> find_chromosome(Dev,size(Data)+N)
	end.


read_chromosome(Dev,N) -> 
	case file:read_line(Dev) of
		eof -> {eof,N};
		{ok,<<$>,Data/binary>>} -> 
			L = size(Data)+1, 
			[Name|_] = string:tokens(binary_to_list(Data)," \n"), 
			{Name,N,L};
		{ok,Data} -> read_chromosome(Dev,N+size(Data))
	end.

			
build_fm(Genome_file,N,Chromo_len,Chromo_name) -> 
	{ok,Dev1} = file:open(Genome_file,[read]),
	statistics(wall_clock),
	
	{ok,Chromo} = file:pread(Dev1, N, Chromo_len),
	file:close(Dev1),
	{_,T0} = statistics(wall_clock),
	io:format("Chromosome has been read for ~p sec~n",[T0/1000]),
	
	{_,Chromo1} = ltrim_N(lists:reverse(Chromo)),
	{_,T1} = statistics(wall_clock),
	io:format("Ns trimmed on the right for ~p sec~n",[T1/1000]),
	{Shift,Chromo2} = ltrim_N(lists:reverse(Chromo1)),
	{_,T2} = statistics(wall_clock),
	io:format("Ns trimmed on the left for ~p sec~n",[T2/1000]),
	
	Body = lists:filter(fun(10)->false;(13)->false;(_)->true end, Chromo2),
	{_,T3} = statistics(wall_clock),
	io:format("CR and new_line symbols removed for ~p sec~n",[T3/1000]),
	
	Nbr = 1 + (( length(Body)+?OVERLAP)  bsr 24),  % number of chunks
	io:format("Chromo has ~p bp and is divided into ~p chunks~n",[length(Body),Nbr]),
	
	build_fm(Body,Shift,Nbr,1,Chromo_name).

build_fm(_,_,Nbr,J,Chromo_name) when J == Nbr+1 -> 
	io:format("All fm-indices for chromosome ~s are built~n",[Chromo_name]);
build_fm(Body,Shift,Nbr,J,Chromo_name) ->
	Chunk_size = (length(Body) + Nbr*?OVERLAP) div Nbr,
	Start = 1+(J-1)*(Chunk_size-?OVERLAP),
	make_index(lists:sublist(Body,Start,Chunk_size),Shift+Start,J,Chromo_name),
	build_fm(Body,Shift,Nbr,J+1,Chromo_name).


make_index(Chunk,Shift,J,Chromo_name) ->

	F_name = fm_name(Chromo_name,J),
	ok = file:write_file(F_name++".ref",list_to_binary(Chunk)),
	Ref_seq = lists:map( fun($a)->$A;($c)->$C;($g)->$G;($t)->$T;
							(N) when $N == N; $n == N->element(random:uniform(4),{$A,$C,$G,$T});
							($B)->$C;($D)->$G;($R)->$A;
							($Y)->$C;($K)->$T;($M)->$A;
							($S)->$C;($W)->$A;($V)->$A;
							($A)->$A;($C)->$C;($G)->$G;($T)->$T;
							(Err)->io:format("Unknown symbol ~p~n", [Err]), error(Err)
						end, Chunk),
	FM = bwt:fm(st:append($$,Ref_seq)),
	Meta = [{pointers, fmi:get_index_pointers(FM)},{shift, Shift}],
	Bin = term_to_binary({Meta,FM}),
	file:write_file(F_name++".fm",Bin).



fm_name(Chrom,J) ->
	case application:get_env(fm_indices) of
		{ok,Path} ->
			filename:join(Path,Chrom)++"_p" ++ integer_to_list(J);
		undefined ->
			?FM_FOLDER++Chrom++"_p" ++ integer_to_list(J)
	end.


get_index(Chrom,J) ->
	{ok,Bin} = file:read_file(fm_name(Chrom,J)++".fm"),
	binary_to_term(Bin).



%get_ref(Chrom,Pos,Len) ->
%	{ok, BwtFiles} = application:get_env(bwt,bwt_files),
%	File = filename:join(BwtFiles, Chrom++".ref"),
%	{ok,Dev} = file:open(File,read),
%	{ok,Ref} = file:pread(Dev, Pos, Len),
%	ok = file:close(Dev),
%	binary_to_term(Ref).



	

ltrim_N(Ls) -> ltrim_N(Ls,0).

ltrim_N([$N|Ls],J) -> ltrim_N(Ls,J+1);
ltrim_N([10|Ls],J) -> ltrim_N(Ls,J);
ltrim_N([13|Ls],J) -> ltrim_N(Ls,J);
ltrim_N(Ls,J) -> {J,Ls}.

	
