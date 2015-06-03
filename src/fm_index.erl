% FM inde construction for the whole genome
% 
% Cloudozer(c), 2015
%

-module(fm_index).
-export([t/0,
		make_indices/1, make_indices/2]).

-define(OVERLAP,200). % an overlap of neibouring chunks of a chromosome


t() ->
	Genome = "bwt_files/test_genome.fasta",
	make_indices(Genome,["2","4"]).

make_indices(ChromoLs) ->
	Genome = "bwt_files/human_g1k_v37_decoy.fasta",
	make_indices(Genome,ChromoLs).


make_indices(Genome_file,ChromoLs) ->
	%% open Genome file
	%% read Chromosome by chromosome, check if it is in the given ChromoLs.
	%% if it is in the list, chunk it and make index for each chunk
	{ok,Dev} = file:open(Genome_file,[read,raw,{read_ahead,1024}]),

	case find_chromosome(Dev,0) of
		eof -> 
			io:format("No chromosomes found~n"),
			file:close(Dev);

		{Chromo_name,N} -> 
			make_indices(Dev,N,ChromoLs,Chromo_name)
	end.

make_indices(Dev,N,ChromoLs,Chromo_name) ->
	case in_list(Chromo_name,ChromoLs) of
		true ->
			io:format("Making fm-index for chromosome ~p ...~n",[Chromo_name]),
			%% read lines until eof or next chromosome found
			case read_chromosome(Dev,0) of
				{eof,Chromo_len} -> 
					build_fm(Dev,N,Chromo_len);
				{Next_chromo,Chromo_len,L} ->
					build_fm(Dev,N,Chromo_len),
					make_indices(Dev,N+Chromo_len+L,ChromoLs,Next_chromo)
			end;
		false ->
			case find_chromosome(Dev,N) of
				eof -> 
					io:format("No more chromosomes found~n"),
					file:close(Dev);

				{Next_chromo_name,N1} -> 
					make_indices(Dev,N1,ChromoLs,Next_chromo_name)
			end
	end.


in_list(_,all) -> true;
in_list(Name,Ls) -> lists:member(Name,Ls).


find_chromosome(Dev,N) ->
	case file:read_line(Dev) of
		eof -> eof;
		{ok,[$>|Data]} -> L = length(Data)+1, [Name|_] = string:tokens(Data," \n"), {Name,N+L};
		{ok,Data} -> find_chromosome(Dev,length(Data)+N)
	end.


read_chromosome(Dev,N) -> 
	case file:read_line(Dev) of
		eof -> {eof,N};
		{ok,[$>|Data]} -> L = length(Data)+1, [Name|_] = string:tokens(Data," \n"), {Name,N,L};
		{ok,Data} -> read_chromosome(Dev,N+length(Data))
	end.
			
build_fm(Dev,N,Chromo_len) -> ok.


	