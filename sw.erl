% Smith-Waterman algo
% for finding best local matching between two sequences
%
% http://en.wikipedia.org/wiki/Smith%E2%80%93Waterman_algorithm
%

-module(sw).
-export([sw/2, sw/4,
		sw_multi/4,
		get_alignments/3,
		rand_seq/1]).

-define(MATCH,2).
-define(MISMATCH,-1).
-define(GAP_PENALTY,-1.5).
-define(UNDEF,0).

-define(THRESHOLD, 0.47).



rand_seq(N)->
	rand_seq([],N).
rand_seq(Acc,0) -> Acc;
rand_seq(Acc,N) -> rand_seq([lists:nth(random:uniform(4),[$A,$C,$G,$T])|Acc], N-1).



sw_multi(N,[_|_]=Seq,Ref_seq_name,File) -> 
	case get_reference_seq(Ref_seq_name,File) of
		error -> error;
		Ref_seq ->
			Overlap = round(length(Seq) / ?THRESHOLD),
			Chunk_size = Overlap*20,
			Collector_pid = spawn(?MODULE,get_alignments,[self(),[],0]),
			spawn_sw(Collector_pid,N,Seq,Ref_seq,Chunk_size,Overlap,0),
			receive
				{alignments,Res} ->
					io:format("~p alignments found~n",[length(Res)]),
					Res
			end
	end. 


get_alignments(Pid,Acc,Chunks_proc) -> 
	receive
		no_match -> get_alignments(Pid,Acc,Chunks_proc+1);
		{C1,Match,C2} -> get_alignments(Pid,[{C1,Match,C2}|Acc],Chunks_proc+1);
		{chunks_nbr, Chunks_nbr} -> 
			io:format("~p chunks remained~n",[Chunks_nbr-Chunks_proc]),
			get_rest_alignments(Pid,Acc,Chunks_nbr-Chunks_proc)
	end.

get_rest_alignments(Pid,Acc,0) -> Pid ! {alignments,Acc};
get_rest_alignments(Pid,Acc,K) ->
	receive
		no_match -> get_rest_alignments(Pid,Acc,K-1);
		{C1,Match,C2} -> get_rest_alignments(Pid,[{C1,Match,C2}|Acc],K-1)
	end.



spawn_sw(Collector_pid,0,Seq,Ref_seq,Chunk_size,Overlap,K) ->
	receive
		processed -> spawn_sw(Collector_pid,1,Seq,Ref_seq,Chunk_size,Overlap,K)
	end;

spawn_sw(Collector_pid,N,Seq,Ref_seq,Chunk_size,Overlap,K) when length(Ref_seq) > Chunk_size+Overlap ->
	io:format("N:~p,~n",[N]),
	{Chunk,Tail} = lists:split(Chunk_size+Overlap,Ref_seq),
	Right = lists:nthtail(Chunk_size,Chunk),
	spawn(?MODULE, sw, [self(), Collector_pid,Seq,Chunk]),

	spawn_sw(Collector_pid,N-1,Seq,Right++Tail,Chunk_size,Overlap,K+1);

spawn_sw(Collector_pid,_,Seq,Ref_seq,_,_,K) ->
	spawn(?MODULE, sw, [self(), Collector_pid,Seq, Ref_seq]),
	io:format("~p chunks sent to alignment~n",[K+1]),
	Collector_pid ! {chunks_nbr, K+1}.



sw(Pid,Collector_pid,Seq,Ref_seq) -> 
	Collector_pid ! sw(Seq,Ref_seq),
	Pid ! processed.



sw([_|_]=W1,[_|_]=W2) when length(W1) > length(W2) -> sw(W2,W1);

sw([_|_]=W1,[_|_]=W2) ->
	THR = length(W1) * ?THRESHOLD * ?MATCH, 
	%io:format("Threshold: ~p~n",[THR]),
	Tab = [ {L1,[{0,undef}]} || L1 <- W1 ],
	Tab1 = build_tab(Tab,W2),
	case find_max(Tab1,W2,THR) of
		{C1,Match,C2} ->
			io:format("~s~n~s~n~s~n",[C1,Match,C2]),
			{C1,Match,C2};
		no_match ->
			io:format("Match above threshold ~p% not found~n",[?THRESHOLD * 100]),
			no_match
	end;

sw(N1,N2) when N1 < N2 -> sw(rand_seq(N1),rand_seq(N2));
sw(N1,N2) when N1 >= N2 -> sw(rand_seq(N2),rand_seq(N1)).


get_reference_seq(Ref_seq_name,File) ->
	case file:open(File++".index",read) of
		{ok,Dev1} ->
			case get_reference_position(Ref_seq_name,Dev1) of
				{Pos,Len} ->
					ok=file:close(Dev1),
					case file:open(File,read) of
						{ok,Dev2} ->
							{ok,Reference_seq} = file:pread(Dev2, Pos, Len),
							lists:filter(fun(S)-> S =/= 10 end,Reference_seq);
        
						{error,_} -> 
							io:format("Cannot open reference sequence file: ~p~n",[File]),
							error
					end;
					
				unknown_reference ->
					io:format("Unknown reference:~p. Check reference name or make an index~n",[Ref_seq_name]),
					ok=file:close(Dev1),
					error
			end;

		{error,Reason} ->
			io:format("index file: '~s' cannot be opened~n~p~n",[File++".index",Reason]),
			error
	end.





get_reference_position(Ref_seq_name,Dev) ->
	case file:read_line(Dev) of
		eof -> unknown_reference;
		{ok,Data} -> 
			[Ref,Pos,Len]=string:tokens(Data," \n"),
			case Ref =:= Ref_seq_name of
				true -> {list_to_integer(Pos),list_to_integer(Len)};
				_ -> get_reference_position(Ref_seq_name,Dev)
			end
	end.
	



build_tab(Tab,[S2|W2]) ->
	Tab1 = build_tab([], 0, 0, Tab, S2),
	build_tab(Tab1,W2);
build_tab(Tab,[]) -> Tab.


build_tab(AccT, V11, V10, [{S1,[{V01,_}|_]=Ls}|Tab], S2) ->
	%io:format("V11:~p, V10:~p, S1:~c, V01:~p~n",[V11,V10,S1,V01]),
	{V,Dir} = lists:max([
		{0,undef},
		{V11+sigma(S1,S2),ul},
		{V10+?GAP_PENALTY,l},
		{V01+?GAP_PENALTY,u} 
		]),
	build_tab([{S1,[{V,Dir}|Ls]}|AccT], V01, V, Tab, S2);

build_tab(AccT,_,_,[],_) -> lists:reverse(AccT).



find_max(Tab,W2,THR) ->
	%io:format("Tab~n~p~n",[Tab]),
	{Vmax,_} = lists:max([ lists:max(Column) || {_,Column} <- Tab ]),
	case Vmax < THR of
		true -> no_match;
		_ ->
			Tab1 = remove_last_columns(Vmax,lists:reverse(Tab)),
			%io:format("Tab after removal~n~p~n",[Tab1]),
			[{_,Col}|_] = Tab1,
			Index = get_index(1,Col,Vmax),
			%io:format("Index:~p~n",[Index]),
			extract_matches([],[],[],Index,Tab1,lists:reverse(W2))
	end.




extract_matches(X1, Match, X2, Index, [{S1,Column}|Tab],W2) ->
	{V,S2, Dir} = get_next(Index,Column,W2),
	case V =:= 0 of
		true -> {X1,Match,X2};
		_ -> 
			case Dir of
				u -> extract_matches(["-"|X1], [" "|Match], [S2|X2], Index+1, [{S1,Column}|Tab], W2);
				l -> extract_matches([S1|X1],  [" "|Match], ["-"|X2], Index, Tab, W2);
				ul when S1=/=S2 -> extract_matches([S1|X1],  [" "|Match], [S2|X2], Index+1, Tab, W2);
				ul -> extract_matches([S1|X1],  ["|"|Match], [S2|X2], Index+1, Tab, W2)
			end
	end;
extract_matches(X1,Match,X2,_,[],_) -> {X1,Match,X2}.



get_next(1,[{V,Dir}|_],[S2|_]) -> {V,S2,Dir};
get_next(Index,[_|Column],[_|W2]) -> get_next(Index-1, Column, W2);
get_next(_,_,[]) -> {0, " ", undef}.




get_index(Index,[{Vmax,_}|_],Vmax) -> Index;
get_index(Index,[_|Col],Vmax) ->
	get_index(Index+1,Col,Vmax).



remove_last_columns(Vmax,[{_,Col}|Rest]=Tab) ->
	{V,_} = lists:max(Col),
	case V < Vmax of
		true ->
			remove_last_columns(Vmax,Rest);
		_ ->
			Tab
	end.



sigma(S,S) -> ?MATCH;
sigma(_,$N) -> ?UNDEF;
sigma($N,_) -> ?UNDEF;
sigma(_,_) -> ?MISMATCH.

