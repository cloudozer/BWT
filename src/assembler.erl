% a module devoted to de novo assembly of genome
%
% (c) Cloudozer, 2015
%


-module(assembler).
-export([start_worker/3,
		encode_G/1
		]).



%% worker builds a sub-graph and then returns it back to Pid
start_worker(Pid,Uid,{J,Read}) ->
	G_str = encode_G(Read),
	Gph = init(Uid,Read,J,G_str),
	worker(Pid,Uid,Gph,G_str).


worker(Pid,Uid,Gph,Prev_read) ->
	receive
		get_graph -> 
			io:format("Graph got ~p nodes~n",[length(digr:nodes(Gph))]),
			digr:export_to_dot(Gph,"sembly"),
			Pid ! {graph,Gph}; 						%% sends graph to master and terminates
		{merge_to,Wid} -> {add_graph, Wid ! Gph}; 		%% sends graph to the other worker and terminates
		{add_graph,Gph1} -> 							%% merges a sub_graph and confirms that to master
			Pid ! {self(),merged},
			worker(Pid,Uid,merge_graphs(Gph,Gph1,Prev_read), Prev_read);
		{next_read,R_id,Read} -> 										%% tries to attach read and confirms the result to master
			case attach_read(Uid,Gph,R_id,Read) of
				{Size,Gph1} -> Pid ! {self(), Size}, worker(Pid,Uid,Gph1,Read);
				false -> Pid ! not_attached, worker(Pid,Uid,Gph,Read)
			end
	end.


% TODO: initialise the graph for a given Read
init(Uid,Seq,Pos,G_str) -> 
	digr:add_node(uid:next(Uid),[{g_string,G_str},{read,Seq},{loc,0},{pos,Pos}],digr:new()).



%% tries to attach a given Read to the graph
attach_read(Uid,{Nodes,_}=Gph,R_id,Read) ->
	G_str = encode_G(Read),
	%% try to attach the Read to top-level nodes of Gph
	Top_nodes = lists:filter(fun({Node_id,_})->digr:incidents(Node_id,Gph)==[] end, Nodes),
	%io:format("Top_nodes: ~w~n",[[ ID || {ID,_}<-Top_nodes]]),
	attach_read(Uid,Gph,R_id,Read,G_str,Top_nodes,[],false).

attach_read(Uid,Gph,R_id,Read,Read_str,[{Node_id,Attrs}|Nodes],Acc,Attached) ->
	{g_string,Graph_str} = lists:keyfind(g_string,1,Attrs),
	%io:format("g_string:~p~n",[Graph_str]),
	%io:format("read:    ~p~n",[Read_str]),
	case align(Read_str,Graph_str) of
		[] -> attach_read(Uid,Gph,R_id,Read,Read_str,Nodes,[{Node_id,Attrs}|Acc],Attached);
		Candidates ->
			%io:format("G-string matches: ~p~n",[Candidates]), 
			Gph1 = add_parent_node(Uid,Gph,Node_id,Graph_str,Attrs,R_id,Read,Read_str,Candidates),
			%throw(not_implemented),
			attach_read(Uid,Gph1,R_id,Read,Read_str,Nodes,[{Node_id,Attrs}|Acc],true)
	end;

attach_read(Uid,Gph,R_id,Read,Read_str,[],Nodes,false) ->
	%% find all children nodes and continue search
	Children = lists:usort( lists:foldl(fun({Nid,_},Acc)-> digr:neighbors(Nid,Gph)++Acc
										end, [], Nodes)
	),

	%io:format("Children: ~p~n",[Children]),
	case Children of
		[] -> false;
		_ -> attach_read(Uid,Gph,R_id,Read,Read_str,Children,[],false)
	end;

attach_read(_,Gph,_,_,_,[],_,true) -> {digr:size(Gph),Gph}.


%% matches a shorter Read_str against a longer Graph_str
%% if it doesnot match it returns false. Otherwise it returns a position
align(Read_str,[Ng|G_str]) -> 
	[Nr|R_str] = lists:reverse(Read_str),
	align(left,R_str,[Nr],G_str,[Ng],[]).

align(left,[Nr|R_str],MatchR,[Ng|G_str],MatchG,Acc) ->
	case MatchR =:= lists:reverse(MatchG) of
		true -> align(left,R_str,[Nr|MatchR],G_str,[Ng|MatchG],[lists:sum(MatchR)|Acc]);
		false-> align(left,R_str,[Nr|MatchR],G_str,[Ng|MatchG],Acc)
	end;
align(left,R_str,MatchR,[],MatchG,Acc) -> 
	align(middle2,R_str,MatchR,lists:sum(MatchG),lists:reverse(MatchG),Acc);

align(left,[],MatchR,G_str,MatchG,Acc) -> 
	align(middle1,lists:sum(MatchR),lists:reverse(MatchR),G_str,MatchG,Acc);
	
align(middle1,Sum,Read_rev,[Ng|G_str],MatchG,Acc) ->
	Last = lists:last(MatchG),
	MatchG1 = [Ng|lists:droplast(MatchG)],
	case Read_rev =:= MatchG of
		true -> align(middle1,Sum+Last,Read_rev,G_str,MatchG1,[Sum|Acc]);
		false-> align(middle1,Sum+Last,Read_rev,G_str,MatchG1,Acc)
	end;
align(middle1,Sum,Read_rev,[],MatchG,Acc) -> 
	align(right,Sum,Read_rev,MatchG,Acc);

align(middle2,[],MatchR,Sum,MatchG_rev,Acc) ->
	align(right,Sum,MatchR,MatchG_rev,Acc);

align(middle2,Read_str,MatchR,Sum,MatchG_rev,Acc) ->
	[Nr|Read_str1] = Read_str,
	Last = lists:last(MatchR),
	case Read_str =:= MatchG_rev of
		true -> align(middle2,Read_str1,[Nr|lists:droplast(MatchR)],Sum+Last,MatchG_rev,[Sum|Acc]);
		false-> align(middle2,Read_str1,[Nr|lists:droplast(MatchR)],Sum+Last,MatchG_rev,Acc)
	end.

align(right,Sum,[_|Read_rev1]=Read_rev,MatchG,Acc)	->
	Last1 = lists:last(MatchG),
	MatchG1 = lists:droplast(MatchG),
	case Read_rev =:= MatchG of
		true -> align(right,Sum+Last1,Read_rev1,MatchG1,[Sum|Acc]);
		false-> align(right,Sum+Last1,Read_rev1,MatchG1,Acc)
	end;
align(right,_,[],[],Acc) -> Acc.



%% Checks if a read matches with Node at each position
%% if it does, it adds child and parent node
add_parent_node(Uid,Gph,Node_id,Graph_str,Attrs,R_id,Read,Read_str,[Sh|Shifts]) ->
	{read,G_read} = lists:keyfind(read,1,Attrs),
	R_body = lists:sum(Read_str),
	G_body = lists:sum(Graph_str),
	if
		Sh < R_body ->
			io:format(" LEFT alignment~n"),
			L = length(Read),
			Overlap = Sh + get_first_G(lists:reverse(Read)) + get_first_G(G_read) - 1,
			R = lists:sublist(Read,L-Overlap+1,Overlap),
			G = lists:sublist(G_read,Overlap),
			case R =:= G of
				true ->
					{loc,Loc} = lists:keyfind(loc,1,Attrs),
					Loc1 = Loc + Overlap - length(Read),
					Kid_id = uid:next(Uid),
					Gph1 = digr:add_node(Kid_id,[{read,Read},{loc,Loc1},{g_string,Read_str},{pos,R_id}],Gph),

					%% add new parent node
					%io:format("~p~n~p~n",[Read,G_read]),
					Left_part = lists:sublist(Read,length(Read)-Overlap),
					Par_read = Left_part++G_read,
					io:format("New parent: ~p~n",[length(Par_read)]),
					Par_id = uid:next(Uid),
					Gph2 = digr:add_node(Par_id,[{read,Par_read},
												{loc,Loc1},{g_string,encode_G(Par_read)}],Gph1),
					%% add two edges
					Gph3 = digr:add_edge({Par_id,Kid_id}, digr:add_edge({Par_id,Node_id},Gph2) );

				false-> Gph3 = Gph
			end;

		Sh =:= R_body andalso Sh =:= G_body -> 
			Gph3 = Gph,
			io:format(" CENTER alignment~n");

		Sh =:= R_body -> 
			Gph3 = Gph,
			io:format(" CENTER alignment~n");

		Sh =:= G_body -> Gph3 = Gph, io:format(" CENTER alignment~n");

		Sh > G_body -> Gph3 = Gph, io:format(" RIGHT alignment~n");

		true -> throw(impossible_case), Gph3 = Gph
	end,
	add_parent_node(Uid,Gph3,Node_id,Graph_str,Attrs,R_id,Read,Read_str,Shifts);
add_parent_node(_,Gph,_,_,_,_,_,_,[]) -> Gph.




merge_graphs(Gph1,Gph2,Read) -> Gph1.  %% TODO.
	


get_first_G(Str) -> get_first_G(Str,1).
get_first_G([$G|_],Count) -> Count;
get_first_G([_|Str],Count) -> get_first_G(Str,Count+1).



encode_G(Read) -> encode_G(Read,1,[]).

encode_G([$G|Read],Count,Acc) -> encode_G(Read,1,[Count|Acc]);
encode_G([_|Read],Count,Acc) -> encode_G(Read,Count+1,Acc);
encode_G([],_,Acc) -> [_|G_str] = lists:reverse(Acc), G_str.



