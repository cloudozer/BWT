% a module devoted to de novo assembly of genome
%
% (c) Cloudozer, 2015
%


-module(assembler).
-export([start_worker/2
		]).




%% worker builds a sub-graph and then returns it back to Pid
start_worker(Pid,{J,Read}) ->
	G_str = encode_G(Read),
	Gph = init(Read,J,G_str),
	worker(Pid,Gph,G_str).


worker(Pid,Gph,Prev_read) ->
	receive
		get_graph -> Pid ! Gph; 						%% sends graph to master and terminates
		{merge_to,Wid} -> {add_graph, Wid ! Gph}; 		%% sends graph to the other worker and terminates
		{add_graph,Gph1} -> 							%% merges a sub_graph and confirms that to master
			Pid ! {self(),merged},
			worker(Pid, merge_graphs(Gph,Gph1,Prev_read), Prev_read);
		{R_id,Read} -> 										%% tries to attach read and confirms the result to master
			case attach_read(Gph,R_id,Read) of
				{Size,Gph1} -> Pid ! {self(), Size}, worker(Pid,Gph1,Read);
				false -> Pid ! not_attached, worker(Pid,Gph,Read)
			end
	end.


% TODO: initialise the graph for a given Read
init(Seq,Pos,G_str) -> 
	digr:add_node(Pos,[{g_string,G_str},{read,Seq},{loc,0}],digr:new()).



%% tries to attach a given Read to the graph
attach_read({Nodes,_}=Gph,R_id,Read) ->
	G_str = encode_G(Read),
	%% try to attach the Read to top-level nodes of Gph
	Top_nodes = lists:filter(fun({Node_id,_})->digr:incidents(Node_id,Gph)==[] end, Nodes),
	%io:format("Top_nodes: ~w~n",[[ ID || {ID,_}<-Top_nodes]]),
	attach_read(Gph,R_id,Read,G_str,Top_nodes,[],false).

attach_read(Gph,R_id,Read,Read_str,[{Node_id,Attrs}|Nodes],Acc,Attached) ->
	{g_string,Graph_str} = lists:keyfind(g_string,1,Attrs),
	%io:format("g_string:~p~n",[Graph_str]),
	case align(Read_str,Graph_str) of
		false -> attach_read(Gph,R_id,Read,Read_str,Nodes,[{Node_id,Attrs}|Acc],Attached);
		J -> 
			Gph1 = add_parent_node(Gph,Node_id,Attrs,R_id,Read,Read_str,J),
			attach_read(Gph1,R_id,Read,Read_str,Nodes,[{Node_id,Attrs}|Acc],true)
	end;

attach_read(Gph,R_id,Read,Read_str,[],Nodes,false) ->
	%% find all children nodes and continue search
	Children = lists:usort( lists:foldl(fun({Nid,_},Acc)-> digr:neighbors(Nid,Gph)++Acc
										end, [], Nodes)
	),

	%io:format("Children: ~p~n",[Children]),
	case Children of
		[] -> false;
		_ -> attach_read(Gph,R_id,Read,Read_str,Children,[],false)
	end;

attach_read(Gph,_,_,_,[],_,true) -> {digr:size(Gph),Gph}.


%% matches a shorter Read_str against a longer Graph_str
%% if it doesnot match it returns false. Otherwise it returns a position
align(Read_str,Graph_str) -> false.



add_parent_node(Gph,Node_id,Attrs,R_id,Read,Read_str,J) ->
	Gph.



merge_graphs(Gph1,Gph2,Read) -> Gph1.  %% TODO.
	




encode_G(Read) -> encode_G(Read,1,[]).

encode_G([$G|Read],Count,Acc) -> encode_G(Read,1,[Count|Acc]);
encode_G([_|Read],Count,Acc) -> encode_G(Read,Count+1,Acc);
encode_G([],Count,Acc) -> lists:reverse([Count|Acc]).



