% a module devoted to de novo assembly of genome
%
% (c) Cloudozer, 2015
%


-module(assembler).
-export([start_worker/3,
		encode_G/1,
		align/2,
		t/0
		]).

t() -> 
	S1 = [2,3,4,3,7,2,4,1,1,9,1,5,5,5,4,3,3,1,2,7,2,3,5,2,1,9,2],
	S2 = [2,1,9,2,9,3,3,5,1,1,2,3,9,3,3,2,1,4,1,3,3,2,1,5,3,1,1,9,3,1,1,1],

	align(S2,S1).

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
	io:format("New read: ~p~n",[R_id]),
	%% try to attach the Read to top-level nodes of Gph
	Top_nodes = lists:filter(fun({Node_id,_}) -> length(digr:incidents(Node_id,Gph))==0 end, Nodes),
	%io:format("Top_nodes: ~w~n",[[ ID || {ID,_}<-Top_nodes]]),
	attach_read(Uid,Gph,R_id,Read,G_str,Top_nodes,[],false).

attach_read(Uid,Gph,R_id,Read,Read_str,[{Node_id,Attrs}|Nodes],Acc,Attached) ->
	{g_string,Graph_str} = lists:keyfind(g_string,1,Attrs),
	%io:format("g_string:~p~n",[Graph_str]),
	%io:format("read:    ~p~n",[Read_str]),
	case align(Read_str,Graph_str) of
		[] -> attach_read(Uid,Gph,R_id,Read,Read_str,Nodes,[{Node_id,Attrs}|Acc],Attached);
		Candidates ->
			io:format("G-string matches: ~p~n",[Candidates]), 
			case add_parent_node(Uid,Gph,Node_id,Graph_str,Attrs,R_id,Read,Read_str,Candidates) of
				false -> attach_read(Uid,Gph,R_id,Read,Read_str,Nodes,[{Node_id,Attrs}|Acc],Attached);
				Gph1 -> attach_read(Uid,Gph1,R_id,Read,Read_str,Nodes,[{Node_id,Attrs}|Acc],true)
			end
	end;

attach_read(Uid,Gph,R_id,Read,Read_str,[],Nodes,false) ->
	%% find all children nodes and continue search
	io:format("read not aligned. Aligning to the children ...~n"),
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
align(Read,[Ng|Ref]) ->
	[Nr|R_rev] = lists:reverse(Read),
	Q = queue:new(),
	align(R_rev,queue:in(Nr,Q),Nr,Ref,queue:in(Ng,Q),Ng,Nr,[]).

align([Nr|R_rev],Qr,Sum,[Ng|Ref],Qg,Sum,Sum,Acc) ->
	case equal(Qr,Qg) of
		true -> align(R_rev,queue:in_r(Nr,Qr),Sum+Nr,Ref,queue:in(Ng,Qg),Sum+Ng,Sum+Nr,[Sum|Acc]);
		false-> align(R_rev,queue:in_r(Nr,Qr),Sum+Nr,Ref,queue:in(Ng,Qg),Sum+Ng,Sum+Nr,Acc)
	end;
align([Nr|R_rev],Qr,Sr,[Ng|Ref],Qg,Sg,Sr,Acc) ->
	align(R_rev,queue:in_r(Nr,Qr),Sr+Nr,Ref,queue:in(Ng,Qg),Sg+Ng,Sr+Nr,Acc);

align([],Qr,Sum,[Ng|Ref],Qg,Sum,S,Acc) ->
	{{value,G},Qg1} = queue:out(Qg),
	case equal(Qr,Qg) of
		true -> align([],Qr,Sum,Ref,queue:in(Ng,Qg1),Sum-G+Ng,S+G,[S|Acc]);
		false-> align([],Qr,Sum,Ref,queue:in(Ng,Qg1),Sum-G+Ng,S+G,Acc)
	end;
align([],Qr,Sr,[Ng|Ref],Qg,Sg,S,Acc) ->
	{{value,G},Qg1} = queue:out(Qg),
	align([],Qr,Sr,Ref,queue:in(Ng,Qg1),Sg-G+Ng,S+G,Acc);

align([Nr|Read],Qr,Sum,[],Qg,Sum,S,Acc) ->
	{{value,R},Qr1} = queue:out_r(Qr),
	case equal(Qr,Qg) of
		true -> align(Read,queue:in_r(Nr,Qr1),Sum-R+Nr,[],Qg,Sum,S+Nr,[S|Acc]);
		false-> align(Read,queue:in_r(Nr,Qr1),Sum-R+Nr,[],Qg,Sum,S+Nr,Acc)
	end;
align([Nr|Read],Qr,Sr,[],Qg,Sg,S,Acc) ->
	{{value,R},Qr1} = queue:out_r(Qr),
	align(Read,queue:in_r(Nr,Qr1),Sr-R+Nr,[],Qg,Sg,S+Nr,Acc);

align([],Qr,Sr,[],Qg,Sg,S,Acc) ->
	Len=queue:len(Qr),Len=queue:len(Qg),
	f_align(Len,Qr,Sr,Qg,Sg,S,Acc).

f_align(0,_,0,_,0,_,Acc) -> Acc;
f_align(Len,Qr,Sum,Qg,Sum,S,Acc) ->
	{{value,G},Qg1} = queue:out(Qg),
	{{value,R},Qr1} = queue:out_r(Qr),
	case equal(Qr,Qg) of
		true -> f_align(Len-1,Qr1,Sum-R,Qg1,Sum-G,S+G,[S|Acc]);
		false-> f_align(Len-1,Qr1,Sum-R,Qg1,Sum-G,S+G,Acc)
	end;
f_align(Len,Qr,Sr,Qg,Sg,S,Acc) -> 
	{{value,G},Qg1} = queue:out(Qg),
	{{value,R},Qr1} = queue:out_r(Qr),
	%io:format("Args:~p~n",[[Len,Qr,Sr,Qg,Sg,S]]),
	f_align(Len-1,Qr1,Sr-R,Qg1,Sg-G,S+G,Acc).


equal(Q1,Q2) -> queue:to_list(Q1) =:= queue:to_list(Q2).



%% Checks if a read matches with Node at each position
%% if it does, it adds child and parent node
add_parent_node(Uid,Gph,Node_id,Graph_str,Attrs,R_id,Read,Read_str,Candidates) ->
	add_parent_node(Uid,Gph,Node_id,Graph_str,Attrs,R_id,Read,Read_str,Candidates,false).

add_parent_node(Uid,Gph,Node_id,Graph_str,Attrs,R_id,Read,Read_str,[Sh|Shifts],Attached) ->
	{read,G_read} = lists:keyfind(read,1,Attrs),
	R_body = lists:sum(Read_str),
	G_body = lists:sum(Graph_str),
	if
		Sh < R_body ->
			L = length(Read),
			Overlap = Sh + get_first_G(lists:reverse(Read)) + get_first_G(G_read) - 1,
			R = lists:sublist(Read,L-Overlap+1,Overlap),
			G = lists:sublist(G_read,Overlap),
			case R =:= G of
				true ->
					io:format(" LEFT alignment~n"),
			
					{loc,Loc} = lists:keyfind(loc,1,Attrs),
					Loc1 = Loc + Overlap - length(Read),
					Kid_id = uid:next(Uid),
					Gph1 = digr:add_node(Kid_id,[{read,Read},{loc,Loc1},{g_string,Read_str},{pos,R_id}],Gph),

					%% add new parent node
					%io:format("~p~n~p~n",[Read,G_read]),
					Left_part = lists:sublist(Read,length(Read)-Overlap),
					Par_read = Left_part++G_read,
					io:format("New parent. Length: ~p~n",[length(Par_read)]),
					Par_id = uid:next(Uid),
					Gph2 = digr:add_node(Par_id,[{read,Par_read},
												{loc,Loc1},{g_string,encode_G(Par_read)}],Gph1),
					%% add two edges
					Gph3 = digr:add_edge({Par_id,Kid_id}, digr:add_edge({Par_id,Node_id},Gph2) ),
					add_parent_node(Uid,Gph3,Node_id,Graph_str,Attrs,R_id,Read,Read_str,Shifts,true);

				false-> 
					add_parent_node(Uid,Gph,Node_id,Graph_str,Attrs,R_id,Read,Read_str,Shifts,Attached)
			end;

		Sh =:= R_body andalso Sh =:= G_body -> 
			io:format(" CENTER alignment~n"),
			add_parent_node(Uid,Gph,Node_id,Graph_str,Attrs,R_id,Read,Read_str,Shifts,Attached);

		Sh =:= R_body -> 
			io:format(" CENTER alignment~n"),
			add_parent_node(Uid,Gph,Node_id,Graph_str,Attrs,R_id,Read,Read_str,Shifts,Attached);

		Sh =:= G_body -> 
			io:format(" CENTER alignment~n"),
			add_parent_node(Uid,Gph,Node_id,Graph_str,Attrs,R_id,Read,Read_str,Shifts,Attached);

		Sh < G_body -> % Shift is larger than Read
			io:format(" CENTER alignment~n"),
			add_parent_node(Uid,Gph,Node_id,Graph_str,Attrs,R_id,Read,Read_str,Shifts,Attached);

		Sh > G_body -> 
			L = length(G_read),
			io:format("Bodies:~p, ~p~n",[G_body,R_body]),
			io:format("Shift:~p~n",[Sh]),
			Overlap = R_body + G_body - Sh + get_first_G(Read) + get_first_G(lists:reverse(G_read)) - 1,
			io:format("Overlap:~p~n",[Overlap]),
			G = lists:sublist(G_read,L-Overlap+1,Overlap),
			R = lists:sublist(Read,Overlap),
			io:format("~p~n~p~n",[G,R]),
			case R =:= G of
				true ->
					io:format(" RIGHT alignment~n"),
			
					{loc,Loc} = lists:keyfind(loc,1,Attrs),
					Loc1 = Loc - Overlap + length(G_read),
					Kid_id = uid:next(Uid),
					Gph1 = digr:add_node(Kid_id,[{read,Read},{loc,Loc1},{g_string,Read_str},{pos,R_id}],Gph),

					%% add new parent node
					%io:format("~p~n~p~n",[Read,G_read]),
					Left_part = lists:sublist(Read,length(Read)-Overlap),
					Par_read = Left_part++G_read,
					io:format("New parent. Length: ~p~n",[length(Par_read)]),
					Par_id = uid:next(Uid),
					Gph2 = digr:add_node(Par_id,[{read,Par_read},
												{loc,Loc},{g_string,encode_G(Par_read)}],Gph1),
					%% add two edges
					Gph3 = digr:add_edge({Par_id,Kid_id}, digr:add_edge({Par_id,Node_id},Gph2) ),
					add_parent_node(Uid,Gph3,Node_id,Graph_str,Attrs,R_id,Read,Read_str,Shifts,true);

				false-> 
					add_parent_node(Uid,Gph,Node_id,Graph_str,Attrs,R_id,Read,Read_str,Shifts,Attached)
			end;


			
		true ->
			io:format("G_body:~p, R_body:~p, Shift:~p~n",[G_body,R_body,Sh]), 
			throw(impossible_case)
	end;
	
add_parent_node(_,Gph,_,_,_,_,_,_,[],true) -> Gph;
add_parent_node(_,_,_,_,_,_,_,_,[],false) -> false.




merge_graphs(Gph1,Gph2,Read) -> Gph1.  %% TODO.
	


get_first_G(Str) -> get_first_G(Str,1).
get_first_G([$G|_],Count) -> Count;
get_first_G([_|Str],Count) -> get_first_G(Str,Count+1).



encode_G(Read) -> encode_G(Read,1,[]).

encode_G([$G|Read],Count,Acc) -> encode_G(Read,1,[Count|Acc]);
encode_G([_|Read],Count,Acc) -> encode_G(Read,Count+1,Acc);
encode_G([],_,Acc) -> [_|G_str] = lists:reverse(Acc), G_str.



