% a module devoted to de novo assembly of genome
%
% (c) Cloudozer, 2015
%


-module(g_string).
-export([align/2,t/0]).

t() -> 
	S1 = [2,3,4,3,7,2,4,1,1,9,1,5,5,5,4,3,3,1,2,7,2,3,5,2,1,9,2],
	S2 = [2,1,9,2,9,3,3,5,1,1,2,3,9,3,3,2,1,4,1,3,3,2,1,5,3,1,1,9,3,1,1,1],

	align(S2,S1).


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


