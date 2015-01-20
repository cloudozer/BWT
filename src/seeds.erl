% Seeds finding algo 
% 
% Cloudozer(c), 2014
%

-module(seeds).
-export([generate_fs/3]).



generate_fs(Qsec,Len,Defects) ->
	
	{ok,Dev} = file:open("fs.erl",write),
	io:format(Dev,"% Seeds finding algo
%
% Cloudozer(c), 2014~n~n
-module(fs).
-export([matches/~p, find_seeds/1]).~n",
	[Len]),

	%% find_seeds fun
	Args_str = args(Len),
	io:format(Dev,"~n
find_seeds(Ref_Seq) ->
	find_seeds([],0, Ref_Seq).~n
find_seeds(Acc,N,[~s]) ->
	case matches(~s) of
		true -> [N,Acc];
		_ -> Acc
	end;~n",
		[Args_str,Args_str]),

	[_,_,_|Args_str1]=Args_str,
	io:format(Dev,"
find_seeds(Acc,N,[~s|Ref_Seq]) ->
	case matches(~s) of
		true -> find_seeds([N|Acc],N+length([~s]),Ref_Seq);
		_ -> find_seeds(Acc,N+1,[~s|Ref_Seq])
	end;~n
find_seeds(Acc,_,_) -> Acc.~n~n~n",
		[Args_str,Args_str,Args_str,Args_str1]),

	SubQsec = lists:sublist(Qsec,Len+Defects),
	gen_pattern([],SubQsec,Len,Defects,Dev),

	io:format(Dev,"matches(~s) -> false.~n",[false_string(Len)]),
	file:close(Dev).



gen_pattern(Acc,SubQsec,Len,0,Dev) ->
	N = Len - length(Acc),
	Tail = lists:sublist(SubQsec,N),
	Sequence = lists:reverse(Tail)++Acc,
	print_pattern(Sequence,Dev);

gen_pattern(Acc,[S|SubQsec],Len,Defects,Dev) when length(Acc)<Len ->
	gen_pattern([$*|Acc],SubQsec,Len,Defects-1,Dev), % Mutation
	gen_pattern([$*|Acc],[S|SubQsec],Len,Defects-1,Dev), % Insertion
	gen_pattern(Acc,SubQsec,Len,Defects-1,Dev), % Deletion
	gen_pattern([S|Acc],SubQsec,Len,Defects,Dev); % Match	
gen_pattern(_,_,_,_,_) -> ok.


print_pattern(Sequence,Dev) ->
	[_|String]=lists:foldl(fun(S,Acc)->
		case S of
			$* -> ",_"++Acc;
			_ -> ",$"++[S]++Acc
		end
							end,"",Sequence ),
	
	io:format(Dev,"matches(~s) -> true;~n",[String]).


false_string(1) -> "_";
false_string(N) -> "_,"++false_string(N-1).



args(N) -> 
	[_|Res]=lists:foldl(fun(J,Acc)-> 
							",X"++integer_to_list(J)++Acc
						end,"",lists:reverse(lists:seq(1,N)) ),
	Res.



