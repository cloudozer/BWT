%
% (c) Cloudozer, 2014
% 
% 

-module(msw).
-export([main/4,main_serial/4,
		worker/5, get_reference_position/2, get_chunk/3]).

-include("bwt.hrl").


main(N,[_|_]=Seq,Ref_seq_name,File) ->
	seeds:generate_fs(Seq,15,2),
	compile:file("fs.erl",[report_errors]),

	case get_reference_position(Ref_seq_name,File) of
		{Pos,Len} ->
			Chunk_size = Len div N,
			[ spawn(?MODULE, worker, [self(),Seq,File,
									Pos+J*Chunk_size, 
									Chunk_size+?THRESHOLD+length(Seq) ]) || J <- lists:seq(0,N-1)],
			Res = collect(N,[]),
			io:format("~p match(es) obtained~n",[length(Res)]),
			Res;

		error -> error
	end.

main_serial(N,[_|_]=Seq,Ref_seq_name,File) ->
  seeds:generate_fs(Seq,15,2),
  compile:file("fs.erl",[report_errors]),
  code:add_path("."),
  code:load_file(fs),

  case get_reference_position(Ref_seq_name,File) of
    {Pos,Len} ->
      Chunk_size = Len div N,

      Res = lists:foldl(
        fun(J,Acc) ->
          spawn(?MODULE, worker, [self(),Seq,File,
                      Pos+J*Chunk_size,
                      Chunk_size+?THRESHOLD+length(Seq) ]),

          receive
            error ->
              io:format("An error occur. ~n"),
              Acc;
            Matches ->
              Matches++Acc
          end
        end,
        [],
        lists:seq(0,N-1)
      ),
      io:format("~p match(es) obtained~n",[length(Res)]),
      Res;

    error -> error
  end.


collect(0,Acc) -> Acc;
collect(N,Acc) ->
	receive
		error ->
			io:format("An error occur. ~n"),
			collect(N-1,Acc);
		Matches -> 
			collect(N-1,Matches++Acc)
	end.



worker(Pid,Seq,File,Pos,Len) ->
	 case get_chunk(File,Pos,Len) of
	 	error -> Pid ! error;
	 	Ref_seq ->
	 		Seeds = fs:find_seeds(Ref_seq),
	 		%%io:format("~p seeds found~n",[length(Seeds)]),
	 		Res = lists:foldl(fun(S,Acc)->  
 				case sw:sw(Seq,lists:sublist(Ref_seq,S,length(Seq)+?THRESHOLD)) of
 					no_match -> Acc;
 					Match -> [{Pos+S,Match}|Acc]
 				end
	 						  end,[],Seeds),
	 		Pid ! Res
	 end.




get_chunk(File,Pos,Len) ->
	case file:open(File,read) of
		{ok,Dev} ->
			{ok,Ref_seq} = file:pread(Dev, Pos, Len),
                        file:close(Dev),
			lists:filter(fun(10)->false; (13)->false; (_)->true end,Ref_seq);
%% 			lists:filter(fun(S)-> S > 64 end,Ref_seq);
		{error,Reason} ->
			io:format("file: '~s' cannot be opened~n~p~n",[File,Reason]),
			error
	end.



get_reference_position(Ref_seq_name,File) ->
	case file:open(File++".index",read) of
		{ok,Dev} ->
			find_ref_position(Ref_seq_name,Dev);
			
		{error,Reason} ->
			io:format("index file: '~s' cannot be opened~n~p~n",[File++".index",Reason]),
			error
	end.

find_ref_position(Ref_seq_name,Dev) ->
	case file:read_line(Dev) of
		eof -> 
			io:format("Reference name not found~n",[]),
			error;
		{ok,Data} -> 
			[Ref,Pos,Len]=string:tokens(Data," \n"),
			case Ref =:= Ref_seq_name of
				true -> 
					file:close(Dev),
					{list_to_integer(Pos),list_to_integer(Len)};
				_ -> find_ref_position(Ref_seq_name,Dev)
			end
	end.


