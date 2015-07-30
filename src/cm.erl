% CIGAR maker module 
% 
% 
% Cloudozer(c), 2015
%

-module(cm).
-export([start_cigar_makers/3,
		cigar_maker/2
		]).


start_cigar_makers(N,Sink,LingdRef) ->
	lists:foreach(  fun(I) ->
		NodeName = list_to_atom("cm_" ++ integer_to_list(I)),
		{ok,_} = lingd:create(LingdRef, NodeName, []),
		navel:call_no_return(NodeName, erlang, spawn, [?MODULE, cigar_maker,[{navel:get_node(),self()},Sink]])
	end,lists:seq(1,N)).



cigar_maker({AlqN,AlqP}=Alq, Sink) ->
	navel:call_no_return(AlqN,erlang,send,[AlqP,{self(),ready}]),
	receive
		{_Ref,Read={_SeqName, QsecBin},Chunk,Pos,_D} ->
			io:format("CM: got read. Aligned: "),
			% run SW and send results to Sink
			case sw:sw(120,100) of
				no_match -> io:format("no_match~n");
				{Score,CIGAR} -> 
					io:format("~p, ~p~n",[Score,CIGAR]),
					Sink ! {Read,Chunk,Pos,Score,CIGAR}
			end,


      Results = lists:foldl(fun({{SeqName, QsecBin}, Seeds1}, Acc) ->
        Qsec = binary_to_list(QsecBin),
        Cigars = lists:foldl(fun({S,D}, Acc) ->

          Ref_len = length(Qsec) + D,
          Start_pos = S - Ref_len + ?REF_EXTENSION_LEN,

          <<_:Start_pos/bytes,Ref_seq:Ref_len/bytes,_/binary>> = Ref,
          Ref_seq1 = binary_to_list(Ref_seq),

          case sw:sw(Qsec,Ref_seq1) of
            no_match -> Acc;
            Cigar -> [{Cigar,S - Ref_len + Shift,Ref_seq1} | Acc]
          end

        end, [],  Seeds1),

        case Cigars of
          [] -> Acc;
          [{Cigar,P,RefSeq}] ->
            [{cigar, Chromosome, SeqName, Cigar, P, RefSeq} | Acc];
          _ ->
            [{TopCigar,P,RefSeq} | _] = lists:sort(fun({{R1,_},_,_}, {{R2,_},_,_}) -> R1 > R2 end, Cigars),
            [{cigar, Chromosome, SeqName, TopCigar, P, RefSeq} | Acc]
        end
      end, [], Seeds),


			spawn(?MODULE,cigar_maker,[Alq,Sink]);

		quit -> ok	
	end.
