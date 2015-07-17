#!/usr/bin/env escript

main([Subnet, Wc]) ->
        lists:foreach(
                fun(I) ->
                        os:cmd("xl destroy bwtw"++integer_to_list(I))
                end,
                lists:seq(1,30)
        ),
        MasterIp = "{172,16,1,254}",
        Gateway = "172.16." ++ Subnet ++ ".1",
        lists:foreach(
                fun(Id) ->
                        Name = "bwtw"++integer_to_list(Id),
                        IpAddr = "172.16."++Subnet++"."++integer_to_list(Id),
                        Bridge = "br"++integer_to_list(Id),
                        Xl = 
                                "xl create /dev/null '"
                                        "name=\""++Name++"\"; "
                                        "kernel=\"bwtw.img\"; "
                                        "extra=\""
                                                "-worker_bwt_app master_ip "++MasterIp++" "
                                                "-ipaddr "++IpAddr++" -netmask 255.255.0.0 -gateway "++Gateway++" "
                                                "-home /BWT -pz /BWT/apps/master/ebin /BWT/apps/worker_bwt_app/ebin /BWT/deps/goldrush/ebin /BWT/deps/lager/ebin /BWT/ebin " 
                                                "-eval \\\"worker_bwt_app_app:dev()\\\""
                                        "\"; "
                                        "memory=1024; "
                                        "vif=[\"bridge="++Bridge++"\"]"
                                "'",
                        io:format("~s\n",[Xl]),
                        io:format(os:cmd(Xl))
                end,
                lists:seq(1, list_to_integer(Wc))
        );
main(_) ->
        io:format("usage:\n\tbwtw <subnet> <worker count>\n").
