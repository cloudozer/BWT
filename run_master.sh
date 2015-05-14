rm -rf deps
./rebar get-deps compile && sed -i "s/127.0.0.1/`hostname -I`/g" rel/master/files/vm.args && ./rebar generate && ./rel/master/master/bin/master start
