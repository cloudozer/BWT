# Setup LINCX cluster with LING aligner workers

Below is the steps required to setup hosts with aligner workers.
Assuming that aligner master already launched on host 172.16.1.254.

## Dom0 network setup

Each worker seats on separate bridge, so we need to create bridges:
	$ brctl addbr brX
	$ ip link set brX address 00:16:EF:01:00:0X
(replace X with bridge number)

Create bridge for LINCX configurator:
	$ brctl addbr xenbr0
	$ ip ad ad 192.168.1.1/24 dev xenbr0

Create bridge for cross-host worker traffic:
	$ brctl addbr xenbr1
	$ ip ad ad 172.16.X.252/24 dev xenbr1
(replace X with host number)

Enable forwarding for cross-host traffic:
	$ echo 1 > /proc/sys/net/ipv4/ip_forward
	$ echo 1 > /proc/sys/net/ipv4/conf/all/proxy_arp

## LINCX setup

Get and compile sources:
	$ git clone https://github.com/FlowForwarding/lincx.git
	$ cd lincx
	$ ./rebar get-deps compile

Create lincx.yml (assuming you will have 8 workers per host):
```
ipconf:
  ipaddr: 192.168.1.2
  netmask: 255.255.255.0
  gateway: 192.168.1.1

ports:
  - {id: 1, bridge: br1}
  - {id: 2, bridge: br2}
  - {id: 3, bridge: br3}
  - {id: 4, bridge: br4}
  - {id: 5, bridge: br5}
  - {id: 6, bridge: br6}
  - {id: 7, bridge: br7}
  - {id: 8, bridge: br8}
  - {id: 9, bridge: xenbr1}

controllers:
  - 192.168.1.1:6653

listen: 0.0.0.0:6653

memory: 512
```

Build image:
	$ ./railing image
Launch LINCX:
	$ xl create domain_config
Setup LINCX flow tables:
	$ ./scripts/hetzner.erl X
(replace X with host number)

Networking part is done!

## Setup BWT
Build image:
	$ git clone -b bwtling https://github.com/cloudozer/BWT.git
	$ cd BWT
	$ ./rebar get-deps compile
	$ ./railing image -i bwt_files -lsasl

Create domain config bwtX.dom for each worker:
```
name = "bwtX"
kernel = "BWT.img"
extra = "-ipaddr 172.16.Y.X -netmask 255.255.0.0 -home /BWT -pz /BWT/apps/master/ebin /BWT/apps/worker_bwt_app/ebin /BWT/bwt_files /BWT/deps/goldrush/ebin /BWT/deps/lager/ebin /BWT/ebin -eval 'application:start(sasl),application:start(bwt),application:start(worker_bwt_app)'"
memory = 1024
vif = ['bridge=brX']
```
(replace X with worker number and Y with host number)

Launch workers:
	$ xl create bwtX.dom

Workers setup is done! Now you can attach to master, ensure that workers are succesfully connected and issue some work to them:
	$ gen_server:call(master , {run, "bwt_files/SRR770176_1.fastq", "GL000193.1", Z}).
(replace Z with total number of your workers)
