
# How to run DNA aligner app on a cluster.

[TODO: update with LING specifics]

## Checkout and build
	$ git clone https://github.com/cloudozer/BWT.git
	$ cd BWT
	$ git checkout 0.1
	$ ./rebar get-deps compile
	
## Getting DNA files
1. Download an archive: https://docs.google.com/uc?id=0B2DPaltm6IwpYVFHOEZYSGpldHc&export=download
2. Extract it to the BWT folder
	
## Run test on local machine
	$ ./start_local.sh

## Run test on a cluster

### Cluster's nodes requirements
* Friendly Linux
* Erlang OTP 17
* Git
* Internet access

### Master node Setup

	$ cd BWT
	$ erl -pa ebin deps/*/ebin apps/*/ebin -name master@%HOST_NAME% -setcookie secret_gc -eval "master_app:dev()"

### Worker node Setup

Ensure that the following script runs on boot-up. Replace 'erlangonxen.org' wiht the master's hostname in apps/worker_bwt_app/src/worker_bwt_app_sup.erl.

	cd %BWT_FOLDER%
	git pull
	./rebar update-deps compile
	erl -pa ebin deps/*/ebin apps/*/ebin -name erl1@%HOST_NAME% -setcookie secret_gc -eval "worker_bwt_app_app:dev()"

### To run aligner on the cluster
1. Start master and worker nodes.
2. Run:

	# from your laptop:
	$ ./start_cluster.sh %MASTER_HOST_NAME% %WORKERS_NUMBER%
	
	%% or from Erlang shell of the master node
	1> gen_server:call(master , {run, "bwt_files/SRR770176_1.fastq", "GL000193.1", %WORKERS_NUMBER%}).

[Disregards Info below this line]

# Big file processing 

These are small tools to help process large files in Erlang.  In general, the strategy is to read in the file as an array of possibly overlapping Erlang binary "chunks".  These can then be processed in parallel/concurrently.

## How to run
1. download bio_pfile.erl
2. launch Erlang shell
3. compile: c(bio_pfil)
4. run: bio_pfile:read(Filename,NumerOfChunks) or bio_pfile:read(FileName,NumberOfChunks,SizeOfOverlap)
        both of which return an array of chunk elements: {{StartPos,Length},BinaryData}

## Example

    1> Data = bio_pfile:read("../data/GCA_000001405.15_GRCh38_full_analysis_set.fna",10000).
    [{{0,32553715},<<">chr1  AC:CM000663.2  gi:568336023  LN:248956422  rl:Chromosome  M5:6aef897c3d6ff0c78aff06ac189178dd     AS"...>>},
    {{32543715,32563715},<<"ACCTCATAGATTGGTCATCTTTTTCTC\nCTATATTTCTCTAATATTTAATCTCTCTCTCTCTCTCTCTTTGTATGTGCATTGCCTTTGGAGAGATTTC\nC"...>>},
    {{65097430,32563715},<<"AATCAAGAAAATATGTTTACCAAAA\nTGCATTGCAATTTTCCCAAACCTGAGTCTTCAAATAACAAACATGAACTTATAGGTACTGTGAACTAGAA"...>>},
    {{97651145,32563715},<<"CAAGAATTGAGGTTTGGGAAACT\nCCATCTAGATTTCAGAGGATGTATGGAAATACCTGGATGTCCAGGCAGTAGTTTGCTGCAAGGGTGTG"...>>},
    {{813832875,32563715},<<"TT\nT"...>>},
    {{846386590,...},<<...>>},
    {{...},...},
    {...}|...]
    2> length(Data).                                                                        
    10001
    3> lists:nth(1,Data).                                                                   
    {{0,32553715},<<">chr1  AC:CM000663.2  gi:568336023  LN:248956422  rl:Chromosome  M5:6aef897c3d6ff0c78aff06ac189178dd  AS:GRC"...>>}
    4>  


5. run: bio_pfile:spawn_find_pattern(ChunkArray,BinaryPattern) which returns an array of all the stat positions where the pattern was found as {StartPosition,LengthOfPattern}.

## Example

    4>  bio_pfile:spawn_find_pattern(Data,<<"TATATTCAGTCTTTCTAACACCATTTATTGAAGAGACTGTAG">>).
    [{162758595,42}]
