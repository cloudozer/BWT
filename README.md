
python3 -m http.server 8888

# LING
sudo ../gator/gator --listen --bind-to 127.0.0.1 --port 4387
make && ./scripts/start_local.es bwt_files/SRR770176_1.fastq_tiny GL000193.1 http://127.0.0.1:8888 [box1] "[{vm,ling}]"

# BEAM

./rebar co && ./scripts/start_local.es bwt_files/SRR770176_1.fastq 22 http://127.0.0.1:8888 "[{box1,'46.4.100.178'}]" "[{vm,beam}]"

# Checkout and build
	$ git clone https://github.com/cloudozer/BWT.git
	$ cd BWT
	$ git checkout master
	$ ./rebar get-deps
	# Setup domain config files (bwtm.dom and bwtw.dom)
	$ make
	
## Getting DNA files
1. Download an archive: https://docs.google.com/uc?id=0B2DPaltm6IwpYVFHOEZYSGpldHc&export=download
2. Extract it to the BWT folder
	
## Run test on local machine using 2 workers
	$ ./scripts/start_local.sh SRR770176_1.fastq GL000193.1 2

### Cluster's nodes requirements
* Friendly Linux
* Xen
* Erlang OTP 17
* Git
* Internet access

### Master node Setup

Edit domain config file 'bwtm.dom', setup expected number of workers, ssh port, etc.

	$ make
	$ sudo xl create -c bwtm.dom

### Worker node Setup

Edit domain config file 'bwtm.dom', setup master ip address, etc.

	$ make
	$ sudo xl create -c bwtw.dom

# Secure Shell connection to a Ling node

	$ ssh %NODE_HOST% -p %PORT%   # (password: 1)


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
