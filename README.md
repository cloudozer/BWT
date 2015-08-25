# Clone BWT, switch to branch 'source_sink_multibox':

    $ git clone https://github.com/cloudozer/BWT.git
    $ git checkout source_sink_multibox
    
# Build

    $ cd BWT
    $ ./rebar get-deps co
    
# Get mouse genome
    $ wget ftp://hgdownload.cse.ucsc.edu/goldenPath/mm9/chromosomes/chr17.fa.gz
    $ gunzip -c chr17.fa.gz > bwt_files/chr17.fa
    
    $ wget http://ftp.era.ebi.ac.uk/vol1/fastq/ERR002/ERR002814/ERR002814_1.fastq.gz
    $ gunzip -c ERR002814_1.fastq.gz > bwt_files/ERR002814_1.fastq
    
# Make fm index:
    
    $ erl -pa ebin
    1> fm_index:make_indices("bwt_files/chr17.fa", ["chr17"]).
    Making fm-index for chromosome chr17 ...
    ...
    <it works something like 10 minutes>
    ...
    All fm-indices for chromosome chr17 are built
    No more chromosomes found
    
    quit erlang shell
    2> q().

# Setup and run HTTP storage

    wget http://nginx.org/download/nginx-1.9.4.tar.gz
    tar xvf nginx-1.9.4.tar.gz
    cd nginx-1.9.4
    ./configure
    make
    sudo make install

    $ ./scripts/make_refs_list.py > fm_indices/index.json
    $ python scripts/http/nginx-server.py . 8888
    
# Setup cluster
Make sure your user on one of the servers ('main' box) is allowed to rsh to all rest servers without being prompted for a password.
Each server must have the same compiled BWT source code in the same path.

# Start
From 'main' box:

    $ ./scripts/start_local.es bwt_files/ERR002814_1.fastq chr17 http://erlangonxen.org:8888 "[{box1,'erlangonxen.org',24},{box2,'46.4.100.178',24},{box4,'46.4.85.166',24}]" "[{vm,beam}]"
     you will see
    
    ERR002814.6748574 IL11_585:8:330:753:844/1      chr17_p3.fm      36352383      36M      72      TCCAGCTTGGGGGAGGGGTAGCTGCAGTAGTTTCCT
    ERR002814.6748588 IL11_585:8:330:797:680/1      chr17_p6.fm      90461772      36M      72      CTCTAACTACATTTAAACACTCACAAATCAAAGGAC
    ...
    <a lot of CIGARs>
    Fastq complited within 69.966899 secs.
    
    <manually terminate the process>
