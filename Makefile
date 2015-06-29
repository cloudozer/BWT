
.PHONY: default

FASTQ=bwt_files/SRR770176_1.fastq

default: deps $(FASTQ)
	rm -rf .railing
	./rebar compile
	./railing image -i bwt_files -i fm_indices -lsyntax_tools -lcompiler \
			-lcrypto -lasn1 -lpublic_key -lssh -iapps/master/priv/ssh_dir
	mv BWT.img bwtm.img
	./railing image -i bwt_files -i fm_indices -lsyntax_tools -lcompiler -x $(FASTQ)
	mv BWT.img bwtw.img

deps:
	./rebar get-deps

$(FASTQ):
	curl http://erlangonxen.org/bwt/SRR770176_1.fastq.xz -o $@.xz
	xz -d $@.xz
