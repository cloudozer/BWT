#!/usr/bin/python
#
# genome reading functions
#  
#  
#
####################################################### 

import sys
from math import log10


def main(genome_file):
	print "File:", genome_file
	fragments_points = make_index(genome_file)
	if fragments_points == []:
		print "There is no genome sequence in the file:%s\n" % genome_file
	else:
		print "%i genomes found:" % len(fragments_points)
		


def make_index(data_file):
	res = []
	print "Reading file. Please wait..."
	with open(data_file) as f:
	        line = f.readline()
	        Cn = 0
	        N = 0
	        K = 0
	        ID = ''
	        while line != '':
	            LL = len(line)
	            if line[0] == '>':
	            	res.append((ID,N,K))
	            	K = 0
	            	N = Cn+LL
	            	ID = line.split()[0][1:] 
	            else:
	            	K += LL
	            
	            Cn += LL
	            line = f.readline()
	        res.append((ID,N,K))
	res.pop(0)
	f.close()

	with open(data_file+'.index','w') as f:
		for t in res:
			f.write(t[0]+" "+str(t[1])+" "+str(t[2])+"\n")
	f.close()

	return res





if __name__ == '__main__':
    args = sys.argv[:]
    if len(args) == 2:
        main(args[1])
    else:
        print "Usage: <python> <genome_read.py> <data_file>"
    