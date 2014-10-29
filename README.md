# Burrows-Wheeler Transform

Compression techniques work by finding repeated patterns in the data and encoding the duplications more compactly.
http://en.wikipedia.org/wiki/Burrows%E2%80%93Wheeler_transform

## How to run

1. download bwt.erl
2. launch Erlang shell
3. compile:  c(bwt)
4. run bwt:bwt(String)

## Example
	1> bwt:bwt("BANANA") 
	> Output: BNN$AAA
	[66,78,78,"$",65,65,65]
	2> 


# Smith-Waterman Algorithm 

The Smith–Waterman algorithm performs local sequence alignment; that is, for determining similar regions between two strings or nucleotide or protein sequences.
http://en.wikipedia.org/wiki/Smith%E2%80%93Waterman_algorithm

## How to run
1. download sw.erl
2. launch Erlang shell
3. compile: c(sw)
4. run: sw:sw(String1,String2).

## Example
	2> sw:sw("AGGTCA","CGCT").
	GTC
	| |
	G-C
	ok
	3> 

You may also run sw function with random sequences of any length as:

	1> sw:sw(20,40).
	CAGAGAAGCGT-GCG--TGC
	||| |  || | |||  |||
	CAG-G--GCATCGCGTTTGC
	ok
	2> 
