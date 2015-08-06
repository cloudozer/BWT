-define(THRESHOLD, 20).

%% seed finding algorithm parameters
-define(TOLERANCE,20). % a max indel between aligned parts 
-define(MIN_LEN,11). % a minimal length of the string that should be matched
-define(MAX_RANGE,45).
-define(MAX_LEN,15). % a maximal length of the string after which the range should be less than MAX_RANGE

%% suffix array construction algorithm parameters
-define(SORTED_MAX,1000000). %maximal length of array to sort


%% Smith-Waterman sigma function parameters
-define(MATCH,2).
-define(UNKNOWN,1.6).
-define(MISMATCH,-1).
-define(GAP_PENALTY,-2).
-define(GAP_EXT_PENALTY,-1).

-define(UNDEF,0).

