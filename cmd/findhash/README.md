FindHash
========

This tool brute forces instance values for the default hash algorithm used
with carbon-relay.py.  The idea is to discover a solution where the spread
between the number of buckets on the hash ring associated with each hash
ring member is the smallest.  This should produce a more even distribution
of metrics through the cluster.

This serves (presently) no purpose for the Jump style hash ring.

Original Author: Jack Neely <jjneely@42lines.net>
2017/04/24

Step #1
-------

Generate a template file.  This is a text file of `SERVER:INSTANCE` pairs
separated by newlines.  Unknown instance values are left out (but not the
trailing colon).

Example:

    graphite-data-001:cb6f1823-126b-4fd6-9071-4c8b8392d9c8
    graphite-data-002:310e3cb4-1457-4fc4-8f5c-196437f8801c
    graphite-data-003:cf009318-5915-403a-bb1d-4c9a06907e06
    graphite-data-004:01510bf1-d82d-4b73-9e93-9e967bd0bb36
    graphite-data-005:

Save this text file.

Step #2
-------

Run `findhash` with this text file as the sole argument.

    $ ./findhash testme
    Possible solution:
            Max buckets per host: 13310
            Min buckets per host: 12980
            graphite-data-002:310e3cb4-1457-4fc4-8f5c-196437f8801c
            graphite-data-004:01510bf1-d82d-4b73-9e93-9e967bd0bb36
            graphite-data-003:cf009318-5915-403a-bb1d-4c9a06907e06
            graphite-data-001:cb6f1823-126b-4fd6-9071-4c8b8392d9c8
            graphite-data-005:fd8581de-1505-4d81-a10b-d1214e4356b6
    Possible solution:
            Max buckets per host: 13291
            Min buckets per host: 12984
            graphite-data-003:cf009318-5915-403a-bb1d-4c9a06907e06
            graphite-data-005:bef765eb-d0e6-4c10-ad94-6fe651397bea
            graphite-data-001:cb6f1823-126b-4fd6-9071-4c8b8392d9c8
            graphite-data-002:310e3cb4-1457-4fc4-8f5c-196437f8801c
            graphite-data-004:01510bf1-d82d-4b73-9e93-9e967bd0bb36
    Possible solution:
            Max buckets per host: 13182
            Min buckets per host: 12997
            graphite-data-004:01510bf1-d82d-4b73-9e93-9e967bd0bb36
            graphite-data-003:cf009318-5915-403a-bb1d-4c9a06907e06
            graphite-data-001:cb6f1823-126b-4fd6-9071-4c8b8392d9c8
            graphite-data-002:310e3cb4-1457-4fc4-8f5c-196437f8801c
            graphite-data-005:5ed781bb-8482-4aae-bfa5-76c9570d89bf

Results will continue as they are found.  The `-max` and `-min` options
will filter the results so that the range of buckets per host are
within the given min and/or max.

The `-filter` option will filter the result output as to only show the
servers matching the prefix passed to the `-filter` option.

Step #3
-------

Complete your hashring member file with the chosen solution so that the
defined hashring is complete.  Use the `-analyze` option to print a basic
analysis of the hashring.

	$ ./findhash -analyze testme
	Node graphite-data-001:cb6f1823-126b-4fd6-9071-4c8b8392d9c8:    13121
	Node graphite-data-002:310e3cb4-1457-4fc4-8f5c-196437f8801c:    13086
    Node graphite-data-003:cf009318-5915-403a-bb1d-4c9a06907e06:    13182
    Node graphite-data-004:01510bf1-d82d-4b73-9e93-9e967bd0bb36:    13149
    Node graphite-data-005:5ed781bb-8482-4aae-bfa5-76c9570d89bf:    12997
    
    Ideal bucket count per server: 13107.00
    Spread: 13182 - 12997 = 185
    Deviation: 63.4445

Step #4
-------

Build a list of Graphite metric keys and run an analysis on how they map
to nodes in the given hashring.  The list is newline separated.  This is
a sub function of the `-analyze` function.

    $ ./findhash -analyze -keys ood-test-metrics testme
    Node graphite-data-001:cb6f1823-126b-4fd6-9071-4c8b8392d9c8:    13121
    Node graphite-data-002:310e3cb4-1457-4fc4-8f5c-196437f8801c:    13086
    Node graphite-data-003:cf009318-5915-403a-bb1d-4c9a06907e06:    13182
    Node graphite-data-004:01510bf1-d82d-4b73-9e93-9e967bd0bb36:    13149
    Node graphite-data-005:5ed781bb-8482-4aae-bfa5-76c9570d89bf:    12997
    
    Ideal bucket count per server: 13107.00
    Spread: 13182 - 12997 = 185
    Deviation: 63.4445
    Keys per node:
    graphite-data-001:cb6f1823-126b-4fd6-9071-4c8b8392d9c8  141
    graphite-data-002:310e3cb4-1457-4fc4-8f5c-196437f8801c  146
    graphite-data-003:cf009318-5915-403a-bb1d-4c9a06907e06  172
    graphite-data-004:01510bf1-d82d-4b73-9e93-9e967bd0bb36  161
    graphite-data-005:5ed781bb-8482-4aae-bfa5-76c9570d89bf  139
    
    Total Metric Keys: 759
    Ideal keys per node: 151.80
    Deviation: 12.7028


