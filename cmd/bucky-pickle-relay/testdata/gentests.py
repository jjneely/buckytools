#!/usr/bin/python

import pickle
import time

from types import StringType, FloatType, ListType, TupleType

def ts():
    return time.time()

data = {
    "invalid.000": { "foo": "bar" },
    "invalid.001": [ "string" ],
    "invalid.002": [ [ 5, ( ts(), 42 ) ] ],

    "test.001"   : [ [ "test.001", [ ts(), 42 ] ],
                     [ "test.002", [ ts(), 43 ] ],
                     [ "test.003", ( ts(), 44 ) ],
                     ( "test.004", ( ts(), 45 ) ),
                   ],

    "test.002"   : [ [ "test.001", [ str(ts()), 42 ] ],
                     [ "test.002", [ ts(), "43" ] ],
                     [ "test.003", ( str(ts()), "44" ) ],
                     ( "test.004", ( str(ts()), "3.14159265358979323846264338327950288419716939937510" ) ),
                   ],

    "test.003"   : [ [ "test.001", [ int(ts()), 42 ] ],
                     [ "test.002", [ int(ts()), 43 ] ],
                     [ "test.003", ( int(ts()), 44 ) ],
                     ( "test.004", ( int(ts()), 45 ) ),
                   ],

    "test.004"   : [ [ "test.001", [ ts(), 42.3456 ] ],
                     [ "test.002", [ ts(), 3.14159265358979323846 ] ],
                     [ "test.003", ( ts(), 2.71828 ) ],
                     ( "test.004", ( ts(), -9.7 ) ),
                   ],

    "test.005"   : [ [ "test.001", [ ts(), (1<<64) - 1 ] ],
                     [ "test.002", [ ts(), (1<<64) + 0 ] ],
                     [ "test.003", ( ts(), (1<<64) + 1 ) ],
                     ( "test.004", ( ts(), (1<<64) + 2 ) ),
                   ],

    "test.006"   : ( [ "test.001", [ int(ts()), 3.14 ] ],
                     [ "test.002", [ int(ts()), 43 ] ],
                     [ "test.003", ( int(ts()), 44 ) ],
                     ( "test.004", ( int(ts()), 45 ) ),
                   ),
}

if __name__ == "__main__":
    for key in data.keys():
        with open("%s.pickle" % key, 'w') as fd:
            pickle.dump(data[key], fd)
        with open("%s.line" % key, 'w') as fd:
            if type(data[key]) != ListType and type(data[key]) != TupleType:
                continue
            for l in data[key]:
                if len(l) != 2:
                    continue  # invalid pickle format will be dropped
                if type(l[0]) != StringType:
                    continue
                if type(l[1]) != ListType and type(l[1]) != TupleType:
                    continue
                if len(l[1]) != 2:
                    continue

                # Format floats the way Go will
                dp = []
                if type(l[1][0]) == FloatType:
                    dp.append("%.12f" % l[1][0])
                else:
                    dp.append(l[1][0])
                if type(l[1][1]) == FloatType:
                    dp.append("%.12f" % l[1][1])
                else:
                    dp.append(l[1][1])

                s = "%s %s %s\n" % (l[0], dp[1], dp[0])
                fd.write(s)

