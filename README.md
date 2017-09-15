Go Buckytools
==============

Tools for managing large consistent hashing Graphite clusters.

Buckminster (Bucky) Fuller once said,

> If you want to teach people a new way of thinking, don’t bother trying to teach
> them. Instead, give them a tool, the use of which will lead to new ways of
> thinking.

Carbon molecules known as fullerenes were also named after Bucky due
to their geodesic arrangement of atoms.  So, this is my contribution
to the Graphite naming scheme.

When working large consistent hashing Graphite clusters doing simple
maintenance can involve a lot of data moving around.  Normally, one would reach
for the Carbonate tools:

    https://github.com/jssjr/carbonate

These are good tools and I highly recommend them.

However, when the terabytes of WSP files and the number of storage nodes
stack up you start to find scaling problems:

* The python implementation of whisper-fill is slow
* There is no atomic method to rebalance a single metric
  resulting in query inconsistency for possible days after the
  metric was moved
* Handling renames and resulting backfills at scale is still
  difficult

So I wanted to use the speed and concurrency of Go to build more efficient
tools to help manage large consistent hashing cluster.

Tools
=====

These are the tools included and their functionality.

* **bucky-pickle-relay** -- A daemon that accepts and decodes Graphite's
  Pickle protocol and will tcp stream the equivalent plaintext version
  to a carbon-relay.  Useful with [carbon-c-relay][1].
* **bucky-fill** -- A `whisper-fill` compatible utility that is nearly
  an order of magnitude faster.
* **bucky-isempty** -- A utility for discovering WSP databases that
  contain no valid data points.
* **buckyd** -- A daemon for each Graphite node that tracks the
  configuration of the hash ring and exposes a REST API for
  interacting with the raw metric DBs on disk.
* **bucky** -- Command line Graphite cluster manager.  Modules:
  * **backfill** -- Backfill old metrics into new names.
  * **delete** -- Delete metrics via list or regular expression.
  * **du** -- Measure the storage consumed by a list of regular expression of
    metrics.
  * **inconsistent** -- Find metrics that are stored in the wrong server
    according to the hash ring.
  * **json** -- Convert newline separated lists to JSON arrays.
  * **list** -- Discover and verify metrics.
  * **locate** -- Calculate metric locations from the hash ring.
  * **rebalance** -- Move inconsistent metrics to the correct location
    and delete the source immediately after successful backfill.
  * **restore** -- Restore from a tar archive.
  * **servers** -- List each server's known hash ring and verify that
    all hash rings are consistent.
  * **tar** -- Make an archive of a list or regular expression of metric
    names and dump it in tar format to STDOUT.
* **gentestmetrics** -- Command that generates random Graphite style metrics
  to stdout purely for testing.
* **bucky-sparsify** -- Rewrites `.wsp` files into sparse files.

The heavy lifting commands use a set of worker threads to do IO work
which can be set at the command line with -w.

Assumptions
===========

These tools assume the following are true:

* You Graphite carbon-cache servers have one Whisper DB store.  Not multiple
  mount points with carbon-cache configured with their own DB store.
* Your hash ring is set to a `REPLICATION_FACTOR` of 1

These aren't set in stone, just what I was working with as I built the tool.  I
very much hope that some of these will be solved with further development.

Daemon Usage
============

Each data storage node in the Graphite cluster needs a **buckyd** daemon
running as the same user as the other Graphite tools.  I use an Upstart
job to keep mine running.  The important bit here is that you must
pass to the daemon as arguments the members of the consistent hash ring.

    $ cat /etc/init/buckyd.conf
    description "Buckyd Daemon for Managing Graphite Clusters"
    author      "Jack Neely <jjneely@42lines.net>"

    setuid graphite

    exec /path/to/buckyd -node graphite010-g5 \
        -sparse -hash carbon -b 192.168.1.1:5678 \
	graphite010-g5:a graphite010-g5:b \
	graphite011-g5:a graphite011-g5:b \
	graphite012-g5:a graphite012-g5:b

Here `-node` is the name of this Graphite node in the hashring (if different
from what is derived from the host name).  `-b` or `-bind` is the address to
bind to.  You can also specify `-prefix` where your Whisper data store is and
`-tmpdir` where the daemon can write temporary files.  The `-sparse` option
instructs buckyd to create sparse whisper files that take less disk space.
The `-hash` option chooses the hashring algorithm.

The non-option arguments
are the servers and instances that make up the hashring.  Order is important.
The hashring members can be specified in the following formats:

* `SERVER`
* `SERVER:INSTANCE`
* `SERVER:PORT:INSTANCE`

This exposes a REST API that is documented in REST_API_NOTES.md.

Client Usage
============

The **bucky** tool is self documenting.  You can run:

    bucky help

to see a list of modules and available flags and what arguments are needed.
Detailed help is available by specifying a module name:

    bucky help backfill

Most commands need a `--host` or `-h` flag to specify the initial Graphite
host to connect to where the client will discover the entire hash ring.
You can also set the `BUCKYHOST` environment variable rather than
specify this flag for each command.

Other common flags are:

* `-s` Operate only on the initial Graphite host.
* `-f` Requests the remote daemon to refresh its cache of local metrics.
* `-j` Read from STDIN or dump to STDOUT JSON data rather than text.
* `-r` Regular expression mode.
* `-w` Number of worker threads.

Examples
========

Rebalance a cluster with newly added storage nodes.  Check if you need to
use the `-no-delete` flag.  The default behavior is to move metrics and
delete the source after a successful copy.

    $ bucky rebalance -h graphite010-g5:4242 \
        -w 25 2>&1 | tee rebalance.log

Discover the exact storage used by a set of metrics:

    $ export BUCKYHOST=-h graphite010-g5:4242
    $ bucky du -r '^1min\.ipvs\.'

Make a backup of all of the metrics in the `carbon` namespace.  Using the
[pigz][2] parallel gzip compression tool.  (Normal gzip would otherwise bottleneck
the process.)

    $ bucky tar -w 25 -r '^carbon\.' | pigz > filename.tar

Backfill or rename metrics with a JSON hash of old name to new name.  This
does not delete the source metric.  It is a copy/fill operation.

    $ bucky backfill -w 25 foo.json

Find inconsistent metrics or metrics that are in the wrong place in the
cluster according to the hashring:

    $ bucky inconsistent

Building from Source
====================

To build from the Go source:

* Make sure your `GOPATH` environment variable is set to your Go
  [workspace][3].
* Run: `go get github.com/jjneely/buckytools`
* Change directory into `$GOPATH/src/github.com/jjneely/buckytools`
* Run: `go install ./...`
* Binaries should now be installed to `$GOPATH/bin`

This can also be built as a Debian/Ubuntu package.  (Tested on Ubuntu Trusty,
and Xenial.)  The [git-buildpackage][4] is what I use to produce builds.
This requires `golang` debian packages.

* Run: `gbp buildpackage`

Notes
=====

Deleting Metrics
----------------

The daemon makes no effort to remove possibly empty directories when deleting
a metric.  This can potentially cause race conditions with carbon-cache.py
creating a new metric in a would be deleted directory.  Once carbon-cache.py
closes the file handle to a file in a deleted directory that file will also
be deleted.  The delete action must not cause harm to other metrics.

To prune old or empty directories from your Graphite whisper store use a
cron job similar to this:

    /usr/bin/find ${prefix}/storage/whisper -type d -empty -mtime +1 -delete

This checks that the directory has not been modified in more than 1 day
which, in most cases, avoids race conditions.

Google Snappy Compression
-------------------------

To further scale the speed at which this tool will move metric data from
one location to another it uses Snappy compression by default.  This can be
disabled with the `-no-encoding` flag.  When using many workers this can
double (or more) the throughput.  The Snappy compression frame protocol also
handles CRC checks for data integrity.

To Do / Bugs / Contributing
===========================

Contributions are welcome!  Please make a GitHub pull request.  Below are
some low hanging fruit (and some more annoying issues) that need help.

* Unit tests with Go's `net/http/httptest` package.  Test that the buckyd
  daemon manipulates the on disk Whisper files correctly.
* Authentication -- Negotiate and Kerberos support.  Probably Basic as well.
* Make all modules aware of possible duplicate metrics.
* Retries
* Graceful restarts and shutdowns?  https://github.com/facebookgo/grace
* graphite-project/carbon's master branch contains this change:

    https://github.com/graphite-project/carbon/commit/024f9e67ca47619438951c59154c0dec0b

  This will cause a few metrics to be assigned a different position in the
  hash ring.  We need to account for this algorithm change somehow.

  Buckytools supports multiple different hashing algorithms and this can be
  setup as a different support hashing type.

[1]: https://github.com/grobian/carbon-c-relay
[2]: http://zlib.net/pigz/
[3]: https://golang.org/doc/code.html
[4]: http://honk.sigxcpu.org/projects/git-buildpackage/manual-html/gbp.html
