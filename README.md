Go Buckytools
==============

Buckyballs and buckytubes are fullerene of carbon or carbon molecules such
as a Buckminsterfullerene.  They are similar to graphite but the carbon
structures are regular.  Hopefully, this code will allow multiple Graphite
servers to work and scale better together.

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

So I wanted to use the speed of Go to build more efficient tools to help
manage large consistent hashing cluster.  Goals:

* Make a whisper-fill compatible binary that's an order of magnitude
  faster
* Make an atomic operation to rebalance or relocate an existing metric
  or close to it
* Build some higher level useful tools for managing a cluster:
  * Build Graphite webui's search index
  * Make arbitrary tar files of metrics
  * List metrics from regex/list
  * Fetch/Ship raw WSP data
  * Find/relocate metrics in the wrong place
  * Rename/backfill from regex or hash/list
  * Restore arbitrary tar files of metrics
  * Sanity, clean empty directories
  * delete metrics from list/regex
  * delete metrics that don't belong on each host
  * tar up metrics that don't belong on each host

Assumptions
===========

These tools assume the following are true:

* You Graphite carbon-cache servers have one Whisper DB store.  Not multiple
  mount points with carbon-cache configured with their own DB store.

Notes
=====

I have a set of tasks written in Fabric that do this for me currently.  They
are slow and poor in many ways.  I want to replace them with this code base.
The Fabric tasks are:

    graphite2.backfill
    graphite2.backfillFromMap
    graphite2.deleteDontBelong
    graphite2.deleteEmptyDirectories
    graphite2.deleteMetrics
    graphite2.migrate
    graphite2.restoreTar
    graphite2.sanity
    graphite2.showDontBelong
    graphite2.showMetrics
    graphite2.tar
    graphite2.tarDontBelong

