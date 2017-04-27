Buckyd REST API Specification
=============================

/metrics
--------

Returns a JSON array listing the metrics on the local host.  May return a status
code of 202 Accepted when the internal cache is being rebuilt.  In that case
the client should sleep and try again.

Methods:

* GET
* POST

Query Parameters:

* list - This should be a JSON encoded array of Graphite metric keys.  The
  request will return any metrics in the list that are also present in the
  local store.
* force - Force a cache rebuild.  This will force the API to return a status
  code of 202 Accepted.
* regex - A regular expression.  Metric keys found locally that match this
  expression will be returned.

/metrics/<metric.key>
---------------------

Operates on specific metrics.  The metric key is the Graphite metric key
or name and not a file path.

Methods:

* HEAD - Stat the metric and return the results in a JSON encoded
  header field named X-Metric-Stat.
* GET - Fetch the raw Whisper DB file.  os.Stat() info in X-Metric-Stat.
* PUT - Replace the raw Whisper DB with supplied content.
* POST - Update the Whisper DB by backfilling the on disk version.  Does not
  overwrite existing points, but will fill in data if the matching on disk
  data point is null.  See Carbonate's whisper-fill.py.
* DELETE - Remove this metric from the file system.

GET requests will encode the response with Google's Snappy compression
algorithm when the header "Accept-Encoding: snappy" is present in the
headers of the GET request.  PUT and POST accept "Content-Encoding: snappy"
for Snappy compressed Whisper data as well.  Otherwise, the identity
encoding is assumed.  Encoding requests have no affect on HEAD or DELETE.

/hashring
---------

Return hashring information to the client.  The server doesn't actually do
anything with this data but store it and hand it to the client when asked.
This becomes a way for the client to discover the other members in the
consistent hashsing graphite cluster and to detect if all nodes are
setup the same way.

Methods:

* GET - Return a JSON encoded hash with two items: Name (the name of the
  current node) and Nodes (a list of all the server/instance pairs in the
  ring.
