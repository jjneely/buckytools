# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2017-08-17
### Added

* Support for FNV1a hashing compatible with [carbon-c-relay][1] hash method
  `fnv1a_ch`.  Issue #17

### Changed

* Server, Port, and Instance that uniquely identify a carbon-cache daemon
  in the hash ring (and tune how the hashring works) are now always specified
  by `SERVER[:PORT][=INSTANCE]`.  This is backwards incompatible, but fixes
  issues where the port and instance values could be confused.  Issue #17

## [0.3.2] 2017-06-21

### Fixed

* Support both Tuples and Lists which are now handled differently in the
  updated ogorek vendored package

## [0.3.1] 2017-06-21

### Added

* Unit tests for bucky-pickle-relay

### Changed

* Inverted delete option in bucky rebalance.  Delete is now off by default.
* Conform to Go best practices for repo layout
* Update vendored packages

### Fixed

* Fix tar/restore after Snappy changes

## [0.3.0] 2017-04-27

### Added

* Use Snappy framing format for Whisper data over the wire.  This makes
  transfer of time series databases significantly faster.

[1]: https://github.com/grobian/carbon-c-relay
