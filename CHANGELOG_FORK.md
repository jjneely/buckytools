# Changelog

This changelog documents specific changes added by `go-graphite` project members to this repo.

## [0.4.2-gg] - 2019-10-21

* Dep update go-whisper for bad point size fix (reference: go-graphite/go-whisper#7) (#14, @bom-d-van)
* Fix flock bug and vendor github.com/go-graphite/go-whisper (#13, @bom-d-van)
* fill: always enable flock when syncing whisper files (#12, @bom-d-van)
* Support cwhipser format (#11, @bom-d-van)
* Adopt github.com/go-graphite/go-whisper library (#10, @bom-d-van)

## [0.4.1-gg] - Unreleased

* Added docker-compose config to setup 3-node cluster locally (#5, @grzkv)
* Add modify command (#4, @bom-d-van)
* Fix building from go-graphite (#3, @azhiltsov)
* Having timeouts for http connections is a good idea. (#2, @azhiltsov)
* Fix for fill bug https://github.com/jjneely/buckytools/issues/19 (#1, @Civil)