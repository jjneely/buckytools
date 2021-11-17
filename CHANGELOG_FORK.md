# Changelog

This changelog documents specific changes added by `go-graphite` project members to this repo.

## [0.5.2-gg] - 2021-11-17

* Support cwhipser format by @bom-d-van in https://github.com/go-graphite/buckytools/pull/11
* fill: always enable flock when syncing whisper files by @bom-d-van in https://github.com/go-graphite/buckytools/pull/12
* Fix flock bug and vendor github.com/go-graphite/go-whisper by @bom-d-van in https://github.com/go-graphite/buckytools/pull/13
* Dep update go-whisper for bad point size fix by @bom-d-van in https://github.com/go-graphite/buckytools/pull/14
* Merge remote-tracking branch 'jjneely/master' into master by @iain-buclaw-sociomantic in https://github.com/go-graphite/buckytools/pull/15
* Update CHANGELOG.md by @deniszh in https://github.com/go-graphite/buckytools/pull/16
* Adding modify command documentation to README by @deniszh in https://github.com/go-graphite/buckytools/pull/17
* Fix package builds of go-graphite/buckytools by @iain-buclaw-sociomantic in https://github.com/go-graphite/buckytools/pull/18
* dep update github.com/go-graphite/go-whisper by @iain-buclaw-sociomantic in https://github.com/go-graphite/buckytools/pull/19
* Update go-whisper for fixing a old interval trimming bug by @bom-d-van in https://github.com/go-graphite/buckytools/pull/20
* buckyd/cwhipser spt by @bom-d-van in https://github.com/go-graphite/buckytools/pull/23
* make http timeout parameterizable by @dams in https://github.com/go-graphite/buckytools/pull/24
* Migrate to go module by @bom-d-van in https://github.com/go-graphite/buckytools/pull/25
* rebalance: new offload feature/flag to speed up rebalancing by @bom-d-van in https://github.com/go-graphite/buckytools/pull/26
* rebalance: introduce go-carbon health check with better sync rate control by @bom-d-van in https://github.com/go-graphite/buckytools/pull/27
* rebalance: use -metrics-per-second when resetting sync speed with health check by @bom-d-van in https://github.com/go-graphite/buckytools/pull/29


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
