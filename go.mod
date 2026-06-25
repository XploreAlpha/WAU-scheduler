module github.com/wau/scheduler

go 1.25.0

require (
	github.com/redis/go-redis/v9 v9.20.1
	github.com/wau/registry v0.0.1
)

require (
	github.com/XploreAlpha/wau-trust v0.0.0-00010101000000-000000000000
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)

replace github.com/XploreAlpha/wau-trust => /home/inamoto888/project/wau-trust

replace github.com/wau/registry => ../wau-registry

replace github.com/wau/registry/registry => ../wau-registry/registry
