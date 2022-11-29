RM = rm -f
targets=bucky  bucky-fill  bucky-isempty  bucky-pickle-relay  bucky-sparsify  buckyd  findhash  gentestmetrics

SHELL := /bin/bash
OS := $(shell uname)

PHONY: all

all: $(targets)

bucky:
	go build -mod vendor ./cmd/$@

bucky-fill:
	go build -mod vendor ./cmd/$@

bucky-isempty:
	go build -mod vendor ./cmd/$@

bucky-pickle-relay:
	go build -mod vendor ./cmd/$@

bucky-sparsify:
	go build -mod vendor ./cmd/$@

buckyd:
	go build -mod vendor ./cmd/$@

findhash:
	go build -mod vendor ./cmd/$@

gentestmetrics:
	go build -mod vendor ./cmd/$@

test:
	pushd ./testing/data && python3 gentests.py && popd
	go test ./...

# To keep testdata after running test for debugging, run the following command:
#
# 	make e2e_test REBALANCE_FLAGS=-keep-testdata
#
# Run test_setup_$os first
e2e_test: clean bucky buckyd
	go run -mod vendor testing/copy/main.go $(COPY_FLAGS)
	go run -mod vendor testing/rebalance/main.go $(REBALANCE_FLAGS)
	go run -mod vendor testing/backfill2/main.go $(BACKFILL_FLAGS)

e2e_test_rebalance_health_check: clean bucky buckyd
	go run -mod vendor testing/rebalance_health_check/main.go $(REBALANCE_FLAGS)

e2e_test_setup:
ifeq ($(OS),Linux)
	sudo ip addr add 127.0.1.7 dev lo
	sudo ip addr add 127.0.1.8 dev lo
	sudo ip addr add 127.0.1.9 dev lo
endif
ifeq ($(OS),Darwin)
	sudo ifconfig lo0 alias 127.0.1.7 up
	sudo ifconfig lo0 alias 127.0.1.8 up
	sudo ifconfig lo0 alias 127.0.1.9 up
endif

e2e_test_setup_clean:
ifeq ($(OS),Linux)
	sudo ip addr del 127.0.1.7/32 dev lo
	sudo ip addr del 127.0.1.8/32 dev lo
	sudo ip addr del 127.0.1.9/32 dev lo
endif
ifeq ($(OS),Darwin)
	sudo ifconfig lo0 -alias 127.0.1.7
	sudo ifconfig lo0 -alias 127.0.1.8
	sudo ifconfig lo0 -alias 127.0.1.9
endif

clean_test:
	rm -rf bucky buckyd
	rm -rf testdata_rebalance_*
	rm -rf testdata_copy_*
	rm -rf testdata_backfill2_*
	rm -rf testdata_rebalance_health_check_*
	rm -rf testing/data/*.line
	rm -rf testing/data/*.pickle
	rm -rf fill/*.wsp
	rm -rf cmd/bucky-sparsify/sourcefile
	rm -rf cmd/bucky-sparsify/sourcefile.sparse

clean: clean_test
	$(RM) $(targets)
