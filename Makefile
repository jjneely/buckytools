RM = rm -f
targets=bucky  bucky-fill  bucky-isempty  bucky-pickle-relay  bucky-sparsify  buckyd  findhash  gentestmetrics

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

# To keep testdata after running test for debugging, run the following command:
#
# 	make test REBALANCE_FLAGS=-keep-testdata
#
# Run test_setup_$os first
test: clean bucky buckyd
	go run -mod vendor testing/copy.go $(COPY_FLAGS)
	go run -mod vendor testing/rebalance.go $(REBALANCE_FLAGS)
	go run -mod vendor testing/backfill2.go $(BACKFILL_FLAGS)

# only works on linux
test_setup_linux:
	sudo ip addr add 10.0.1.7 dev lo
	sudo ip addr add 10.0.1.8 dev lo
	sudo ip addr add 10.0.1.9 dev lo

test_setup_clean_linux:
	sudo ip addr del 10.0.1.7/32 dev lo
	sudo ip addr del 10.0.1.8/32 dev lo
	sudo ip addr del 10.0.1.9/32 dev lo

test_setup_osx:
	sudo ifconfig lo0 alias 10.0.1.7 up
	sudo ifconfig lo0 alias 10.0.1.8 up
	sudo ifconfig lo0 alias 10.0.1.9 up

test_setup_clean_osx:
	sudo ifconfig lo0 -alias 10.0.1.7
	sudo ifconfig lo0 -alias 10.0.1.8
	sudo ifconfig lo0 -alias 10.0.1.9

test_rebalance_health_check: clean bucky buckyd
	go run -mod vendor testing/rebalance_health_check.go $(REBALANCE_FLAGS)

clean_test:
	rm -rf bucky buckyd
	rm -rf testdata_rebalance_*
	rm -rf testdata_copy_*
	rm -rf testdata_backfill2_*
	rm -rf testdata_rebalance_health_check_*

clean:
	$(RM) $(targets)
