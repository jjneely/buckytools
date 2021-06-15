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

clean:
	$(RM) $(targets)
