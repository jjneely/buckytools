RM = rm -f
targets=bucky  bucky-fill  bucky-isempty  bucky-pickle-relay  bucky-sparsify  buckyd  findhash  gentestmetrics

PHONY: all

all: $(targets)

bucky:
	go build ./cmd/$@

bucky-fill:
	go build ./cmd/$@

bucky-isempty:
	go build ./cmd/$@

bucky-pickle-relay:
	go build ./cmd/$@

bucky-sparsify:
	go build ./cmd/$@

buckyd:
	go build ./cmd/$@

findhash:
	go build ./cmd/$@

gentestmetrics:
	go build ./cmd/$@

clean:
	$(RM) $(targets)
