# Buckytools Makefile
#
# This Makefile is mostly for Debian packaging purposes, however
# it does build all the tools and the Bucky client/server.  We
# package built binaries here as a recent Go compiler is not
# packages/available for Precise.
#
# Nothing done here, we don't compile in the DSC build process

commands = bucky buckyd bucky-fill bucky-isempty bucky-pickle-relay \
		   bucky-sparsify

all: build

build: *.go $(commands)

.PHONY: $(commands)
$(commands):
	cd $@; go build

install:
	$(foreach c,$(commands), install -D -m 0755 $(c)/$(c) $(DESTDIR)/usr/bin/$(c);)

dsc:
	dpkg-source -b .

clean:
	$(foreach c,$(commands), rm -f $(c)/$(c);)
	find . -name \*~ -delete
	find . -name \*.wsp -delete
