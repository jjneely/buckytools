# Buckytools Makefile
#
# This Makefile is mostly for Debian packaging purposes, however
# it does build all the tools and the Bucky client/server.  We
# package built binaries here as a recent Go compiler is not
# packages/available for Precise.
#
# Nothing done here, we don't compile in the DSC build process
all:

build: *.go bucky buckyd bucky-fill bucky-isempty

.PHONY: bucky buckyd bucky-fill bucky-isempty
bucky buckyd bucky-fill bucky-isempty:
	cd $@; go build

install:
	install -D -m 0755 bucky/bucky $(DESTDIR)/usr/bin/bucky
	install -D -m 0755 buckyd/buckyd $(DESTDIR)/usr/bin/buckyd
	install -D -m 0755 bucky-fill/bucky-fill $(DESTDIR)/usr/bin/bucky-fill
	install -D -m 0755 bucky-isempty/bucky-isempty $(DESTDIR)/usr/bin/bucky-isempty

dsc:
	dpkg-source -b .

clean:
	find . -name \*~ -delete
	find . -name \*.wsp -delete
