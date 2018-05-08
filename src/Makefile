#
# This file is in the Public Domain.
#

CFLAGS=		-std=gnu11 -O2 -g -W -Wextra -Werror -D_BSD_SOURCE
CFLAGS+=	-Wstrict-prototypes -Wmissing-prototypes -Wpointer-arith
CFLAGS+=	-Wshadow -Wcast-qual -Wwrite-strings

ifeq ($(DEBUG),1)
DEBUG=		1
endif
ifeq ($(MAKECMDGOALS),tests_st)
CFLAGS+=	-DNOSMP
DEBUG=		1
endif
ifeq ($(MAKECMDGOALS),tests_mt)
DEBUG=		1
endif
ifeq ($(MAKECMDGOALS),vis)
DEBUG=		1
endif

ifdef DEBUG
CFLAGS+=	-O0 -DDEBUG -fno-omit-frame-pointer
CFLAGS+=	-fprofile-arcs -ftest-coverage
else
CFLAGS+=	-DNDEBUG
endif

LIB=		libmasstree
INCS=		masstree.h

OBJS=		masstree.o

$(LIB).la:	LDFLAGS+=	-rpath $(LIBDIR)
install/%.la:	ILIBDIR=	$(DESTDIR)/$(LIBDIR)
install:	IINCDIR=	$(DESTDIR)/$(INCDIR)
#install:	IMANDIR=	$(DESTDIR)/$(MANDIR)/man3/

lib: $(LIB).la

%.lo: %.c
	libtool --mode=compile --tag CC $(CC) $(CFLAGS) -c $<

$(LIB).la: $(shell echo $(OBJS) | sed 's/\.o/\.lo/g')
	libtool --mode=link --tag CC $(CC) $(LDFLAGS) -o $@ $(notdir $^)

install/%.la: %.la
	mkdir -p $(ILIBDIR)
	libtool --mode=install install -c $(notdir $@) $(ILIBDIR)/$(notdir $@)

install: $(addprefix install/,$(LIB).la)
	libtool --mode=finish $(LIBDIR)
	mkdir -p $(IINCDIR) && install -c $(INCS) $(IINCDIR)
	#mkdir -p $(IMANDIR) && install -c $(MANS) $(IMANDIR)

tests_st: tests_st.o
	$(CC) $(CFLAGS) $(LDFLAGS) $< -o masstree_st_test -lpthread

tests_mt: tests_mt.o
	$(CC) $(CFLAGS) $(LDFLAGS) $< -o masstree_mt_test -lpthread

test:
	$(MAKE) tests_st
	$(MAKE) tests_mt
	./masstree_st_test
	#gcov -c -b ./tests_st.c
	./masstree_mt_test
	#gcov -c -b ./tests_mt.c

vis: vis.o
	$(CC) $(CFLAGS) $(LDFLAGS) $< -o masstree_vis
	./vis.sh

clean:
	libtool --mode=clean rm
	rm -rf .libs *.o *.lo *.la *.gcno *.gcov *.gcda
	rm -f masstree_vis

.PHONY: all lib install test vis clean
