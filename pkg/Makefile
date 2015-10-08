NAME=	libmasstree

all: rpm

rpm:
	mkdir -p SOURCES && tar czpvf SOURCES/$(NAME).tar.gz ../src
	rpmbuild -ba -v --define "_topdir ${PWD}" SPECS/$(NAME).spec

clean:
	rm -rf BUILD BUILDROOT RPMS SOURCES SRPMS

.PHONY: all rpm clean
