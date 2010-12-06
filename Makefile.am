ACLOCAL_AMFLAGS = -I m4

# includes append to these:
SUFFIXES =
PHONY =
TESTS =
check_PROGRAMS =
CLEANFILES =
bin_PROGRAMS =
noinst_HEADERS =
lib_LTLIBRARIES =
noinst_LTLIBRARIES =
noinst_PROGRAMS =
include_HEADERS =
nobase_include_HEADERS =
EXTRA_HEADERS =
BUILT_SOURCES=
EXTRA_DIST = 

SUBDIRS =

test-docs:
	(cd docs && $(MAKE) test-docs)
include cassandra/include.am
include coroebus/include.am
include tests/include.am

TESTS += ${check_PROGRAMS}


fedora:
	rm -f ~/rpmbuild/RPMS/x86_64/coroebus-$(VERSION)*.rpm
	rm -f ~/rpmbuild/SRPMS/coroebus-$(VERSION)*.rpm
	cp coroebus-$(VERSION).tar.gz ~/rpmbuild/SOURCES/
	rpmbuild -ba support/coroebus.spec
	cp ~/rpmbuild/RPMS/x86_64/coroebus-$(VERSION)*.rpm .
	cp ~/rpmbuild/SRPMS/coroebus-$(VERSION)*.rpm .

generic:
	rm -f ~/rpmbuild/RPMS/x86_64/coroebus-$(VERSION)*.rpm
	rm -f ~/rpmbuild/SRPMS/coroebus-$(VERSION)*.rpm
	cp coroebus-$(VERSION).tar.gz ~/rpmbuild/SOURCES/
	rpmbuild -ba support/coroebus.spec
	cp ~/rpmbuild/RPMS/x86_64/coroebus-$(VERSION)*.rpm .
	cp ~/rpmbuild/SRPMS/coroebus-$(VERSION)*.rpm .

rpm: all dist generic fedora

lcov: lcov-clean check
	@echo
	@echo "	------------------------------------------------------"
	@echo "Make sure ./configure was run with '--enable-coverage'"
	@echo "------------------------------------------------------"
	@echo
	cd coroebus && lcov --capture --directory . --base-directory .. --output-file lcov.out
	genhtml -o lcov --function-coverage -t coroebus coroebus/lcov.out

lcov-clean: clean
	rm -rf lcov */lcov.out 
	find . -name '*.gcno' | xargs rm -f
	find . -name '*.gcda' | xargs rm -f



