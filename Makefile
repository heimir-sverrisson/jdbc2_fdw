# contrib/jdbc2_fdw/Makefile

MODULE_big = jdbc2_fdw
OBJS = jdbc2_fdw.o option.o deparse.o connection.o jq.o

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

EXTENSION = jdbc2_fdw
DATA = jdbc2_fdw--1.0.sql

REGRESS = jdbc2_fdw

JDBC_CONFIG = jdbc_config

SHLIB_LINK += -ljvm

UNAME = $(shell uname)

# Special treatment for Mac OS X
ifeq ($(UNAME), Darwin)
#	SHLIB_LINK = -I/System/Library/Frameworks/JavaVM.framework/Headers -L/System/Library/Frameworks/JavaVM.framework/Libraries -ljvm -framework JavaVM
	SHLIB_LINK += -I$(JAVA_HOME)/include -L$(JAVA_HOME)/jre/lib -framework JavaVM
endif


TRGTS = JAVAFILES

JAVA_SOURCES = \
	JDBCUtils.java \
	JDBCDriverLoader.java \
 
PG_CPPFLAGS=-D'PKG_LIB_DIR=$(pkglibdir)' -I$(libpq_srcdir)

JFLAGS = -d $(pkglibdir)

# all:$(TRGTS)

#JAVAFILES:
#	javac $(JFLAGS) $(JAVA_SOURCES)

# the db name is hard-coded in the tests
override USE_MODULE_DB =

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/jdbc2_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
