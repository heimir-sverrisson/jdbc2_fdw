# contrib/jdbc2_fdw/Makefile

MODULE_big = jdbc2_fdw
OBJS = jdbc2_fdw.o option.o deparse.o connection.o

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

EXTENSION = jdbc2_fdw
DATA = jdbc2_fdw--1.0.sql

REGRESS = jdbc2s_fdw

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
