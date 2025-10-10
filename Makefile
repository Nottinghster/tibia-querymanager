SRCDIR = src
BUILDDIR = build
OUTPUTEXE = querymanager

CC = gcc
CXX = g++
CFLAGS = -m64 -fno-strict-aliasing -pedantic -Wno-unused-parameter -Wall -Wextra -pthread
CXXFLAGS = $(CFLAGS) --std=c++11
LFLAGS = -Wl,-t

# IMPORTANT(fusion): Using TABS outside recipes may cause errors in some cases.
# This is because .RECIPEPREFIX defaults to it and any line starting with it is
# assumed to introduce another recipe command. If indentation is needed before
# any target is specified, use SPACES.

DEBUG ?= 0
ifneq ($(DEBUG), 0)
  CFLAGS += -g -Og -DENABLE_ASSERTIONS=1
else
  CFLAGS += -O2
endif

DATABASE ?= sqlite
ifeq ($(DATABASE), sqlite)
  DATABASEOBJ = $(BUILDDIR)/database_sqlite.obj $(BUILDDIR)/sqlite3.obj
  CFLAGS += -DDATABASE_SQLITE=1
else ifeq ($(DATABASE), postgres)
  DATABASEOBJ = $(BUILDDIR)/database_postgres.obj
  CFLAGS += -DDATABASE_POSTGRESQL=1
  LFLAGS += -lpq
else ifeq ($(DATABASE), mariadb)
  DATABASEOBJ = $(BUILDDIR)/database_mysql.obj
  CFLAGS += -DDATABASE_MYSQL=1 -DDATABASE_MARIADB=1
  LFLAGS += -lmariadb
else
  $(error Unsupported DATABASE: `$(DATABASE)`. Valid options are `sqlite`, `postgres`, or `mariadb`)
endif

$(BUILDDIR)/$(OUTPUTEXE): $(BUILDDIR)/connections.obj $(BUILDDIR)/hostcache.obj $(BUILDDIR)/query.obj $(BUILDDIR)/querymanager.obj $(BUILDDIR)/sha256.obj $(DATABASEOBJ)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LFLAGS)

$(BUILDDIR)/connections.obj: $(SRCDIR)/connections.cc $(SRCDIR)/querymanager.hh
	@mkdir -p $(@D)
	$(CXX) -c $(CXXFLAGS) -o $@ $<

$(BUILDDIR)/hostcache.obj: $(SRCDIR)/hostcache.cc $(SRCDIR)/querymanager.hh
	@mkdir -p $(@D)
	$(CXX) -c $(CXXFLAGS) -o $@ $<

$(BUILDDIR)/query.obj: $(SRCDIR)/query.cc $(SRCDIR)/querymanager.hh
	@mkdir -p $(@D)
	$(CXX) -c $(CXXFLAGS) -o $@ $<

$(BUILDDIR)/querymanager.obj: $(SRCDIR)/querymanager.cc $(SRCDIR)/querymanager.hh
	@mkdir -p $(@D)
	$(CXX) -c $(CXXFLAGS) -o $@ $<

$(BUILDDIR)/sha256.obj: $(SRCDIR)/sha256.cc $(SRCDIR)/querymanager.hh
	@mkdir -p $(@D)
	$(CXX) -c $(CXXFLAGS) -o $@ $<

$(BUILDDIR)/database_sqlite.obj: $(SRCDIR)/database_sqlite.cc $(SRCDIR)/querymanager.hh
	@mkdir -p $(@D)
	$(CXX) -c $(CXXFLAGS) -o $@ $<

$(BUILDDIR)/database_postgres.obj: $(SRCDIR)/database_postgres.cc $(SRCDIR)/querymanager.hh
	@mkdir -p $(@D)
	$(CXX) -c $(CXXFLAGS) -o $@ $<

$(BUILDDIR)/database_mysql.obj: $(SRCDIR)/database_mysql.cc $(SRCDIR)/querymanager.hh
	@mkdir -p $(@D)
	$(CXX) -c $(CXXFLAGS) -o $@ $<

$(BUILDDIR)/sqlite3.obj: $(SRCDIR)/sqlite3.c $(SRCDIR)/sqlite3.h
	@mkdir -p $(@D)
	$(CC) -c $(CFLAGS) -o $@ $<

.PHONY: clean

clean:
	@rm -rf $(BUILDDIR)

