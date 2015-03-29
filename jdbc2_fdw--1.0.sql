/* contrib/jdbc2_fdw/jdbc2_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION jdbc2_fdw" to load this file. \quit

CREATE FUNCTION jdbc2_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION jdbc2_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER jdbc2_fdw
  HANDLER jdbc2_fdw_handler
  VALIDATOR jdbc2_fdw_validator;
