-- Runs ONCE on first PostgreSQL container initialization
-- (docker-entrypoint-initdb.d), in the context of database ${APP_DB}
-- (default `app`) as user ${POSTGRES_USER} (default `bigdata`).
--
-- The `app` database itself is created automatically by the POSTGRES_DB
-- environment variable; this script creates the second database for the
-- Airflow metadata (one PostgreSQL instance, two databases — plan 4.1/5.4).

CREATE DATABASE airflow_meta;
