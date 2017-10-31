#!/bin/bash

# Setup Postgres

sudo -u postgres createuser --superuser adi
sudo -u postgres createdb -E UTF8 -l en_US.UTF8 -T template0 -O adi nycodex

sudo -u postgres psql -d nycodex <<EOL
ALTER ROLE adi WITH PASSWORD 'password';
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS api;
CREATE SCHEMA IF NOT EXISTS metadata;
EOL
