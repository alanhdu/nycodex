#!/bin/bash

# Setup Postgres

sudo -u postgres createuser --superuser adi
sudo -u postgres createdb -E UTF8 -l en_US.UTF8 -T template0 -O adi nycodex

sudo -u postgres psql -d nycodex <<EOL
ALTER ROLE adi WITH PASSWORD 'password';
EOL

sudo -u postgres psql -d nycodex -f setup.sql
