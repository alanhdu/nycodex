FROM postgres:11.7

RUN apt-get update --yes && \
    apt-get install --yes postgresql-11-postgis-3
