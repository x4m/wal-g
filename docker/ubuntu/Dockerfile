FROM ubuntu:18.04

ENV DEBIAN_FRONTEND noninteractive
ENV TERM xterm-256color

ENV PGDATA /var/lib/postgresql/10/main

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    htop \
    nano \
    postgresql-10 \
    postgresql-contrib \
    liblzo2-2 \
    rm -rf /var/lib/apt/lists/*

RUN rm -rf $PGDATA
