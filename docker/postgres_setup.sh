#!/bin/bash

docker run \
        --detach \
        --volume /docker/moringa/week8/pgdata:/var/lib/postgresql/data \
        --env POSTGRES_PASSWORD=pg_W33k8 \
        --env POSTGRES_DB=call_log_db \
        --name week8_pg \
        --restart always \
        -p 5432:5432 \
        postgres:12-alpine
