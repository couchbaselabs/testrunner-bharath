#!/bin/bash
set -e

[[ "$1" == "couchbase-server" ]] && {
    echo "Starting Couchbase Server -- Web UI available at http://<ip>:8091"
    #echo $(ls /usr/sbin)
    #echo $(ls -lR /etc/runit)
    exec /usr/bin/runsvdir /etc/service/
}

exec "$@"
