#!/usr/bin/env bash

# wait-for-it.sh is a script that blocks until a specified host and port are available.

HOST=$1
PORT=$2
shift 2
CMD="$@"

echo "Waiting for $HOST:$PORT to be available..."

while ! nc -z $HOST $PORT; do
  sleep 1
done

echo "$HOST:$PORT is available! Running command..."
exec $CMD
