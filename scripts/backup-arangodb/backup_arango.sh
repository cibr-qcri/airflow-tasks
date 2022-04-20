#!/bin/sh

arangodump \
  --server.endpoint "tcp://$ARANGO_HOST:$ARANGO_PORT" \
  --server.username "$ARANGO_USERNAME" \
  --all-databases true \
  --output-directory "/backups/arangodb/" \
  --overwrite	true
