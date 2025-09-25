#!/bin/bash

echo "Creating [mictlanx] network..."

docker network create --driver=bridge mictlanx || true

echo "Removing existing peers"
docker compose -f mictlanx-peer.yml down || true

echo "Starting a new peer"
docker compose -f mictlanx-peer.yml up -d
