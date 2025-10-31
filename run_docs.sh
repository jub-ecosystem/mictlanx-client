#!/bin/bash
IMAGE_NAME="mictlanx-docs"
CONTAINER_NAME="mictlanx-mkdocs"

if [ "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]; then
    echo "Removing old container: $CONTAINER_NAME"
    docker rm -f $CONTAINER_NAME
fi

echo "Building image: $IMAGE_NAME"
docker build -t $IMAGE_NAME .

echo "Starting container: $CONTAINER_NAME"
docker run -d \
  --name $CONTAINER_NAME \
  -p 8000:8000 \
  $IMAGE_NAME

echo "Container $CONTAINER_NAME is running at http://localhost:8000"

