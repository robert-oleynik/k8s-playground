#!/bin/bash

set -xe

# Generate PKL Config
pkl-gen-go ./tester/conf.schema.pkl --base-path github.com/robert-oleynik/k8s-playground

# Build raft storage image
docker build --tag k8s-storage --build-arg 'TARGET=storage' .

if [ "$1" = "--minikube" ]; then
	docker image save -o ./build/k8s-storage.tar k8s-storage:latest
	minikube image load build/k8s-storage.tar
	kubectl -n playground delete pods storage-0 storage-1 storage-2
fi
