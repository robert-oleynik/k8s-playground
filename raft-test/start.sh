#!/bin/sh

tmux split-window -h 'ADDR="127.0.0.1:5000" PEER1="127.0.0.1:5001" PEER2="127.0.0.1:5002" go run .; sleep 10'
tmux split-window -v 'ADDR="127.0.0.1:5001" PEER1="127.0.0.1:5000" PEER2="127.0.0.1:5002" go run .; sleep 10'
tmux split-window -v 'ADDR="127.0.0.1:5002" PEER1="127.0.0.1:5000" PEER2="127.0.0.1:5001" go run .; sleep 10'
