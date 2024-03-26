#!/bin/sh

tmux split-window -h 'IDX=0 go run .; bash'
tmux split-window -v 'IDX=1 go run .; bash'
tmux split-window -v 'IDX=2 go run .; bash'
