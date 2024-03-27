#!/bin/bash

tmux split-window -h 'IDX=0 go run .; echo "Press Enter to exit"; read'
sleep 1
tmux split-window -v 'IDX=1 go run .; echo "Press Enter to exit"; read'
sleep 1
tmux split-window -v 'IDX=2 go run .; echo "Press Enter to exit"; read'
