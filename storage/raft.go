package main

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/google/uuid"
	"github.com/robert-oleynik/k8s-playground/raft"
	"go.uber.org/zap"
)

var raftServer *raft.Raft[RaftCommand] = nil
var raftState *State = nil

type State struct {
	guard *sync.RWMutex
	data  map[uuid.UUID][]byte
}

type RaftCommand struct {
	Id     uuid.UUID
	Data   []byte
	Delete bool
}

func LaunchRaftWithContext(port uint16, serviceConf ServiceConfig, ctx context.Context) error {
	conf := raft.DebugConfig()

	r := raft.NewWithConfig[RaftCommand](conf)
	if serviceConf.Id != 0 {
		r.Id = serviceConf.Id
		zap.S().Infow("raft", "id", r.Id)
	}
	raftState = &State{
		guard: &sync.RWMutex{},
		data:  make(map[uuid.UUID][]byte),
	}
	go func() {
		for {
			select {
			case cmd := <-r.Apply:
				raftState.Apply(cmd)
			case <-ctx.Done():
				return
			}
		}
	}()
	r.Log = raft.NewMemoryLog[RaftCommand]()
	go func() {
		if err := StartDiscoverer(r, serviceConf); err != nil {
			zap.L().Error("discoverer failed", zap.Error(err))
		}
	}()

	listener, err := net.Listen("tcp", fmt.Sprintf("[::]:%d", port))
	if err != nil {
		return fmt.Errorf("listener: %w", err)
	}
	raftServer = r
	if err := r.Serve(listener); err != nil {
		return fmt.Errorf("raft: %w", err)
	}
	return nil
}

func (state *State) Apply(cmd RaftCommand) error {
	state.guard.Lock()
	defer state.guard.Unlock()
	if cmd.Delete {
		delete(state.data, cmd.Id)
	} else {
		state.data[cmd.Id] = cmd.Data
	}
	return nil
}

func (state *State) Get(id uuid.UUID) ([]byte, bool) {
	state.guard.RLock()
	defer state.guard.RUnlock()
	content, ok := state.data[id]
	return content, ok
}
