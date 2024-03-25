package main

import (
	"log"
	"net"
	"os"

	"github.com/robert-oleynik/k8s-playground/raft"
	"go.uber.org/zap"
)

type State struct {
	Value int
}

func (state *State) Apply(command interface{}) error {
	value := command.(int)
	state.Value = value
	return nil
}

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("failed to init logger: %v", err)
	}
	defer logger.Sync()
	zap.ReplaceGlobals(logger)

	addr := os.Getenv("ADDR")
	peer1 := os.Getenv("PEER1")
	peer2 := os.Getenv("PEER2")
	peers := []string{}
	if peer1 != "" {
		peers = append(peers, peer1)
	}
	if peer2 != "" {
		peers = append(peers, peer2)
	}
	r := raft.NewWithConfig(raft.DebugConfig())
	r.Log = raft.NewDevelopmentLog()
	r.Peer = raft.NewDevelopmentPeers(peers)
	zap.L().Sugar().Infof("listening on %s", addr)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal("failed to listen for socket", zap.Error(err))
	}
	if err := r.Serve(listener); err != nil {
		logger.Fatal("session failed", zap.Error(err))
	}
}
