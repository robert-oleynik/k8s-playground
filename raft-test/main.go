package main

import (
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"

	"github.com/robert-oleynik/k8s-playground/raft"
	"go.uber.org/zap"
)

type State struct {
	Value int
}

func (state *State) Apply(command int) error {
	state.Value = command
	return nil
}

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("failed to init logger: %v", err)
	}
	defer logger.Sync()
	zap.ReplaceGlobals(logger)

	idx, err := strconv.Atoi(os.Getenv("IDX"))
	if err != nil {
		logger.Fatal("err", zap.Error(err))
	}

	host := "127.0.0.1"
	ports := []uint16{5000, 5001, 5002}
	self := raft.NewPeer(rand.Uint32(), host, ports[idx], 0)
	if self.Id == 0 {
		self.Id = 1
	}
	peers := raft.NewPeers(self)
	for i, port := range ports {
		if i == idx {
			continue
		}
		zap.L().Info("join peer", zap.Int("idx", i))
		peer := raft.NewPeer(0, host, port, 0)
		if err := peers.Join(peer); err != nil {
			zap.L().Error("failed to join raft cluster", zap.Error(err))
			continue
		}
		break
	}
	zap.L().Info("join complete")

	r := raft.NewWithConfig[int](raft.DebugConfig())
	r.Peers = peers
	r.Log = raft.NewDevelopmentLog[int]()
	zap.L().Sugar().Infof("listening on %s:%d", host, ports[idx])
	listener, err := net.Listen("tcp", r.Peers.Self.Addr.String())
	if err != nil {
		logger.Fatal("failed to listen for socket", zap.Error(err))
	}
	if err := r.Serve(listener); err != nil {
		logger.Fatal("session failed", zap.Error(err))
	}
}
