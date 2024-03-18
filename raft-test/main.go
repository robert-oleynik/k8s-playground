package main

import (
	"log"
	"os"

	"github.com/robert-oleynik/snippets/raft"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("failed to init logger: %v", err)
	}
	defer logger.Sync()

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
	config := raft.DefaultConfig()
	config.Addr = addr
	if err := raft.Start(logger, config, peers); err != nil {
		logger.Fatal("failed to create raft server", zap.Error(err))
	}
}
