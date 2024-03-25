package raft

import (
	"fmt"
	"net/rpc"
	"time"

	"go.uber.org/zap"
)

type PeerManager interface {
	Discover() error
	Broadcast(method string, args any, timeout time.Duration) (bool, error)
	Peers() []Peer
}

type Peers []Peer

type Peer struct {
	client *rpc.Client
	Id     uint64
	Addr   string
	Role   Role
}

func (self *Peer) Call(serviceMethod string, args any, reply any) error {
	if self.client == nil {
		client, err := rpc.DialHTTP("tcp", self.Addr)
		if err != nil {
			return fmt.Errorf("dial http: %w", err)
		}
		self.client = client
	}
	if err := self.client.Call(serviceMethod, args, reply); err != nil {
		return fmt.Errorf("service call: %w", err)
	}
	return nil
}

func (self *Peer) RequestVote(args VoteRequest) (Reply, error) {
	var reply Reply
	err := self.Call("Raft.RequestVote", args, &reply)
	return reply, err
}

func (self *Peer) AppendEntries(args AppendEntriesRequest) (Reply, error) {
	var reply Reply
	err := self.Call("Raft.AppendEntries", args, &reply)
	return reply, err
}

func NewDevelopmentPeers(peerAddrs []string) Peers {
	peers := make([]Peer, len(peerAddrs))
	for i, addr := range peerAddrs {
		peers[i] = Peer{Addr: addr}
	}
	return Peers(peers)
}

func (peers Peers) Discover() error {
	return nil
}

func (peers Peers) Peers() []Peer {
	return peers
}

func (peers Peers) Broadcast(method string, args any, timeout time.Duration) (bool, error) {
	zap.L().Debug("raft: starting broadcast",
		zap.String("method", method),
		zap.Any("args", args),
		zap.Duration("timeout", timeout))
	ch := make(chan bool, len(peers))
	for _, peer := range peers.Peers() {
		go func() {
			var reply Reply
			if err := peer.Call(method, args, &reply); err != nil {
				zap.L().Error("raft: peer call failed", zap.Error(err))
			} else if reply.Success {
				ch <- true
			}
		}()
	}
	zap.L().Debug("raft: waiting for broadcast responses",
		zap.String("method", method),
		zap.Duration("timeout", timeout))
	totalTimeout := time.After(timeout)
	counter := 0
	for counter*2 < len(peers) {
		select {
		case <-ch:
			counter += 1
		case <-totalTimeout:
			zap.L().Debug("raft: broadcast timeout",
				zap.Duration("timeout", timeout),
				zap.Int("votes", counter+1))
			return false, nil
		}
	}
	return counter*2 >= len(peers), nil
}
