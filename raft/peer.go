package raft

import (
	"fmt"
	"net/rpc"
	"sync"
)

type Discoverer interface {
	// Call to discover services
	Discover() error
	Peers() []Peer
}

type PeerAddr struct {
	Host string
	Port string
}

type Peer struct {
	Client       *rpc.Client
	Id           uint32
	mtx          *sync.RWMutex
	lastLogIndex uint64
}

func NewPeer(host string, port string) (Peer, error) {
	peer := Peer{
		Client:       nil,
		mtx:          &sync.RWMutex{},
		Id:           0,
		lastLogIndex: 0,
	}
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		return peer, err
	}
	peer.Client = client
	return peer, nil
}

func (peer *Peer) FetchInfo() error {
	var reply Info
	if err := peer.Client.Call("Raft.Info", struct{}{}, &reply); err != nil {
		return fmt.Errorf("raft info: %w", err)
	}
	peer.mtx.Lock()
	peer.Id = reply.Id
	peer.lastLogIndex = reply.LogIndex
	peer.mtx.Unlock()
	return nil
}

func (peer *Peer) SetLastLogIndex(index uint64) {
	peer.mtx.Lock()
	peer.lastLogIndex = index
	peer.mtx.Unlock()
}

func (peer *Peer) LastLogIndex() uint64 {
	peer.mtx.RLock()
	defer peer.mtx.RUnlock()
	return peer.lastLogIndex
}
