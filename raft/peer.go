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
	Id   uint32
	host string
	port string

	Client   *rpc.Client
	mtx      *sync.RWMutex
	logIndex uint64
}

func NewPeer(host string, port string) (Peer, error) {
	peer := Peer{
		Id:   0,
		host: host,
		port: port,

		Client:   nil,
		mtx:      &sync.RWMutex{},
		logIndex: 0,
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
	peer.logIndex = reply.LogIndex
	peer.mtx.Unlock()
	return nil
}

func (peer *Peer) SetLastLogIndex(index uint64) {
	peer.mtx.Lock()
	peer.logIndex = index
	peer.mtx.Unlock()
}

func (peer *Peer) LastLogIndex() uint64 {
	peer.mtx.RLock()
	defer peer.mtx.RUnlock()
	return peer.logIndex
}
