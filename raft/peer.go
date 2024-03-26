package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"sync"

	"go.uber.org/zap"
)

type PeerAddr struct {
	Host string
	Port uint16
}

type Peer struct {
	mtx    *sync.Mutex
	client *rpc.Client
	Id     uint32
	Addr   PeerAddr
}

type Peers struct {
	Self      Peer
	mtx       *sync.RWMutex
	LeaderIdx int
	peers     []Peer
}

func NewPeers(host string, port uint16) Peers {
	id := rand.Uint32()
	if id == 0 {
		id = 1
	}
	return Peers{
		Self: Peer{
			mtx:    &sync.Mutex{},
			client: nil,
			Id:     id,
			Addr:   PeerAddr{Host: host, Port: port},
		},
		mtx:       &sync.RWMutex{},
		LeaderIdx: -1,
		peers:     []Peer{},
	}
}

func (peers *Peers) Join(peer Peer) error {
	peers.mtx.Lock()
	defer peers.mtx.Unlock()

	var reply JoinReply
	if err := peer.Call("Peers.RequestJoin", peers.Self, &reply); err != nil {
		return fmt.Errorf("request join: %w", err)
	}
	if !reply.Success {
		return errors.New("join request rejected by peer")
	}
	for _, p := range reply.Peers {
		if p.Addr.Host == peer.Addr.Host && p.Addr.Port == peer.Addr.Port {
			peer.Id = p.Id
			peers.peers = []Peer{p}
			break
		}
	}

	var error error = nil
	for _, p := range reply.Peers {
		if p.Id == peer.Id {
			continue
		}
		if err := peer.Call("Peers.RequestJoin", peers.Self, &reply); err != nil {
			error = fmt.Errorf("failed to call peer: %w", err)
			break
		} else if !reply.Success {
			error = errors.New("join request by peer")
			break
		}
	}
	if error != nil {
		for _, peer := range peers.peers {
			var reply struct{}
			if err := peer.Call("Peers.NotifyLeave", peers.Self, &reply); err != nil {
				zap.L().Error("failed to notify leave",
					zap.Error(err))
			}
		}
	} else {
		peers.LeaderIdx = -1
		for i, peer := range peers.peers {
			if reply.LeaderId == peer.Id {
				peers.LeaderIdx = i
				break
			}
		}
	}
	return error
}

func (peers *Peers) GetPeers() []Peer {
	result := []Peer{}
	peers.mtx.RLock()
	copy(result, peers.peers)
	peers.mtx.RUnlock()
	return result
}

type JoinReply struct {
	Success  bool
	LeaderId uint32
	Peers    []Peer
}

func (peers *Peers) RequestJoin(req Peer, reply *JoinReply) error {
	req.mtx = &sync.Mutex{}
	req.client = nil

	peers.mtx.Lock()
	reply.Success = true
	for _, peer := range peers.peers {
		if peer.Id == req.Id {
			reply.Success = false
			break
		}
	}
	reply.Success = reply.Success && req.Id == peers.Self.Id
	if reply.Success {
		copy(reply.Peers, peers.peers)
		reply.Peers = append(reply.Peers, peers.Self)
		peers.peers = append(peers.peers, req)
		if peers.LeaderIdx == -1 {
			reply.LeaderId = peers.peers[peers.LeaderIdx].Id
		} else {
			reply.LeaderId = 0
		}
	}
	peers.mtx.Unlock()

	zap.L().Info("join requested",
		zap.Uint32("id", req.Id),
		zap.String("host", req.Addr.Host),
		zap.Uint16("port", req.Addr.Port),
		zap.Bool("success", reply.Success))
	return nil
}

func (peers *Peers) NotifyLeave(req Peer, reply *struct{}) error {
	peers.mtx.Lock()
	deletions := []int{}
	for i, peer := range peers.peers {
		if peer.Id == req.Id {
			deletions = append(deletions, i)
		}
	}
	for off, i := range deletions {
		i -= off
		if i == peers.LeaderIdx {
			peers.LeaderIdx = -1
		} else if i < peers.LeaderIdx {
			peers.LeaderIdx--
		}
		if i+1 < len(peers.peers) {
			peers.peers = append(peers.peers[:i], peers.peers[i+1:]...)
		} else {
			peers.peers = peers.peers[:i]
		}
	}
	peers.mtx.Unlock()

	zap.L().Info("leave notified",
		zap.Uint32("id", req.Id),
		zap.String("host", req.Addr.Host),
		zap.Uint16("port", req.Addr.Port))
	return nil
}

func (peer *Peer) Call(method string, request any, reply any) error {
	peer.mtx.Lock()
	if peer.client == nil {
		client, err := rpc.DialHTTP("tcp", fmt.Sprintf(peer.Addr.String()))
		if err != nil {
			peer.mtx.Unlock()
			return fmt.Errorf("http dial: %w", err)
		}
		peer.client = client
	}
	peer.mtx.Unlock()
	if err := peer.client.Call(method, request, reply); err != nil {
		if nerr, ok := err.(net.Error); ok && !nerr.Timeout() {
			peer.mtx.Lock()
			peer.client = nil
			peer.mtx.Unlock()
		}
	}
	return nil
}

func (addr *PeerAddr) String() string {
	return fmt.Sprintf("%s:%d", addr.Host, addr.Port)
}
