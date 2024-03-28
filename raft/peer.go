package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"net/rpc"
	"sync"

	"go.uber.org/zap"
)

type PeerAddr struct {
	Host string
	Port uint16
}

type Peer struct {
	mtx          *sync.Mutex
	client       *rpc.Client
	Id           uint32
	Addr         PeerAddr
	lastLogIndex uint64
}

func NewPeer(id uint32, host string, port uint16, lastLogIndex uint64) Peer {
	return Peer{
		mtx:          &sync.Mutex{},
		client:       nil,
		Id:           id,
		Addr:         PeerAddr{Host: host, Port: port},
		lastLogIndex: lastLogIndex,
	}
}

func (peer *Peer) SetLastLogIndex(index uint64) {
	peer.mtx.Lock()
	peer.lastLogIndex = index
	peer.mtx.Unlock()
}

func (peer *Peer) LastLogIndex() uint64 {
	peer.mtx.Lock()
	defer peer.mtx.Unlock()
	return peer.lastLogIndex
}

type Peers struct {
	Self      Peer
	mtx       *sync.RWMutex
	LeaderIdx int
	peers     []Peer
}

func NewPeers(self Peer) Peers {
	id := rand.Uint32()
	if id == 0 {
		id = 1
	}
	return Peers{
		Self:      self,
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
	} else if !reply.Success {
		return errors.New("join request rejected by peer")
	}
	for _, p := range reply.Peers {
		if p.Addr.Host == peer.Addr.Host && p.Addr.Port == peer.Addr.Port {
			peer.Id = p.Id
			peers.peers = []Peer{peer}
			break
		}
	}

	var error error = nil
	for _, p := range reply.Peers {
		if p.Id == peer.Id {
			continue
		}
		peer := NewPeer(p.Id, p.Addr.Host, p.Addr.Port, p.LastLogIndex)
		var reply JoinReply
		if err := peer.Call("Peers.RequestJoin", peers.Self, &reply); err != nil {
			error = fmt.Errorf("failed to call peer: %w", err)
			break
		} else if !reply.Success {
			error = errors.New("join request by peer")
			break
		}
		zap.L().Info("peer added",
			zap.Uint32("id", p.Id),
			zap.String("host", p.Addr.Host),
			zap.Uint16("port", p.Addr.Port))
		peers.peers = append(peers.peers, peer)
	}
	if error != nil {
		for _, peer := range peers.peers {
			var reply struct{}
			if err := peer.Call("Peers.NotifyLeave", peers.Self, &reply); err != nil {
				zap.L().Error("failed to notify leave", zap.Error(err))
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

func (peers *Peers) SetLeader(id uint32) {
	peers.mtx.Lock()
	defer peers.mtx.Unlock()
	if id == 0 {
		peers.LeaderIdx = -1
		return
	}
	for i, peer := range peers.peers {
		if peer.Id == id {
			peers.LeaderIdx = i
			return
		}
	}
	zap.L().Error("unkown peer", zap.Uint32("id", id))
	peers.LeaderIdx = -1
}

func (peers *Peers) Leader() *Peer {
	peers.mtx.RLock()
	defer peers.mtx.RUnlock()
	if peers.LeaderIdx != -1 {
		return &peers.peers[peers.LeaderIdx]
	}
	return nil
}

func (peers *Peers) PeerCount() int {
	peers.mtx.RLock()
	defer peers.mtx.RUnlock()
	return len(peers.peers)
}

func (peers *Peers) GetPeers() []Peer {
	result := []Peer{}
	peers.mtx.RLock()
	result = append(result, peers.peers...)
	peers.mtx.RUnlock()
	return result
}

func (peers *Peers) Broadcast(method string, req any) chan *rpc.Call {
	peerList := peers.GetPeers()
	done := make(chan *rpc.Call, len(peerList))
	for _, peer := range peerList {
		var reply Reply
		if _, err := peer.Go(method, req, &reply, done); err != nil {
			zap.L().Error("request failed", zap.String("method", method), zap.Error(err))
			done <- nil
		}
	}
	return done
}

type PeerInfo struct {
	Id           uint32
	Addr         PeerAddr
	LastLogIndex uint64
}

type JoinReply struct {
	Success  bool
	LeaderId uint32
	Peers    []PeerInfo
}

func (peers *Peers) RequestJoin(req PeerInfo, reply *JoinReply) error {
	peer := Peer{
		mtx:    &sync.Mutex{},
		client: nil,
		Id:     req.Id,
		Addr:   req.Addr,
	}

	peers.mtx.Lock()
	reply.Success = true
	for _, peer := range peers.peers {
		if peer.Id == req.Id {
			reply.Success = false
			break
		}
	}
	reply.Success = reply.Success && req.Id != peers.Self.Id
	if reply.Success {
		for _, info := range peers.peers {
			reply.Peers = append(reply.Peers, PeerInfo{
				Id:   info.Id,
				Addr: info.Addr,
			})
		}
		reply.Peers = append(reply.Peers, PeerInfo{
			Id:   peers.Self.Id,
			Addr: peers.Self.Addr,
		})
		peers.peers = append(peers.peers, peer)
		if peers.LeaderIdx != -1 {
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

func (peer *Peer) Go(method string, request any, reply any, done chan *rpc.Call) (*rpc.Call, error) {
	peer.mtx.Lock()
	if peer.client == nil {
		client, err := rpc.DialHTTP("tcp", fmt.Sprintf(peer.Addr.String()))
		if err != nil {
			peer.mtx.Unlock()
			return nil, fmt.Errorf("http dial: %w", err)
		}
		peer.client = client
	}
	peer.mtx.Unlock()
	zap.L().Debug("rpc request",
		zap.String("method", method),
		zap.Any("request", request),
		zap.String("addr", peer.Addr.String()))
	client := peer.client.Go(method, request, reply, done)
	return client, client.Error
}

func (peer *Peer) Call(method string, request any, reply any) error {
	call, err := peer.Go(method, request, reply, nil)
	if err != nil {
		return err
	}
	call = <-call.Done
	return call.Error
}

func (addr *PeerAddr) String() string {
	return fmt.Sprintf("%s:%d", addr.Host, addr.Port)
}
