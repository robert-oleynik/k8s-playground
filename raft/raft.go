package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"
)

// TODO: Update terms
// TODO: Redirect updates to leader

type Role int

const (
	Follower Role = iota
	Leader
	Canidate
)

type Raft[T interface{}] struct {
	electionTimeout time.Duration
	Config          Config

	Id         uint32
	heartbeat  chan struct{}
	Apply      chan T
	PeerEvents chan PeerEvent

	mtx      *sync.Mutex
	role     Role
	term     uint32
	votedFor uint32
	commited uint64
	Log      Log[T]

	peerMtx       *sync.RWMutex
	leaderId      uint32
	peers         map[uint32]*Peer
	peerReplicate map[uint32]chan struct{}
}

type PeerEvent struct {
	Id     uint32
	Peer   *Peer
	Delete bool
}

func New[T interface{}]() *Raft[T] {
	return NewWithConfig[T](DefaultConfig())
}

func NewWithConfig[T interface{}](conf Config) *Raft[T] {
	id := rand.Uint32()
	if id == 0 {
		id = 1
	}
	return &Raft[T]{
		electionTimeout: conf.RandElectionTimeout(),
		Config:          conf,

		Id:         id,
		heartbeat:  make(chan struct{}, conf.QueueSize),
		Apply:      make(chan T, conf.QueueSize),
		PeerEvents: make(chan PeerEvent, conf.QueueSize),

		mtx:      &sync.Mutex{},
		role:     Follower,
		term:     0,
		votedFor: 0,
		commited: 0,

		peerMtx:       &sync.RWMutex{},
		leaderId:      0,
		peers:         make(map[uint32]*Peer),
		peerReplicate: make(map[uint32]chan struct{}),
	}
}

func (raft *Raft[T]) Update(cmds []T) error {
	raft.mtx.Lock()
	role := raft.role
	raft.mtx.Unlock()

	req := UpdateRequest[T]{Data: cmds}
	var reply Reply
	if role == Leader {
		if err := raft.RequestUpdate(req, &reply); err != nil {
			return fmt.Errorf("request update: %w", err)
		}
	} else if role == Follower {
		raft.peerMtx.RLock()
		leader, ok := raft.peers[raft.leaderId]
		raft.peerMtx.RUnlock()
		if !ok {
			return errors.New("no peer elected")
		}
		var reply Reply
		client, err := leader.Connection()
		if err != nil {
			return fmt.Errorf("connection: %w", err)
		}
		if err := client.Call("Raft.RequestUpdate", req, &reply); err != nil {
			return fmt.Errorf("proxy update: %w", err)
		}
		if !reply.Success {
			return errors.New("update rejected")
		}
	} else if role == Canidate {
		return fmt.Errorf("no leader elected: update rejected")
	}
	return nil
}

func (raft *Raft[T]) ServeHTTP(listener net.Listener) error {
	server := rpc.NewServer()
	if err := server.RegisterName("Raft", raft); err != nil {
		return fmt.Errorf("raft: register raft rpc server: %w", err)
	}
	if err := http.Serve(listener, server); err != nil {
		return fmt.Errorf("raft: launch http server: %w", err)
	}
	return nil
}

func (raft *Raft[T]) RequestVotesWithContext(ctx context.Context) (bool, error) {
	raft.mtx.Lock()
	index, err := raft.Log.LastLogIndex()
	if err != nil {
		raft.mtx.Unlock()
		return false, fmt.Errorf("fetch log index: %w", err)
	}
	term, err := raft.Log.Term(index)
	if err != nil {
		raft.mtx.Unlock()
		return false, fmt.Errorf("fetch term: %w", err)
	}
	req := VoteRequest{
		Term:         raft.term,
		CanidateId:   raft.Id,
		LastLogTerm:  term,
		LastLogIndex: index,
	}
	raft.mtx.Unlock()

	raft.peerMtx.RLock()
	replies := make(chan *rpc.Call, len(raft.peers))
	for _, peer := range raft.peers {
		var reply Reply
		client, err := peer.Connection()
		if err != nil {
			zap.L().Error("raft: request vote", zap.Error(err))
		} else {
			client.Go("Raft.RequestVote", req, &reply, replies)
		}
	}
	peerCount := len(raft.peers)
	raft.peerMtx.RUnlock()

	votes := 0
	rejections := 0
	done := ctx.Done()
	for votes*2 < peerCount && rejections*2 <= peerCount {
		select {
		case call := <-replies:
			if call.Error != nil {
				zap.L().Error("raft: request for vote failed", zap.Error(call.Error))
				rejections++
			} else if reply, ok := call.Reply.(*Reply); ok && reply.Success {
				votes++
			} else {
				rejections++
			}
		case <-done:
			zap.L().Debug("raft: failed to gather votes before timeout",
				zap.Duration("timeout", raft.electionTimeout))
			return false, nil
		}
	}
	return votes*2 >= peerCount, nil
}

func (raft *Raft[T]) TeardownLeader() {
	raft.peerMtx.Lock()
	defer raft.peerMtx.Unlock()
	for id := range raft.peers {
		close(raft.peerReplicate[id])
		delete(raft.peerReplicate, id)
	}
}

func (raft *Raft[T]) ElectNewLeader() error {
	zap.L().Debug("start election", zap.Uint32("term", raft.leaderId))

	raft.peerMtx.Lock()
	raft.leaderId = 0
	raft.peerMtx.Unlock()

	raft.mtx.Lock()
	for raft.role == Canidate {
		raft.term += 1
		raft.votedFor = raft.Id
		zap.L().Debug("raft: elect", zap.Uint32("term", raft.term))
		raft.mtx.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), raft.electionTimeout)
		defer cancel()
		success, err := raft.RequestVotesWithContext(ctx)
		if err != nil {
			zap.L().Error("raft: failed requesting votes", zap.Error(err))
			raft.mtx.Lock()
			continue
		}
		zap.L().Debug("raft: election round complete",
			zap.Uint32("term", raft.term),
			zap.Bool("success", success),
			zap.Any("role", raft.role))

		raft.mtx.Lock()
		if success && raft.role == Canidate {
			raft.role = Leader
			for id, peer := range raft.peers {
				if ch, ok := raft.peerReplicate[id]; ok && ch != nil {
					close(ch)
				}
				replicateCh := make(chan struct{}, raft.Config.QueueSize)
				go raft.HandlePeer(peer, replicateCh)
				replicateCh <- struct{}{}
				raft.peerReplicate[id] = replicateCh
			}
		}
	}
	raft.mtx.Unlock()
	return nil
}

func (raft *Raft[T]) RunSession() {
	raft.mtx.Lock()
session:
	for raft.role == Follower {
		raft.mtx.Unlock()
		select {
		case <-raft.heartbeat:
		case <-time.After(raft.electionTimeout):
			zap.L().Info("raft: election timeout",
				zap.Duration("timeout", raft.electionTimeout),
				zap.Uint32("term", raft.term))
			raft.mtx.Lock()
			raft.role = Canidate
			break session
		}
		raft.mtx.Lock()
	}
	role := raft.role
	raft.mtx.Unlock()
	if role == Canidate {
		if err := raft.ElectNewLeader(); err != nil {
			zap.L().Error("raft: leader election failed", zap.Error(err))
		}
	}
}

func (raft *Raft[T]) Serve(listener net.Listener) error {
	raft.commited = raft.Log.CommitedIndex()
	go func() {
		for ev := range raft.PeerEvents {
			raft.peerMtx.Lock()
			if ev.Delete {
				if raft.peerReplicate[ev.Id] != nil {
					close(raft.peerReplicate[ev.Id])
				}
				delete(raft.peerReplicate, ev.Id)
				delete(raft.peers, ev.Id)
				zap.L().Debug("raft: peers updated",
					zap.Uint32("id", ev.Id),
					zap.Bool("delete", ev.Delete))
			} else {
				raft.peers[ev.Id] = ev.Peer
				zap.L().Debug("raft: peers updated",
					zap.Uint32("id", ev.Id),
					zap.String("host", ev.Peer.Host),
					zap.String("port", ev.Peer.Port),
					zap.Bool("delete", ev.Delete))
				if raft.role == Leader {
					if ch, ok := raft.peerReplicate[ev.Id]; ok && ch != nil {
						close(ch)
					}
					ch := make(chan struct{}, raft.Config.QueueSize)
					ch <- struct{}{}
					go raft.HandlePeer(ev.Peer, ch)
					raft.peerReplicate[ev.Id] = ch
				}
			}
			raft.peerMtx.Unlock()
		}
	}()
	go func() {
		// TODO: Only update on peer replies
		heartbeat := time.NewTicker(raft.Config.HeartbeatPeriod)
		for range heartbeat.C {
			raft.mtx.Lock()
			logIndex, err := raft.Log.LastLogIndex()
			raft.mtx.Unlock()
			if err != nil {
				zap.L().Error("raft: fetch log index", zap.Error(err))
			}

			raft.peerMtx.RLock()
			indices := []uint64{logIndex}
			for _, peer := range raft.peers {
				indices = append(indices, peer.logIndex)
			}
			slices.Sort(indices)
			commitIndex := indices[(len(indices)-1)/2]
			raft.peerMtx.RUnlock()

			raft.mtx.Lock()
			if commitIndex != raft.commited {
				entries, err := raft.Log.Commit(commitIndex)
				if err != nil {
					zap.L().Error("failed to commit",
						zap.Uint64("commit", commitIndex),
						zap.Error(err))
					raft.mtx.Unlock()
					continue
				}
				for _, entry := range entries {
					raft.Apply <- entry.Command
				}
				raft.commited = commitIndex
			}
			raft.mtx.Unlock()
		}
	}()
	go raft.RunSession()
	return raft.ServeHTTP(listener)
}

type AppendEntriesRequest[T interface{}] struct {
	Term         uint32
	LeaderId     uint32
	PrevLogTerm  uint32
	PrevLogIndex uint64
	Entries      []LogEntry[T]
	LeaderCommit uint64
}

type VoteRequest struct {
	Term         uint32
	CanidateId   uint32
	LastLogTerm  uint32
	LastLogIndex uint64
}

type UpdateRequest[T interface{}] struct {
	Data []T
}

type Reply struct {
	Term    uint32
	Success bool
}

func (raft *Raft[T]) AppendEntries(req AppendEntriesRequest[T], reply *Reply) error {
	defer func() {
		if len(req.Entries) == 0 {
			return
		}
		zap.L().Debug("raft: append entries",
			zap.Uint32("term", req.Term),
			zap.Uint32("leader", req.LeaderId),
			zap.Uint64("commit", req.LeaderCommit),
			zap.Uint64("logIndex", req.PrevLogIndex),
			zap.Uint32("logTerm", req.PrevLogTerm),
			zap.Int("count", len(req.Entries)),
			zap.Bool("success", reply.Success))
	}()

	raft.mtx.Lock()
	reply.Term = raft.term
	if req.Term < raft.term {
		reply.Success = false
		raft.mtx.Unlock()
		return nil
	}
	reply.Success = true

	raft.term = req.Term
	if raft.leaderId != req.LeaderId || raft.role != Follower {
		becomesFollower := raft.role != Follower
		raft.role = Follower
		raft.leaderId = req.LeaderId
		raft.votedFor = req.LeaderId
		zap.L().Info("raft: new leader",
			zap.Uint32("leaderId", req.LeaderId),
			zap.Uint32("term", req.Term))
		if becomesFollower {
			go raft.RunSession()
			go raft.TeardownLeader()
		}
	}

	if err := raft.Log.Append(req.PrevLogIndex, req.Entries); err != nil {
		zap.L().Error("raft: log append entries", zap.Error(err))
	} else if req.LeaderCommit > raft.Log.CommitedIndex() {
		logs, err := raft.Log.Commit(req.LeaderCommit)
		if err != nil {
			zap.L().Error("commit failed", zap.Error(err))
		} else {
			for _, entry := range logs {
				raft.Apply <- entry.Command
			}
		}
	}

	raft.mtx.Unlock()
	raft.heartbeat <- struct{}{}
	return nil
}

func (raft *Raft[T]) RequestVote(req VoteRequest, reply *Reply) error {
	defer func() {
		zap.L().Debug("raft: vote requested",
			zap.Uint32("term", req.Term),
			zap.Uint32("candidate", req.CanidateId),
			zap.Uint64("logIndex", req.LastLogIndex),
			zap.Uint32("logTerm", req.LastLogTerm),
			zap.Bool("granted", reply.Success))
	}()

	raft.mtx.Lock()
	defer raft.mtx.Unlock()

	reply.Success = false
	if req.Term < raft.term {
		return nil
	}

	if raft.votedFor == 0 || raft.votedFor == req.CanidateId || req.Term > raft.term {
		// TODO: Handle different terms
		logIndex, err := raft.Log.LastLogIndex()
		if err != nil {
			return fmt.Errorf("log: %w", err)
		}

		if logIndex <= req.LastLogIndex {
			raft.votedFor = req.CanidateId
			reply.Success = true
		}
	}
	return nil
}

func (raft *Raft[T]) RequestUpdate(req UpdateRequest[T], reply *Reply) error {
	defer func() {
		zap.L().Debug("raft: request update",
			zap.Int("entries", len(req.Data)),
			zap.Uint32("term", reply.Term),
			zap.Bool("success", reply.Success))
	}()

	raft.mtx.Lock()
	reply.Term = raft.term
	if raft.role != Leader {
		reply.Success = false
		return nil
	}

	entries := make([]LogEntry[T], len(req.Data))
	lastIndex, err := raft.Log.LastLogIndex()
	if err != nil {
		reply.Success = false
		raft.mtx.Unlock()
		return fmt.Errorf("fetch last index: %w", err)
	}
	for i, cmd := range req.Data {
		entries[i] = LogEntry[T]{
			Term:     raft.term,
			LogIndex: lastIndex + uint64(i) + 1,
			Command:  cmd,
		}
	}
	if err := raft.Log.Append(lastIndex, entries); err != nil {
		reply.Success = false
		raft.mtx.Unlock()
		return fmt.Errorf("log append: %w", err)
	}
	raft.mtx.Unlock()

	raft.peerMtx.RLock()
	for _, peer := range raft.peerReplicate {
		peer <- struct{}{}
	}
	raft.peerMtx.RUnlock()

	reply.Success = true
	return nil
}

type Info struct {
	Id       uint32
	LogIndex uint64
}

func (raft *Raft[T]) Info(req struct{}, reply *Info) error {
	reply.Id = raft.Id
	raft.mtx.Lock()
	defer raft.mtx.Unlock()
	index, err := raft.Log.LastLogIndex()
	if err != nil {
		return fmt.Errorf("log index: %w", err)
	}
	reply.LogIndex = index
	return nil
}
