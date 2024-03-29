package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
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

	id        uint32
	heartbeat chan struct{}
	replicate chan struct{}
	Apply     chan T

	mtx        *sync.Mutex
	role       Role
	term       uint32
	votedFor   uint32
	Log        Log[T]
	Discoverer Discoverer

	peerMtx     *sync.RWMutex
	peerC       *sync.Cond
	reqDiscover bool
	leaderId    uint32
	peers       map[uint32]Peer
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

		id:        id,
		heartbeat: make(chan struct{}, conf.QueueSize),
		replicate: make(chan struct{}, conf.QueueSize),
		Apply:     make(chan T, conf.QueueSize),

		mtx:      &sync.Mutex{},
		role:     Follower,
		term:     0,
		votedFor: 0,

		peerMtx:     &sync.RWMutex{},
		peerC:       sync.NewCond(&sync.Mutex{}),
		reqDiscover: false,
		leaderId:    0,
		peers:       make(map[uint32]Peer),
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
		leader, ok := raft.peers[raft.id]
		raft.peerMtx.RUnlock()
		if !ok {
			return errors.New("no peer elected")
		}
		var reply Reply
		if err := leader.Client.Call("Raft.RequestUpdate", req, &reply); err != nil {
			if errors.Is(err, rpc.ErrShutdown) {
				raft.discoverPeers()
			}
			return fmt.Errorf("proxy update: %w", err)
		} else if !reply.Success {
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
		CanidateId:   raft.id,
		LastLogTerm:  term,
		LastLogIndex: index,
	}
	raft.mtx.Unlock()

	raft.peerMtx.RLock()
	replies := make(chan *rpc.Call, len(raft.peers))
	for _, peer := range raft.peers {
		var reply Reply
		peer.Client.Go("Raft.RequestVote", req, &reply, replies)
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
				if errors.Is(call.Error, rpc.ErrShutdown) {
					raft.discoverPeers()
				}
				zap.L().Error("request for vote failed", zap.Error(call.Error))
				rejections++
			} else if reply, ok := call.Reply.(*Reply); ok && reply.Success {
				votes++
			} else {
				rejections++
			}
		case <-done:
			zap.L().Info("failed to gather votes before timeout",
				zap.Duration("timeout", raft.electionTimeout))
			return false, nil
		}
	}
	return votes*2 >= peerCount, nil
}

func (raft *Raft[T]) SendHeartbeat() error {
	raft.mtx.Lock()
	term := raft.term
	logIndex, err := raft.Log.LastLogIndex()
	if err != nil {
		return fmt.Errorf("log index: %w", err)
	}
	commit := raft.Log.CommitedIndex()
	raft.mtx.Unlock()
	raft.peerMtx.RLock()
	for _, peer := range raft.peers {
		peerLogIndex := peer.LastLogIndex()
		go func() {
			index := min(peerLogIndex, logIndex)
			for {
				var reply Reply
				raft.mtx.Lock()
				logTerm, err := raft.Log.Term(index)
				if err != nil {
					zap.L().Error("heartbeat: failed to fetch term", zap.Error(err))
					break
				}
				raft.mtx.Unlock()
				args := AppendEntriesRequest[T]{
					Term:         term,
					LeaderId:     raft.id,
					LeaderCommit: commit,
					PrevLogTerm:  logTerm,
					PrevLogIndex: index,
					Entries:      []LogEntry[T]{},
				}
				if err := peer.Client.Call("Raft.AppendEntries", args, &reply); err != nil {
					if errors.Is(err, rpc.ErrShutdown) {
						raft.discoverPeers()
					}
					zap.L().Error("raft: failed to send heartbeat", zap.Error(err))
				}
				peer.SetLastLogIndex(index)
				if reply.Success {
					break
				}
				index--
			}
		}()
	}
	raft.peerMtx.RUnlock()
	return nil
}

func (raft *Raft[T]) ReplicateLog() error {
	raft.mtx.Lock()
	term := raft.term
	commit := raft.Log.CommitedIndex()
	raft.mtx.Unlock()

	raft.peerMtx.RLock()
	for _, peer := range raft.peers {
		peerLogIndex := peer.LastLogIndex()
		go func() {
			for {
				raft.mtx.Lock()
				logTerm, err := raft.Log.Term(peerLogIndex)
				if err != nil {
					zap.L().Error("heartbeat: failed to fetch term", zap.Error(err))
					break
				}
				entries, err := raft.Log.History(peerLogIndex)
				if err != nil {
					zap.L().Error("entries", zap.Error(err))
					break
				}
				raft.mtx.Unlock()
				if len(entries) == 0 {
					break
				}
				var reply Reply
				args := AppendEntriesRequest[T]{
					Term:         term,
					LeaderId:     raft.id,
					LeaderCommit: commit,
					PrevLogTerm:  logTerm,
					PrevLogIndex: peerLogIndex,
					Entries:      entries,
				}
				if err := peer.Client.Call("Raft.AppendEntries", args, &reply); err != nil {
					if errors.Is(err, rpc.ErrShutdown) {
						raft.discoverPeers()
					}
					zap.L().Error("raft: failed to send heartbeat", zap.Error(err))
				}
				if reply.Success {
					raft.mtx.Lock()
					index := entries[len(entries)-1].LogIndex
					peer.SetLastLogIndex(index)
					zap.L().Info("peer successfully replicated",
						zap.Uint32("id", peer.Id),
						zap.Uint64("lastIndex", index))
					raft.mtx.Unlock()
					break
				}
			}
		}()
	}
	raft.peerMtx.RUnlock()
	return nil
}

func (raft *Raft[T]) ElectNewLeader() error {
	raft.mtx.Lock()
	raft.peerMtx.Lock()
	raft.leaderId = 0
	raft.peerMtx.Unlock()
	raft.role = Canidate
	for raft.role == Canidate {
		raft.term += 1
		raft.votedFor = raft.id
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
			zap.Bool("success", success),
			zap.Any("role", raft.role))

		raft.mtx.Lock()
		if success && raft.role == Canidate {
			raft.role = Leader
		}
	}
	raft.mtx.Unlock()
	return nil
}

func (raft *Raft[T]) RunSession() error {
	go func() {
		for range raft.replicate {
			if err := raft.ReplicateLog(); err != nil {
				zap.L().Error("log replication failed", zap.Error(err))
			}
		}
	}()
	go func() {
		for {
			raft.peerC.L.Lock()
			for raft.reqDiscover {
				raft.peerC.Wait()
			}
			if err := raft.Discoverer.Discover(); err != nil {
				zap.L().Error("failed to discover peers", zap.Error(err))
			}
			raft.peers = make(map[uint32]Peer)
			for _, peer := range raft.Discoverer.Peers() {
				raft.peers[peer.Id] = peer
			}
			raft.reqDiscover = false
			raft.peerC.L.Unlock()
		}
	}()
	go func() {
		for {
			time.Sleep(15 * time.Second)
			raft.discoverPeers()
		}
	}()
	for {
		raft.mtx.Lock()
		r := raft.role
		raft.mtx.Unlock()
		if r == Follower {
			select {
			case <-time.After(raft.electionTimeout):
				zap.L().Debug("raft: election timeout",
					zap.Duration("timeout", raft.electionTimeout),
					zap.Uint32("term", raft.term))
				if err := raft.ElectNewLeader(); err != nil {
					zap.L().Error("raft: failed to elect new leader", zap.Error(err))
				}
				zap.L().Debug("raft: election complete",
					zap.Any("role", raft.role),
					zap.Uint32("term", raft.term))
			case <-raft.heartbeat:
			}
		} else if r == Leader {
			zap.L().Debug("raft: sending heartbeat",
				zap.Duration("heartbeatPeriod", raft.Config.HeartbeatPeriod))
			if err := raft.SendHeartbeat(); err != nil {
				zap.L().Error("raft: heartbeat", zap.Error(err))
			}
			time.Sleep(raft.Config.HeartbeatPeriod)
		} else {
			panic("should not be reached")
		}
	}
}

func (raft *Raft[T]) discoverPeers() {
	raft.peerC.L.Lock()
	raft.reqDiscover = true
	raft.peerC.Broadcast()
	raft.peerC.L.Unlock()
}

func (raft *Raft[T]) Serve(listener net.Listener) error {
	go func() {
		if err := raft.RunSession(); err != nil {
			zap.L().Error("raft: session failed", zap.Error(err))
		}
	}()
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
	Term    uint64
	Success bool
}

func (raft *Raft[T]) AppendEntries(req AppendEntriesRequest[T], reply *Reply) error {
	raft.mtx.Lock()
	if req.Term < raft.term {
		reply.Success = false
		raft.mtx.Unlock()
	} else {
		raft.peerMtx.Lock()
		raft.leaderId = req.LeaderId
		raft.peerMtx.Unlock()
		if raft.role == Canidate || raft.role == Leader {
			raft.role = Follower
		}
		raft.Log.Append(req.PrevLogIndex, req.Entries)
		if req.LeaderCommit > raft.Log.CommitedIndex() {
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

	}
	zap.L().Debug("raft: append entries",
		zap.Uint32("term", req.Term),
		zap.Uint32("leaderId", req.LeaderId),
		zap.Uint64("leaderCommit", req.LeaderCommit),
		zap.Uint64("prevLogIndex", req.PrevLogIndex),
		zap.Uint32("prevLogTerm", req.PrevLogTerm),
		zap.Int("count", len(req.Entries)))
	return nil
}

func (raft *Raft[T]) RequestVote(req VoteRequest, reply *Reply) error {
	raft.mtx.Lock()
	if req.Term < raft.term {
		reply.Success = false
	} else if raft.votedFor == 0 || raft.votedFor == req.CanidateId {
		logIndex, err := raft.Log.LastLogIndex()
		if err != nil {
			raft.mtx.Unlock()
			return fmt.Errorf("log: %w", err)
		} else if logIndex <= req.LastLogIndex {
			raft.votedFor = req.CanidateId
			reply.Success = true
		}
	}
	raft.mtx.Unlock()
	zap.L().Debug("raft: vote requested",
		zap.Uint32("term", req.Term),
		zap.Uint32("canidateId", req.CanidateId),
		zap.Uint64("lastLogIndex", req.LastLogIndex),
		zap.Uint32("lastLogIndex", req.LastLogTerm),
		zap.Bool("granted", reply.Success))
	return nil
}

func (raft *Raft[T]) RequestUpdate(req UpdateRequest[T], reply *Reply) error {
	raft.mtx.Lock()
	reply.Term = uint64(raft.term)
	if raft.role != Leader {
		reply.Success = false
		raft.mtx.Unlock()
		return nil
	}
	entries := make([]LogEntry[T], len(req.Data))
	lastIndex, err := raft.Log.LastLogIndex()
	if err != nil {
		raft.mtx.Unlock()
		return fmt.Errorf("fetch last index: %w", err)
	}
	for i, cmd := range req.Data {
		entries = append(entries, LogEntry[T]{
			Term:     raft.term,
			LogIndex: lastIndex + uint64(i) + 1,
			Command:  cmd,
		})
	}
	if err := raft.Log.Append(lastIndex+1, entries); err != nil {
		return fmt.Errorf("log append: %w", err)
	}
	raft.mtx.Unlock()

	raft.replicate <- struct{}{}
	reply.Success = true
	return nil
}

type Info struct {
	Id       uint32
	LogIndex uint64
}

func (raft *Raft[T]) Info(req struct{}, reply *Info) error {
	reply.Id = raft.id
	raft.mtx.Lock()
	defer raft.mtx.Unlock()
	index, err := raft.Log.LastLogIndex()
	if err != nil {
		return fmt.Errorf("log index: %w", err)
	}
	reply.LogIndex = index
	return nil
}
