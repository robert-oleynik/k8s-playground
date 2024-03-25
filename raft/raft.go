package raft

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Role int

const (
	Follower Role = iota
	Leader
	Canidate
)

type Raft struct {
	updateQueue     chan interface{}
	electionTimeout time.Duration
	Config          Config

	id        uint32
	heartbeat chan struct{}

	mtx      *sync.Mutex
	role     Role
	term     uint32
	votedFor uint32
	Log      LogManager
	Peer     PeerManager
}

func New() *Raft {
	return NewWithConfig(DefaultConfig())
}

func NewWithConfig(conf Config) *Raft {
	id := rand.Uint32()
	if id == 0 {
		id = 1
	}
	return &Raft{
		electionTimeout: conf.RandElectionTimeout(),
		updateQueue:     make(chan interface{}, conf.QueueSize),
		Config:          conf,

		id:        id,
		heartbeat: make(chan struct{}),

		mtx:      &sync.Mutex{},
		role:     Follower,
		term:     0,
		votedFor: 0,
	}
}

func (raft *Raft) Update(cmd interface{}) {
	raft.updateQueue <- cmd
}

func (raft *Raft) ServeHTTP(listener net.Listener) error {
	server := rpc.NewServer()
	if err := server.Register(raft); err != nil {
		return fmt.Errorf("raft: register rpc server: %w", err)
	}
	if err := http.Serve(listener, server); err != nil {
		return fmt.Errorf("raft: launch http server: %w", err)
	}
	return nil
}

func (raft *Raft) RequestVotes() (bool, error) {
	raft.mtx.Lock()
	term, err := raft.Log.LastTerm()
	if err != nil {
		raft.mtx.Unlock()
		return false, fmt.Errorf("fetch term: %w", err)
	}
	index, err := raft.Log.LastLogIndex()
	if err != nil {
		raft.mtx.Unlock()
		return false, fmt.Errorf("fetch log index: %w", err)
	}
	req := VoteRequest{
		Term:         raft.term,
		CanidateId:   raft.id,
		LastLogTerm:  term,
		LastLogIndex: index,
	}
	raft.mtx.Unlock()
	success, err := raft.Peer.Broadcast("Raft.RequestVote", req, raft.electionTimeout)
	if err != nil {
		zap.L().Error("raft: broadcast failed", zap.Error(err))
	}
	return success, nil
}

func (raft *Raft) SendHeartbeet() error {
	peers := raft.Peer.Peers()
	raft.mtx.Lock()
	term := raft.term
	logIndex, err := raft.Log.LastLogIndex()
	if err != nil {
		return fmt.Errorf("log index: %w", err)
	}
	logTerm, err := raft.Log.LastTerm()
	if err != nil {
		return fmt.Errorf("log term: %w", err)
	}
	commit := raft.Log.CommitedIndex()
	raft.mtx.Unlock()
	for _, peer := range peers {
		go func() {
			var reply Reply
			args := AppendEntriesRequest{
				Term:         term,
				LeaderId:     raft.id,
				LeaderCommit: commit,
				PrevLogTerm:  logTerm,
				PrevLogIndex: logIndex,
				Entries:      []LogEntry{},
			}
			if err := peer.Call("Raft.AppendEntries", args, &reply); err != nil {
				zap.L().Error("raft: failed to send heartbeat", zap.Error(err))
			}
			// TODO: Update Peer Information
		}()
	}
	// TODO: Update last index of each
	return nil
}

func (raft *Raft) ElectNewLeader() error {
	raft.mtx.Lock()
	raft.role = Canidate
	for raft.role == Canidate {
		raft.term += 1
		raft.votedFor = raft.id
		raft.mtx.Unlock()

		success, err := raft.RequestVotes()
		if err != nil {
			zap.L().Error("raft: failed requesting votes", zap.Error(err))
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

func (raft *Raft) RunSession() error {
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
			raft.SendHeartbeet()
			time.Sleep(raft.Config.HeartbeatPeriod)
		} else {
			panic("should not be reached")
		}
	}
}

func (raft *Raft) Serve(listener net.Listener) error {
	go func() {
		if err := raft.RunSession(); err != nil {
			zap.L().Error("raft: session failed", zap.Error(err))
		}
	}()
	return raft.ServeHTTP(listener)
}

type AppendEntriesRequest struct {
	Term         uint32
	LeaderId     uint32
	PrevLogTerm  uint32
	PrevLogIndex uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type VoteRequest struct {
	Term         uint32
	CanidateId   uint32
	LastLogTerm  uint32
	LastLogIndex uint64
}

type Reply struct {
	Term    uint64
	Success bool
}

func (raft *Raft) AppendEntries(req AppendEntriesRequest, reply *Reply) error {
	raft.mtx.Lock()
	if req.Term < raft.term {
		reply.Success = false
	} else {
		if raft.role == Canidate || raft.role == Leader {
			raft.role = Follower
		}
		raft.Log.Append(req.PrevLogIndex, req.Entries)
		if req.LeaderCommit > raft.Log.CommitedIndex() {
			// TODO: Update State
			raft.Log.Commit(req.LeaderCommit)
		}

	}
	raft.mtx.Unlock()
	raft.heartbeat <- struct{}{}
	zap.L().Info("raft: append entries",
		zap.Uint32("term", req.Term),
		zap.Uint32("leaderId", req.LeaderId),
		zap.Uint64("leaderCommit", req.LeaderCommit),
		zap.Uint64("prevLogIndex", req.PrevLogIndex),
		zap.Uint32("prevLogTerm", req.PrevLogTerm),
		zap.Int("count", len(req.Entries)))
	return nil
}

func (raft *Raft) RequestVote(req VoteRequest, reply *Reply) error {
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
	zap.L().Info("raft: vote requested",
		zap.Uint32("term", req.Term),
		zap.Uint32("canidateId", req.CanidateId),
		zap.Uint64("lastLogIndex", req.LastLogIndex),
		zap.Uint32("lastLogIndex", req.LastLogTerm),
		zap.Bool("granted", reply.Success))
	return nil
}
