package raft

import (
	"context"
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
// TODO: Do updates
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

	mtx      *sync.Mutex
	role     Role
	term     uint32
	votedFor uint32
	Log      LogManager[T]
	Peers    Peers
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
		heartbeat: make(chan struct{}),

		mtx:      &sync.Mutex{},
		role:     Follower,
		term:     0,
		votedFor: 0,
	}
}

/**
func (raft *Raft[T]) Update(cmds []T) error {
	raft.mtx.Lock()
	role := raft.role
	raft.mtx.Unlock()

	req := UpdateRequest[T]{Data: cmds}
	var reply UpdateReply
	if role == Leader {
		if err := raft.RequestUpdate(req, &reply); err != nil {
			return fmt.Errorf("request update: %w", err)
		}
	} else if role == Follower {
		// TODO: Request leader
	} else if role == Canidate {
		return fmt.Errorf("no leader elected: update rejected")
	}
	return nil
}
*/

func (raft *Raft[T]) ServeHTTP(listener net.Listener) error {
	server := rpc.NewServer()
	if err := server.Register(raft); err != nil {
		return fmt.Errorf("raft: register raft rpc server: %w", err)
	}
	if err := server.Register(&raft.Peers); err != nil {
		return fmt.Errorf("raft: register peers rpc server: %w", err)
	}
	if err := http.Serve(listener, server); err != nil {
		return fmt.Errorf("raft: launch http server: %w", err)
	}
	return nil
}

func (raft *Raft[T]) RequestVotesWithContext(ctx context.Context) (bool, error) {
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

	peerCount := raft.Peers.PeerCount()
	zap.L().Debug("call", zap.Any("peerCOunt", peerCount))
	replies := raft.Peers.Broadcast("Raft.RequestVote", req)
	votes := 0
	rejections := 0
	for votes*2 < peerCount && rejections*2 <= peerCount {
		select {
		case call := <-replies:
			zap.L().Debug("call", zap.Any("call", call))
			if call == nil {
				rejections++
			} else if reply, ok := call.Reply.(*Reply); ok && reply.Success {
				votes++
			} else {
				rejections++
			}
		case <-ctx.Done():
			zap.L().Info("failed to gather votes before timeout",
				zap.Duration("timeout", raft.electionTimeout))
			return false, nil
		}
	}
	return votes*2 >= peerCount, nil
}

func (raft *Raft[T]) SendHeartbeet() error {
	peers := raft.Peers.GetPeers()
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
			args := AppendEntriesRequest[T]{
				Term:         term,
				LeaderId:     raft.id,
				LeaderCommit: commit,
				PrevLogTerm:  logTerm,
				PrevLogIndex: logIndex,
				Entries:      []LogEntry[T]{},
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

func (raft *Raft[T]) ElectNewLeader() error {
	raft.mtx.Lock()
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

type UpdateReply struct {
	Completed int
	Success   bool
}

func (raft *Raft[T]) AppendEntries(req AppendEntriesRequest[T], reply *Reply) error {
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
	zap.L().Info("raft: vote requested",
		zap.Uint32("term", req.Term),
		zap.Uint32("canidateId", req.CanidateId),
		zap.Uint64("lastLogIndex", req.LastLogIndex),
		zap.Uint32("lastLogIndex", req.LastLogTerm),
		zap.Bool("granted", reply.Success))
	return nil
}

/**
func (raft *Raft[T]) RequestUpdate(req UpdateRequest[T], reply *UpdateReply) error {
	raft.mtx.Lock()
	entries := make([]LogEntry[T], len(req.Data))
	lastIndex, err := raft.Log.LastLogIndex()
	if err != nil {
		raft.mtx.Unlock()
		return fmt.Errorf("fetch last index: %w", err)
	}
	for i, cmd := range req.Data {
		entries = append(entries, LogEntry[T]{
			Term:     raft.term,
			LogIndex: lastIndex + i + 1,
			Command:  cmd,
		})
	}
	if err := raft.Log.Append(lastIndex+1, entries); err != nil {
		return fmt.Errorf("log append: %w", err)
	}
	raft.mtx.Unlock()
	// TODO: Replicate to majority before confirming
	return nil
}
*/
