package raft

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"time"

	"go.uber.org/zap"
)

type Server struct {
	peers  []Peer
	logger *zap.Logger

	id    uint
	state State

	electionTimeout time.Duration
	notifyHeartbeet chan struct{}
}

type Reply struct {
	Term    uint64
	Success bool
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderId     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []interface{}
	LeaderCommit uint64
}

func (self *Server) AppendEntries(req AppendEntriesRequest, reply *Reply) error {
	reply.Term = uint64(self.state.CurrentTerm)
	if len(req.Entries) > 0 {
		reply.Success = true
		return nil
	}
	self.logger.Info("request append entries",
		zap.Uint64("term", req.Term),
		zap.Uint64("leaderId", req.LeaderId),
		zap.Uint64("leaderCommit", req.LeaderCommit),
		zap.Uint64("prevLogIndex", req.PrevLogIndex),
		zap.Uint64("prevLogTerm", req.PrevLogTerm))
	// TODO
	return nil
}

type VoteRequest struct {
	Term         uint64
	CanidateId   uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

func (self *Server) RequestVote(req VoteRequest, reply *Reply) error {
	self.logger.Info("vote requested",
		zap.Uint64("term", req.Term),
		zap.Uint64("canidateId", req.CanidateId),
		zap.Uint64("lastLogIndex", req.LastLogIndex),
		zap.Uint64("lastLogIndex", req.LastLogTerm))
	reply.Term = uint64(self.state.CurrentTerm)
	if self.state.CurrentTerm > uint(req.Term) {
		reply.Success = false
	} else if self.state.VotedFor == 0 || self.state.Log.LastApplied() <= req.LastLogIndex {
		reply.Success = true
	}
	return nil
}

func (self *Server) StartElection() {
	self.state.StartElection()
	votes := make(chan struct{})
	for _, peer := range self.peers {
		go func() {
			voteRequest := VoteRequest{
				Term:         uint64(self.state.CurrentTerm),
				CanidateId:   uint64(self.id),
				LastLogIndex: self.state.Log.LastApplied(),
				LastLogTerm:  self.state.Log.LastLogTerm(),
			}
			reply, err := peer.RequestVote(voteRequest)
			if err != nil {
				self.logger.Error("failed to request vote", zap.Error(err))
			} else if reply.Success {
				votes <- struct{}{}
			}
		}()
	}
	voteCounter := 0
	requiredVotes := len(self.peers)/2 + len(self.peers)%2
	waitTimeout := time.After(self.electionTimeout)
	for voteCounter < requiredVotes {
		select {
		case <-votes:
			voteCounter++
		case <-waitTimeout:
			self.state.Role = Follower
			return
		}
	}
}

func (self *Server) StartSession() {
	defer self.logger.Warn("raft session exited")
	for {
		if self.state.Role == Follower {
			select {
			case <-time.After(self.electionTimeout):
				self.StartElection()
			case _, ok := <-self.notifyHeartbeet:
				if !ok {
					return
				}
			}
		} else if self.state.Role == Leader {
			select {
			case <-time.After(self.electionTimeout):
			case <-self.notifyHeartbeet:
			}
			for _, peer := range self.peers {
				heartbeet := AppendEntriesRequest{
					Term:         uint64(self.state.CurrentTerm),
					LeaderId:     uint64(self.id),
					PrevLogTerm:  0, // TODO
					PrevLogIndex: self.state.Log.LastApplied(),
					Entries:      []interface{}{},
				}
				peer.AppendEntries(heartbeet)
			}
		}
	}
}

func Start(logger *zap.Logger, config Config, peersAddr []string) error {
	logger.Info("startup", zap.Any("config", config))
	listener, err := net.Listen("tcp", config.Addr)
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}

	peers := make([]Peer, len(peersAddr))
	for i, addr := range peersAddr {
		peers[i] = NewPeer(addr)
	}
	server := &Server{
		peers:  peers,
		logger: logger,
		state:  NewState(),

		electionTimeout: config.RandElectionTimeout(),
		notifyHeartbeet: make(chan struct{}, 1),
	}

	rpc.Register(server)
	rpc.HandleHTTP()
	go server.StartSession()

	logger.Info("listening on", zap.String("addr", config.Addr))
	return http.Serve(listener, nil)
}
