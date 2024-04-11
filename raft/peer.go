package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"net/rpc"
	"sync"
	"time"

	"go.uber.org/zap"
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
	Host string
	Port string

	logTerm  uint32
	logIndex uint64

	cm     *sync.Mutex
	client *rpc.Client
}

func NewPeer(host string, port string) *Peer {
	return &Peer{
		Id:   0,
		Host: host,
		Port: port,

		cm:     &sync.Mutex{},
		client: nil,
	}
}

func (peer *Peer) Connect() (*rpc.Client, error) {
	return rpc.DialHTTP("tcp", fmt.Sprintf("%s:%s", peer.Host, peer.Port))
}

func (peer *Peer) Connection() (*rpc.Client, error) {
	peer.cm.Lock()
	defer peer.cm.Unlock()
	if peer.client == nil {
		client, err := peer.Connect()
		if err != nil {
			return nil, fmt.Errorf("client: %w", err)
		}
		return client, nil
	} else {
		return peer.client, nil
	}

}

type Status int

const (
	Ok Status = iota
	Reconnect
	Fatal
)

func (raft *Raft[T]) waitForEvent(
	peer *Peer,
	heartbeat <-chan time.Time,
	replicate <-chan struct{},
	call <-chan *rpc.Call,
) ([]LogEntry[T], Status) {
outer:
	for {
		select {
		case <-heartbeat:
			return []LogEntry[T]{}, Ok
		case _, ok := <-replicate:
			if !ok {
				return nil, Fatal
			}
			entries, err := raft.Log.History(peer.logIndex)
			if err != nil {
				zap.L().Error("raft: wait for event", zap.Error(err))
				continue outer
			}
			return entries, Ok
		case reply := <-call:
			if reply.Error != nil {
				connLost := errors.Is(reply.Error, rpc.ErrShutdown)
				zap.L().Error("raft: peer wait for events",
					zap.Error(reply.Error),
					zap.Bool("recoverable", !connLost))
				if connLost {
					return nil, Reconnect
				}
				continue outer
			}
			args := reply.Args.(AppendEntriesRequest[T])
			if reply.Reply.(*Reply).Success {
				if len(args.Entries) > 0 {
					peer.logIndex = args.Entries[len(args.Entries)-1].LogIndex
					peer.logTerm = args.Entries[len(args.Entries)-1].Term
				}
				continue outer
			}
			old := peer.logIndex
			// TODO: Fetch previous index from log
			peer.logIndex = args.PrevLogIndex - 1
			term, err := raft.Log.Term(peer.logIndex)
			if err != nil {
				peer.logIndex = old
				zap.L().Error("raft: failed to get term", zap.Error(err))
				continue outer
			}
			peer.logTerm = term
		}
	}
}

func (raft *Raft[T]) HandlePeer(peer *Peer, replicate chan struct{}) {
	sessionID := rand.Uint32()
	defer zap.L().Info("raft: peer: session exited",
		zap.Uint32("peerId", peer.Id),
		zap.Uint32("sessionID", sessionID))
	heartbeat := time.NewTicker(raft.Config.HeartbeatPeriod)

	for {
		client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%s", peer.Host, peer.Port))
		if err != nil {
			zap.L().Error("raft: peer connect", zap.Error(err))
			return
		}
		defer client.Close()

		replyChannel := make(chan *rpc.Call, 2)
		defer close(replyChannel)

		var info Info
		if err := client.Call("Raft.Info", struct{}{}, &info); err != nil {
			zap.L().Error("raft: fetch info", zap.Error(err))
			continue
		}
		lastLogIndex, err := raft.Log.LastLogIndex()
		if err != nil {
			zap.L().Error("raft: last log index", zap.Error(err))
			continue
		}
		logIndex := min(info.LogIndex, lastLogIndex)
		logTerm, err := raft.Log.Term(logIndex)
		if err != nil {
			zap.L().Error("raft: failed to get term", zap.Error(err))
			continue
		}

		peer.logIndex = logIndex
		peer.logTerm = logTerm

		zap.L().Debug("raft: connection established",
			zap.Uint32("id", peer.Id),
			zap.String("host", peer.Host),
			zap.String("port", peer.Port),
			zap.Uint64("index", logIndex),
			zap.Uint32("term", logTerm),
			zap.Uint32("sessionID", sessionID))

		for {
			entries, ok := raft.waitForEvent(peer, heartbeat.C, replicate, replyChannel)
			if ok == Fatal {
				return
			} else if ok == Reconnect {
				break
			}

			raft.mtx.Lock()
			if raft.role != Leader {
				raft.mtx.Unlock()
				return
			}

			if len(entries) > 0 {
				zap.L().Debug("raft: replicate peer",
					zap.Uint32("leaderTerm", raft.term),
					zap.Uint32("leaderId", raft.Id),
					zap.Uint32("peerId", peer.Id),
					zap.Uint32("logTerm", logTerm),
					zap.Uint64("logIndex", logIndex),
					zap.Int("count", len(entries)))
			}
			arg := AppendEntriesRequest[T]{
				Term:         raft.term,
				LeaderId:     raft.Id,
				PrevLogTerm:  logTerm,
				PrevLogIndex: logIndex,
				Entries:      entries,
				LeaderCommit: raft.commited,
			}
			raft.mtx.Unlock()

			var reply Reply
			client.Go("Raft.AppendEntries", arg, &reply, replyChannel)
		}
	}
}
