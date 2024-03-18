package raft

import (
	"fmt"
	"net/rpc"
)

type Peer struct {
	addr string
}

func NewPeer(addr string) Peer {
	return Peer{addr}
}

func (self *Peer) Call(serviceMethod string, args any, reply any) error {
	client, err := rpc.DialHTTP("tcp", self.addr)
	if err != nil {
		return fmt.Errorf("dial http: %w", err)
	}
	if err := client.Call(serviceMethod, args, reply); err != nil {
		return fmt.Errorf("service call: %w", err)
	}
	return nil
}

func (self *Peer) RequestVote(args VoteRequest) (Reply, error) {
	var reply Reply
	err := self.Call("Server.RequestVote", args, &reply)
	return reply, err
}

func (self *Peer) AppendEntries(args AppendEntriesRequest) (Reply, error) {
	var reply Reply
	err := self.Call("Server.AppendEntries", args, &reply)
	return reply, err
}
