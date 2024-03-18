package raft

type Role int

const (
	Follower Role = iota
	Canidate
	Leader
)

type State struct {
	Role Role

	CurrentTerm uint
	VotedFor    uint
	Log         Log
}

func NewState() State {
	return State{
		Role: Follower,
	}
}

func (self *State) StartElection() {
	self.Role = Canidate
	self.CurrentTerm++
}
