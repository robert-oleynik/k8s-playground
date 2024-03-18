package raft

type Log struct {
	entries []Entry
}

type Entry struct {
	term  uint
	index uint
	// TODO: data
}

func NewLog() Log {
	return Log{
		entries: []Entry{},
	}
}

func (self *Log) CommitIndex() uint64 {
	return 0
}

func (self *Log) LastApplied() uint64 {
	return 0
}

func (self *Log) LastLogTerm() uint64 {
	return 0
}
