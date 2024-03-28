package raft

import (
	"errors"
	"sync"
)

type LogManager[T interface{}] interface {
	// Append new entries starting at given position. May overwrite existing ones.
	Append(i uint64, entries []LogEntry[T]) error
	// Set last logged entry to provided logged index.
	Commit(logIndex uint64) ([]LogEntry[T], error)
	// Returns a list of indices after
	History(lastLogIndex uint64) ([]LogEntry[T], error)
	// Commited index
	CommitedIndex() uint64
	// Returns true if log entry with id and term exists.
	Validate(index uint64, term uint32) bool
	// Returns the term of the log entry with given index.
	Term(index uint64) (uint32, error)
	// Returns the index of the last commited entry.
	LastLogIndex() (uint64, error)
}

type LogEntry[T interface{}] struct {
	Term     uint32
	LogIndex uint64
	Command  T
}

type InMemoryLog[T interface{}] struct {
	mtx      *sync.RWMutex
	Entries  []LogEntry[T]
	Commited uint
}

func NewDevelopmentLog[T interface{}]() *InMemoryLog[T] {
	return &InMemoryLog[T]{
		mtx:      &sync.RWMutex{},
		Entries:  []LogEntry[T]{},
		Commited: 0,
	}
}

func (log *InMemoryLog[T]) Append(i uint64, entries []LogEntry[T]) error {
	log.mtx.Lock()
	copy(log.Entries[i:], entries[:len(entries[i:])])
	log.Entries = append(log.Entries, entries[len(entries[i:]):]...)
	log.mtx.Unlock()
	return nil
}

func (log *InMemoryLog[T]) Commit(logIndex uint64) ([]LogEntry[T], error) {
	var commited []LogEntry[T]
	log.mtx.Lock()
	defer log.mtx.Unlock()
	if int(logIndex) > len(log.Entries) {
		return commited, errors.New("cannot commit non-existing entry")
	}
	commited = log.Entries[log.Commited:int(logIndex)]
	log.Commited = uint(logIndex)
	return commited, nil
}

func (log *InMemoryLog[T]) History(lastLogIndex uint64) ([]LogEntry[T], error) {
	results := []LogEntry[T]{}
	log.mtx.RLock()
	results = append(results, log.Entries[lastLogIndex:]...)
	log.mtx.RUnlock()
	return results, nil
}

func (log *InMemoryLog[T]) Validate(index uint64, term uint32) bool {
	if len(log.Entries) < int(index) {
		return false
	}
	return log.Entries[index].Term == term
}

func (log *InMemoryLog[T]) LastTerm() (uint32, error) {
	log.mtx.RLock()
	defer log.mtx.RUnlock()
	if len(log.Entries) == 0 {
		return 0, nil
	}
	return log.Entries[len(log.Entries)-1].Term, nil
}

func (log *InMemoryLog[T]) LastLogIndex() (uint64, error) {
	log.mtx.RLock()
	defer log.mtx.RUnlock()
	if len(log.Entries) == 0 {
		return 0, nil
	}
	return log.Entries[len(log.Entries)-1].LogIndex, nil
}

func (log *InMemoryLog[T]) CommitedIndex() uint64 {
	log.mtx.RLock()
	defer log.mtx.RUnlock()
	return uint64(log.Commited)
}
