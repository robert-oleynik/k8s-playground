package raft

import (
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

type Log[T interface{}] interface {
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

type MemoryLog[T interface{}] struct {
	mtx      *sync.RWMutex
	Entries  []LogEntry[T]
	Commited uint64
}

func NewMemoryLog[T interface{}]() *MemoryLog[T] {
	return &MemoryLog[T]{
		mtx:      &sync.RWMutex{},
		Entries:  []LogEntry[T]{},
		Commited: 0,
	}
}

func (log *MemoryLog[T]) Append(i uint64, entries []LogEntry[T]) error {
	log.mtx.Lock()
	defer log.mtx.Unlock()
	if i < uint64(len(log.Entries)) {
		copy(log.Entries[i:], entries[:len(entries[i:])])
		if len(log.Entries[i:]) < len(entries) {
			log.Entries = append(log.Entries, entries[len(log.Entries[i:]):]...)
		}
	} else if i == uint64(len(log.Entries)) {
		log.Entries = append(log.Entries, entries...)
	} else {
		return fmt.Errorf("log: failed to insert at %d in log with length %d",
			i, len(log.Entries))
	}
	return nil
}

func (log *MemoryLog[T]) Commit(logIndex uint64) ([]LogEntry[T], error) {
	zap.L().Debug("commit",
		zap.Uint64("commited", log.Commited),
		zap.Uint64("index", logIndex),
		zap.Int("count", len(log.Entries)))
	var commited []LogEntry[T]
	log.mtx.Lock()
	defer log.mtx.Unlock()
	if logIndex <= log.Commited {
		return []LogEntry[T]{}, nil
	}
	commit := min(uint64(len(log.Entries)), logIndex)
	commited = log.Entries[log.Commited:commit]
	log.Commited = commit
	return commited, nil
}

func (log *MemoryLog[T]) History(lastLogIndex uint64) ([]LogEntry[T], error) {
	results := []LogEntry[T]{}
	log.mtx.RLock()
	results = append(results, log.Entries[lastLogIndex:]...)
	log.mtx.RUnlock()
	return results, nil
}

func (log *MemoryLog[T]) Validate(index uint64, term uint32) bool {
	if len(log.Entries) < int(index) {
		return false
	}
	return log.Entries[index].Term == term
}

func (log *MemoryLog[T]) Term(index uint64) (uint32, error) {
	log.mtx.RLock()
	defer log.mtx.RUnlock()
	if index == 0 {
		return 0, nil
	} else if index <= uint64(len(log.Entries)) {
		return log.Entries[index-1].Term, nil
	}
	return 0, errors.New(fmt.Sprintf("No entry with index %d", index))
}

func (log *MemoryLog[T]) LastLogIndex() (uint64, error) {
	log.mtx.RLock()
	defer log.mtx.RUnlock()
	if len(log.Entries) == 0 {
		return 0, nil
	}
	return log.Entries[len(log.Entries)-1].LogIndex, nil
}

func (log *MemoryLog[T]) CommitedIndex() uint64 {
	log.mtx.RLock()
	defer log.mtx.RUnlock()
	return uint64(log.Commited)
}
