package raft

import (
	"math/rand"
	"time"
)

type Config struct {
	Addr string

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}

func DefaultConfig() Config {
	return Config{
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 250 * time.Millisecond,
	}
}

func (self *Config) RandElectionTimeout() time.Duration {
	diff := int64((self.ElectionTimeoutMax - self.ElectionTimeoutMin) / time.Millisecond)
	off := time.Duration(rand.Int63n(diff)) * time.Millisecond
	return off + self.ElectionTimeoutMin
}
