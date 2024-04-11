package raft

import (
	"math/rand"
	"time"
)

type Config struct {
	QueueSize int

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatPeriod    time.Duration
}

func DefaultConfig() Config {
	return Config{
		QueueSize:          128,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 250 * time.Millisecond,
		HeartbeatPeriod:    100 * time.Millisecond,
	}
}

func DebugConfig() Config {
	return Config{
		QueueSize:          128,
		ElectionTimeoutMin: 2 * time.Second,
		ElectionTimeoutMax: 5 * time.Second,
		HeartbeatPeriod:    time.Second,
	}
}

func (self *Config) RandElectionTimeout() time.Duration {
	diff := int64((self.ElectionTimeoutMax - self.ElectionTimeoutMin) / time.Millisecond)
	off := time.Duration(rand.Int63n(diff)) * time.Millisecond
	return off + self.ElectionTimeoutMin
}
