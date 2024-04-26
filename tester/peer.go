package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Peer struct {
	host string
	port uint16
}

type PeerSet []Peer

type Reply struct {
	id   string
	body string
	rtt  time.Duration
}

func NewPeer(base string, port uint16) Peer {
	return Peer{base, port}
}

func (peer Peer) Get(ctx context.Context, id string) (Reply, error) {
	var reply Reply
	reply.id = id
	reply.rtt = 0

	begin := time.Now()
	url := fmt.Sprintf("http://%s:%d/%s", peer.host, peer.port, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return reply, fmt.Errorf("http request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return reply, fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		reply.rtt = time.Since(begin)
		return reply, errors.New("no such document")
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return reply, fmt.Errorf("body: %w", err)
	}
	reply.rtt = time.Since(begin)
	reply.body = string(body)
	return reply, nil
}

func (peer Peer) Post(ctx context.Context, body string) (Reply, error) {
	var reply Reply
	reply.body = body
	reply.rtt = 0

	begin := time.Now()
	url := fmt.Sprintf("http://%s:%d", peer.host, peer.port)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, strings.NewReader(body))
	if err != nil {
		return reply, fmt.Errorf("http request: %w", err)
	}
	req.Header.Add("Content-Type", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return reply, fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		reply.rtt = time.Since(begin)
		return reply, errors.New("")
	}

	idB, err := io.ReadAll(resp.Body)
	if err != nil {
		return reply, fmt.Errorf("body: %w", err)
	}
	reply.rtt = time.Since(begin)
	reply.id = string(idB)
	return reply, nil
}

func (peer Peer) Put(ctx context.Context, id string, body string) (Reply, error) {
	var reply Reply
	reply.id = id
	reply.body = body
	reply.rtt = 0

	begin := time.Now()
	url := fmt.Sprintf("http://%s:%d/%s", peer.host, peer.port, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, strings.NewReader(body))
	if err != nil {
		return reply, fmt.Errorf("http: %w", err)
	}
	req.Header.Add("Content-Type", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return reply, fmt.Errorf("http send: %w", err)
	}
	reply.rtt = time.Since(begin)
	if resp.StatusCode != 200 {
		return reply, errors.New("put request failed")
	}
	return reply, nil
}

func (peer Peer) Delete(ctx context.Context, id string) (Reply, error) {
	var reply Reply
	reply.id = id
	reply.body = ""

	begin := time.Now()
	url := fmt.Sprintf("http://%s:%d/%s", peer.host, peer.port, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		reply.rtt = 0
		return reply, fmt.Errorf("http: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return reply, fmt.Errorf("http send: %w", err)
	}
	reply.rtt = time.Since(begin)
	if resp.StatusCode != 200 {
		return reply, errors.New("delete request failed")
	}
	return reply, nil
}

type CheckReplicationConfig struct {
	ValidationInterval time.Duration
}

type ReplicationStats struct {
	ok    bool
	rtts  []time.Duration
	total time.Duration
}

func (peerSet PeerSet) CheckReplication(ctx context.Context, conf CheckReplicationConfig, testFn func(peer *Peer, ctx context.Context) (Reply, error)) []ReplicationStats {
	results := make([]ReplicationStats, len(peerSet))
	wg := &sync.WaitGroup{}
	wg.Add(len(peerSet))
	for i, peer := range peerSet {
		timer := time.NewTicker(conf.ValidationInterval)
		go func() {
			defer wg.Done()
			start := time.Now()
			results[i] = ReplicationStats{
				ok:    false,
				rtts:  []time.Duration{},
				total: 0,
			}
			for {
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
				}
				resp, err := testFn(&peer, ctx)
				if resp.rtt > 0 {
					results[i].rtts = append(results[i].rtts, resp.rtt)
				}
				if err != nil {
					continue
				}
				results[i].ok = true
				results[i].total = time.Since(start)
				return
			}
		}()
	}
	wg.Wait()
	return nil
}
