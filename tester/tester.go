package main

import (
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/go-errors/errors"
)

type Tester struct {
	Proxies      []NodeProxy
	Service      Peer
	ClusterNodes PeerSet
}

type TestCase interface {
	Name() string
	Run(tester *Tester) (bool, error)
}

type TestReport struct {
	Passed bool
	Error  *errors.Error
}

func NewTester(proxies ...NodeProxy) *Tester {
	peers := make([]Peer, len(proxies))
	for i, proxy := range proxies {
		peers[i] = NewPeer("localhost", proxy.Port)
	}
	return &Tester{
		Proxies:      proxies,
		Service:      peers[0],
		ClusterNodes: PeerSet(peers[1:]),
	}
}

func (tester *Tester) Run(testCase TestCase) tea.Cmd {
	return func() tea.Msg {
		passed, err := testCase.Run(tester)
		var nerr *errors.Error = nil
		if err != nil {
			nerr = errors.New(err)
		}
		return &TestReport{
			Passed: passed,
			Error:  nerr,
		}
	}
}

type PassTest struct{}
type FailureTest struct{}
type ErrorTest struct{}

func (t PassTest) Name() string {
	return "This test always pass"
}

func (t FailureTest) Name() string {
	return "This test always fails"
}

func (t ErrorTest) Name() string {
	return "This test always errors"
}

func (t PassTest) Run(tester *Tester) (bool, error) {
	time.Sleep(1 * time.Second)
	return true, nil
}

func (t FailureTest) Run(tester *Tester) (bool, error) {
	time.Sleep(1 * time.Second)
	return false, nil
}

func (t ErrorTest) Run(tester *Tester) (bool, error) {
	time.Sleep(1 * time.Second)
	return false, errors.New("an error")
}
