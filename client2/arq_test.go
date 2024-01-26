//go:build time

// SPDX-FileCopyrightText: © 2023 David Stainton
// SPDX-License-Identifier: AGPL-3.0-only
package client2

import (
	"os"
	"testing"
	"time"

	"github.com/charmbracelet/log"
	"github.com/katzenpost/katzenpost/core/crypto/rand"
	"github.com/katzenpost/katzenpost/core/log2"
	"github.com/stretchr/testify/require"
)

var mockRTT time.Duration = time.Second * 2

type mockComposerSender struct {
	t        *testing.T
	requests []*Request
	ch       chan bool
}

func (m *mockComposerSender) ComposeSphinxPacket(request *Request) ([]byte, []byte, time.Duration, error) {
	m.t.Log("ComposeSphinxPacket")
	m.requests = append(m.requests, request)
	return []byte("packet"), []byte("key"), mockRTT, nil
}

func (m *mockComposerSender) SendSphinxPacket(pkt []byte) error {
	defer func() {
		if len(m.requests) == 2 {
			m.ch <- false
		}
	}()
	m.t.Log("SendSphinxPacket")
	return nil
}

type mockSentEventSender struct{}

func (m *mockSentEventSender) SentEvent(response *Response) {}

// disable for now; github CI cannot properly handle any tests that use time.
func TestARQ(t *testing.T) {
	sphinxComposerSender := &mockComposerSender{
		t:        t,
		requests: make([]*Request, 0),
		ch:       make(chan bool, 0),
	}
	logbackend := os.Stderr
	logger := log.NewWithOptions(logbackend, log.Options{
		ReportTimestamp: true,
		Prefix:          "TestARQ",
		Level:           log2.ParseLevel("debug"),
	})

	m := &mockSentEventSender{}

	arq := NewARQ(sphinxComposerSender, m, logger)
	arq.Start()

	appid := new([AppIDLength]byte)
	_, err := rand.Reader.Read(appid[:])
	require.NoError(t, err)

	id := &[MessageIDLength]byte{}
	payload := []byte("hello world")
	providerHash := &[32]byte{}
	queueID := []byte{1, 2, 3, 4, 5, 6, 7}

	require.Equal(t, 0, arq.timerQueue.Len())

	err = arq.Send(appid, id, payload, providerHash, queueID)
	require.NoError(t, err)
	require.Equal(t, 1, arq.timerQueue.Len())

	surbid1 := sphinxComposerSender.requests[0].SURBID
	require.True(t, arq.Has(surbid1))

	<-sphinxComposerSender.ch

	require.Equal(t, 1, arq.timerQueue.Len())
	require.Equal(t, 2, len(sphinxComposerSender.requests))
	surbid2 := sphinxComposerSender.requests[1].SURBID

	require.True(t, arq.Has(surbid2))
	arq.HandleAck(surbid2)
	require.False(t, arq.Has(surbid2))
	require.False(t, arq.Has(surbid1))

	arq.Stop()
}
