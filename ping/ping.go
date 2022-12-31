// ping.go - Katzenpost ping tool
// Copyright (C) 2021  David Stainton
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/fxamacker/cbor/v2"
	"github.com/katzenpost/katzenpost/client"
	"github.com/katzenpost/katzenpost/client/constants"
	"github.com/katzenpost/katzenpost/client/utils"
	"github.com/katzenpost/katzenpost/core/crypto/rand"
)

func sendPing(geo *sphinx.Geometry, session *client.Session, serviceDesc *utils.ServiceDescriptor, printDiff bool) bool {
	msg := make([]byte, geo.ForwardPayloadLen)
	_, err := rand.Reader.Read(pingPayload)
	if err != nil {
		panic(err)
	}

	reply, err := session.BlockingSendUnreliableMessage(serviceDesc.Name, serviceDesc.Provider, msg)

	if err != nil {
		fmt.Printf("\nerror: %v\n", err)
		fmt.Printf(".") // Fail, did not receive a reply.
		return false
	}

	if bytes.Equal(reply, msg) {
		// OK, received identical payload in reply.
		return true
	} else {
		// Fail, received unexpected payload in reply.

		if printDiff {
			fmt.Printf("\nReply payload: %x\nOriginal payload: %x\n", reply, msg)
		}
		return false
	}
}

func sendPings(geo *sphinx.Geometry, session *client.Session, serviceDesc *utils.ServiceDescriptor, count int, concurrency int, printDiff bool) {
	if concurrency > constants.MaxEgressQueueSize {
		fmt.Printf("error: concurrency cannot be greater than MaxEgressQueueSize (%d)\n", constants.MaxEgressQueueSize)
		return
	}
	fmt.Printf("Sending %d Sphinx packets to %s@%s\n", count, serviceDesc.Name, serviceDesc.Provider)

	var passed, failed uint64

	wg := new(sync.WaitGroup)
	sem := make(chan struct{}, concurrency)

	for i := 0; i < count; i++ {

		sem <- struct{}{}

		wg.Add(1)

		// make new goroutine for each ping to send them in parallel
		go func() {
			if sendPing(geo, session, serviceDesc, printDiff) {
				fmt.Printf("!")
				atomic.AddUint64(&passed, 1)
			} else {
				fmt.Printf("~")
				atomic.AddUint64(&failed, 1)
			}
			wg.Done()
			<-sem
		}()
	}
	fmt.Printf("\n")

	wg.Wait()

	percent := (float64(passed) * float64(100)) / float64(count)
	fmt.Printf("Success rate is %f percent %d/%d)\n", percent, passed, count)
}
