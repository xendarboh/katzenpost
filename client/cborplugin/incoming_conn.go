// incoming_conn.go - katzenpost client plugins server incoming connection
// Copyright (C) 2021  David Stainton.
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

// Package cborplugin is a plugin system allowing mix network services
// to be added in any language. It communicates queries and responses to and from
// the mix server using CBOR over HTTP over UNIX domain socket. Beyond that,
// a client supplied SURB is used to route the response back to the client
// as described in our Kaetzchen specification document:
//
// https://github.com/katzenpost/docs/blob/master/specs/kaetzchen.rst
//
package cborplugin

import (
	"container/list"
	"io"
	"net"

	"github.com/fxamacker/cbor/v2"
	"gopkg.in/op/go-logging.v1"

	"github.com/katzenpost/katzenpost/client/constants"
	"github.com/katzenpost/katzenpost/client/events"
	"github.com/katzenpost/katzenpost/core/crypto/rand"
	"github.com/katzenpost/katzenpost/core/log"
)

type incomingConn struct {
	log     *logging.Logger
	conn    net.Conn
	e       *list.Element
	encoder *cbor.Encoder
	decoder *cbor.Decoder

	server  *Server
	session Session

	closeConnectionCh chan bool
}

func newIncomingConn(logBackend *log.Backend, s *Server, conn net.Conn, session Session) *incomingConn {
	c := &incomingConn{
		server:            s,
		conn:              conn,
		session:           session,
		closeConnectionCh: make(chan bool),
		encoder:           cbor.NewEncoder(conn),
		decoder:           cbor.NewDecoder(conn),
	}
	c.log = logBackend.GetLogger("incoming conn")

	c.log.Debugf("New incoming connection: %v", conn.RemoteAddr())

	// Note: Unlike most other things, this does not spawn the worker here,
	// because the worker needs to be spawned after the struct is added to
	// the connection list.

	return c
}

func (c *incomingConn) Close() {
	c.closeConnectionCh <- true
}

func (c *incomingConn) worker() {
	defer func() {
		c.log.Debug("closing")
		c.conn.Close()
		c.server.onClosedConn(c)
	}()

	for {
		select {
		case <-c.closeConnectionCh:
			return
		default:
		}

		command := &ControlCommand{}
		c.decoder.Decode(command)
		c.processCommand(command)
	}
}

func (c *incomingConn) WriteEvent(event Event) {
	err := c.encoder.Encode(event)
	if err != nil {
		c.log.Errorf("WriteEvent failure: %s", err)
	}
}

func (c *incomingConn) processCommand(command *ControlCommand) {
	switch {
	case command.SendMessage != nil:
		id := [constants.MessageIDLength]byte{}
		_, err := io.ReadFull(rand.Reader, id[:])
		if err != nil {
			c.log.Error(err.Error())
		}

		err = c.session.SendMessage(command.SendMessage.Recipient, command.SendMessage.Provider, command.SendMessage.Payload, id)
		if err != nil {
			c.log.Error(err.Error())
		}

		c.server.ReplyToSentMessage(&id, c)
		return
	case command.GetConsensus != nil:
		c.WriteEvent(Event{
			NewDocumentEvent: &events.NewDocumentEvent{
				Document: c.server.consensus(),
			},
		})
	case command.CreateRemoteSpool != nil:
		spoolService, err := c.session.GetService("spool")
		if err != nil {
			c.log.Error(err.Error())
			continue
		}

		spoolReadDescriptor, err := memspoolclient.NewSpoolReadDescriptor(spoolService.Name, spoolService.Provider, c.session)
		if err != nil {
			c.log.Error(err.Error())
			continue
		}

		c.WriteEvent(Event{
			SpoolCreated: &SpoolCreated{
				SpoolReadDescriptor: spoolReadDescriptor,
			},
		})
	case command.ConnectionStatus != nil:
		c.WriteEvent(Event{
			ConnectionStatusEvent: &events.ConnectionStatusEvent{
				IsConnected: c.server.ConnectionStatus(),
			},
		})
	default:
		c.log.Error("one command is required")
		return
	}
}
