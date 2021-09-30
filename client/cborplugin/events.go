// events.go - cbor plugin events
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

package cborplugin

import (
	"github.com/katzenpost/katzenpost/client/events"
)

type Event struct {
	ConnectionStatusEvent     *events.ConnectionStatusEvent
	MessageReplyEvent         *events.MessageReplyEvent
	MessageSentEvent          *events.MessageSentEvent
	MessageIDGarbageCollected *events.MessageIDGarbageCollected
	NewDocumentEvent          *events.NewDocumentEvent
	SpoolCreated              *SpoolCreated
}

type SpoolCreated struct {
	SpoolReadDescriptor *SpoolReadDescriptor
}
