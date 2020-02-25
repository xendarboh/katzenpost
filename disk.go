// disk.go - statefile worker, serialization and encryption
// Copyright (C) 2019  David Stainton.
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

package catshadow

import (
	"errors"
	"io/ioutil"
	"os"
	"time"

	"github.com/katzenpost/core/crypto/ecdh"
	"github.com/katzenpost/core/crypto/rand"
	"github.com/katzenpost/core/worker"
	"github.com/katzenpost/memspool/client"
	"github.com/ugorji/go/codec"
	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/nacl/secretbox"
	"gopkg.in/op/go-logging.v1"
)

const (
	keySize   = 32
	nonceSize = 24
)

// Message encapsulates message that is sent or received.
type Message struct {
	Plaintext []byte
	Timestamp time.Time
	Outbound  bool
}

// State is the struct type representing the Client's state
// which is encrypted and persisted to disk.
type State struct {
	SpoolReadDescriptor *client.SpoolReadDescriptor
	Contacts            []*Contact
	User                string
	Provider            string
	LinkKey             *ecdh.PrivateKey
	Conversations       map[string]map[MessageID]*Message
}

// StateWriter takes ownership of the Client's encrypted statefile
// and has a worker goroutine which writes updates to disk.
type StateWriter struct {
	worker.Worker

	log *logging.Logger

	stateCh   chan []byte
	stateFile string

	key [32]byte
}

func GetStateFromFile(stateFile string, passphrase []byte) (*State, *[keySize]byte, error) {
	secret := argon2.Key(passphrase, nil, 3, 32*1024, 4, keySize)
	rawFile, err := ioutil.ReadFile(stateFile)
	if err != nil {
		return nil, nil, err
	}
	nonce := [nonceSize]byte{}
	copy(nonce[:], rawFile[:nonceSize])
	ciphertext := rawFile[nonceSize:]
	key := [keySize]byte{}
	copy(key[:], secret)
	plaintext, ok := secretbox.Open(nil, ciphertext, &nonce, &key)
	if !ok {
		return nil, nil, errors.New("failed to decrypted statefile")
	}
	state := new(State)
	err = codec.NewDecoderBytes(plaintext, cborHandle).Decode(state)
	if err != nil {
		return nil, nil, err
	}
	return state, &key, nil
}

// LoadStateWriter decrypts the given stateFile and returns the State
// as well as a new StateWriter.
func LoadStateWriter(log *logging.Logger, stateFile string, passphrase []byte) (*StateWriter, *State, error) {
	worker := &StateWriter{
		log:       log,
		stateCh:   make(chan []byte),
		stateFile: stateFile,
	}
	state, key, err := GetStateFromFile(stateFile, passphrase)
	if err != nil {
		return nil, nil, err
	}
	copy(worker.key[:], key[:])
	return worker, state, nil
}

// NewStateWriter is a constructor for StateWriter which is to be used when creating
// the statefile for the first time.
func NewStateWriter(log *logging.Logger, stateFile string, passphrase []byte) (*StateWriter, error) {
	secret := argon2.Key(passphrase, nil, 3, 32*1024, 4, keySize)
	worker := &StateWriter{
		log:       log,
		stateCh:   make(chan []byte),
		stateFile: stateFile,
	}
	copy(worker.key[:], secret[0:32])
	return worker, nil
}

// Start starts the StateWriter's worker goroutine.
func (w *StateWriter) Start() {
	w.log.Debug("StateWriter starting worker")
	w.Go(w.worker)
}

func (w *StateWriter) writeState(payload []byte) error {
	nonce := [nonceSize]byte{}
	_, err := rand.Reader.Read(nonce[:])
	if err != nil {
		return err
	}
	ciphertext := secretbox.Seal(nil, payload, &nonce, &w.key)
	out, err := os.OpenFile(w.stateFile+".tmp", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	outBytes := append(nonce[:], ciphertext...)
	_, err = out.Write(outBytes)
	if err != nil {
		return err
	}
	if err := os.Remove(w.stateFile + "~"); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(w.stateFile, w.stateFile+"~"); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(w.stateFile+".tmp", w.stateFile); err != nil {
		return err
	}
	if err := os.Remove(w.stateFile + "~"); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (w *StateWriter) worker() {
	for {
		select {
		case <-w.HaltCh():
			w.log.Debugf("Terminating gracefully.")
			return
		case newState := <-w.stateCh:
			err := w.writeState(newState)
			if err != nil {
				w.log.Errorf("Failure to write state to disk: %s", err)
				panic(err)
			}
		}
	}
}
