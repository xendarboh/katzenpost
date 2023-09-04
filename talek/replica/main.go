// Copyright (C) 2023  Masala.
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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"gopkg.in/op/go-logging.v1"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/katzenpost/katzenpost/core/log"
	"github.com/katzenpost/katzenpost/server/cborplugin"
	"github.com/katzenpost/katzenpost/talek/replica/common"
	tCommon "github.com/privacylab/talek/common"
	"github.com/privacylab/talek/server"
)

type talekRequestHandler struct {
	replica *server.Replica
	log     *logging.Logger
}

func main() {
	var logLevel string
	var logDir string
	var backing string
	var cfgFile string
	var commonCfgFile string
	var listen string

	flag.StringVar(&backing, "backing", "cpu.0", "PIR daemon method")
	flag.StringVar(&cfgFile, "config", "replica.conf", "Talek Replica Configuration")
	flag.StringVar(&commonCfgFile, "common", "common.conf", "Talek Common Configuration")
	flag.StringVar(&listen, "listen", ":8080", "Listening Address")
	flag.StringVar(&logDir, "log_dir", "", "logging directory")
	flag.StringVar(&logLevel, "log_level", "DEBUG", "logging level could be set to: DEBUG, INFO, NOTICE, WARNING, ERROR, CRITICAL")
	flag.Parse()

	// Ensure that the log directory exists.
	s, err := os.Stat(logDir)
	if os.IsNotExist(err) {
		fmt.Printf("Log directory '%s' doesn't exist.", logDir)
		os.Exit(1)
	}
	if !s.IsDir() {
		fmt.Println("Log directory must actually be a directory.")
		os.Exit(1)
	}

	// Log to a file.
	logFile := path.Join(logDir, fmt.Sprintf("talek_replica.%d.log", os.Getpid()))
	logBackend, err := log.New(logFile, logLevel, false)
	if err != nil {
		panic(err)
	}
	serverLog := logBackend.GetLogger("talek_replica")

	// start service
	tmpDir, err := os.MkdirTemp("", "talek_replica")
	if err != nil {
		panic(err)
	}
	socketFile := filepath.Join(tmpDir, fmt.Sprintf("%d.talek_replica.socket", os.Getpid()))

	// instantiate replica configuration, with defaults
	serverConfig := server.Config{
		Config:           &tCommon.Config{},
		WriteInterval:    time.Second,
		ReadInterval:     time.Second,
		ReadBatch:        8,
		TrustDomain:      &tCommon.TrustDomainConfig{},
		TrustDomainIndex: 0,
	}
	// read commonCfgFile
	commonString, err := ioutil.ReadFile(commonCfgFile)
	if err != nil {
		panic(err)
	}

	// deserialize common
	if err = json.Unmarshal(commonString, serverConfig.Config); err != nil {
		panic(err)
	}
	// instantiate replica srever
	replica := server.NewReplica(serverConfig.TrustDomain.Name, backing, serverConfig)

	h := &talekRequestHandler{replica: replica, log: serverLog}
	cbserver := cborplugin.NewServer(serverLog, socketFile, new(cborplugin.RequestFactory), h)
	// emit socketFile to stdout, because this tells the mix server where to connect
	fmt.Printf("%s\n", socketFile)
	cbserver.Accept()
	cbserver.Wait()
	replica.Close()
	os.Remove(socketFile)
}

func (s *talekRequestHandler) OnCommand(cmd cborplugin.Command) (cborplugin.Command, error) {
	// deserialize request
	switch cmd := cmd.(type) {
	case *cborplugin.Request:
		// expected type
		r := new(common.ReplicaRequest)
		err := cbor.Unmarshal(cmd.Payload, r)
		if err != nil {
			return nil, err
		}
		switch r.Command {
		case common.ReplicaRequestCommand:
			// deserialize talek command
			args := new(tCommon.BatchReadRequest)
			reply := new(tCommon.BatchReadReply)
			err = cbor.Unmarshal(cmd.Payload, args)
			if err != nil {
				return nil, err
			}
			err = s.replica.BatchRead(args, reply)
			if err != nil {
				return nil, err
			}
			serialized, err := cbor.Marshal(reply)
			if err != nil {
				return nil, err
			}
			return &cborplugin.Response{Payload: serialized}, nil
		case common.ReplicaWriteCommand:
			args := new(tCommon.ReplicaWriteArgs)
			err = cbor.Unmarshal(r.Payload, args)
			if err != nil {
				return nil, err
			}
			reply := new(tCommon.ReplicaWriteReply)
			err = s.replica.Write(args, reply)
			if err != nil {
				return nil, err
			}
			serialized, err := cbor.Marshal(reply)
			if err != nil {
				return nil, err
			}
			return &cborplugin.Response{Payload: serialized}, nil
		default:
			return nil, errors.New("Invalid ReplicaCommand type")
		}
	default:
		return nil, errors.New("Invalid Command, expected cborplugin.Request")
	}

	return nil, nil
}

func (s *talekRequestHandler) RegisterConsumer(svr *cborplugin.Server) {
	//
}