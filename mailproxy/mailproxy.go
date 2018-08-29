// mailproxy.go - Katzenpost mailproxy configuration generator
// Copyright (C) 2018  David Stainton.
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

// Package mailproxy provides a library for generating mailproxy
// configuration and key material.
package mailproxy

import (
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/katzenpost/core/crypto/ecdh"
	"github.com/katzenpost/core/crypto/rand"
	"github.com/katzenpost/core/utils"
)

const (
	// NOTICE: change me to correct playground information
	RegistrationAddr    = "127.0.0.1:8080"
	providerName        = "playground"
	providerKeyPin      = "imigzI26tTRXyYLXujLEPI9QrNYOEgC4DElsFdP9acQ="
	authorityAddr       = "127.0.0.1:29483"
	authorityPublicKey  = "o4w1Nyj/nKNwho5SWfAIfh7SMU8FRx52nMHGgYsMHqQ="
	mailproxyConfigName = "mailproxy.toml"
)

func makeConfig(user string, dataDir string) []byte {
	configFormatStr := `
[Proxy]
  POP3Address = "127.0.0.1:2524"
  SMTPAddress = "127.0.0.1:2525"
  DataDir = "%s"

[Logging]
  Disable = false
  Level = "NOTICE"

[NonvotingAuthority]
  [NonvotingAuthority.PlaygroundAuthority]
    Address = "%s"
    PublicKey = "%s"

[[Account]]
  User = "%s"
  Provider = "%s"
  ProviderKeyPin = "%s"
  Authority = "PlaygroundAuthority"

[Management]
  Enable = false
`
	return []byte(fmt.Sprintf(configFormatStr, dataDir, authorityAddr, authorityPublicKey, user, providerName, providerKeyPin))
}

// GenerateConfig is used to generate mailproxy configuration
// files including key material in the specific dataDir directory.
// It returns the link layer authentication public key and the
// identity public key or an error upon failure. This function returns
// the public keys so that they may be used with the Provider
// account registration process.
func GenerateConfig(user string, dataDir string) (*ecdh.PublicKey, *ecdh.PublicKey, error) {
	// Initialize the per-account directory.
	id := fmt.Sprintf("%s@%s", user, providerName)
	basePath := filepath.Join(dataDir, id)
	if err := utils.MkDataDir(basePath); err != nil {
		return nil, nil, err
	}

	// generate and write keys to disk
	linkPriv := filepath.Join(basePath, "link.private.pem")
	linkPub := filepath.Join(basePath, "link.public.pem")
	linkPrivateKey, err := ecdh.Load(linkPriv, linkPub, rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	idPriv := filepath.Join(basePath, "identity.private.pem")
	idPub := filepath.Join(basePath, "identity.public.pem")
	identityPrivateKey, err := ecdh.Load(idPriv, idPub, rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	// write the configuration file
	configData := makeConfig(user, dataDir)
	configPath := filepath.Join(dataDir, mailproxyConfigName)
	err = ioutil.WriteFile(configPath, configData, 0600)
	if err != nil {
		return nil, nil, err
	}
	return linkPrivateKey.PublicKey(), identityPrivateKey.PublicKey(), nil
}
