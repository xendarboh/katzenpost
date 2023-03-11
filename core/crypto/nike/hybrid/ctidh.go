//go:build ctidh
// +build ctidh

package hybrid

import (
	"github.com/katzenpost/katzenpost/core/crypto/nike"
	"github.com/katzenpost/katzenpost/core/crypto/nike/ctidh"
	"github.com/katzenpost/katzenpost/core/crypto/nike/ecdh"
	"github.com/katzenpost/katzenpost/core/crypto/rand"
)

var CTIDHX25519 nike.Scheme = &scheme{
	name:   "CTIDH-X25519",
	first:  ctidh.CTIDHScheme,
	second: ecdh.NewEcdhNike(rand.Reader),
}
