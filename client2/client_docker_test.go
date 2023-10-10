package client2

import (
	"testing"
	"time"

	"github.com/katzenpost/katzenpost/client2/config"
	cpki "github.com/katzenpost/katzenpost/core/pki"
	"github.com/stretchr/testify/require"
)

func TestDockerClientSendReceive(t *testing.T) {
	cfg, err := config.LoadFile("testdata/client.toml")
	require.NoError(t, err)

	egressSize := 100
	d, err := NewDaemon(cfg, egressSize)
	require.NoError(t, err)
	err = d.Start()
	require.NoError(t, err)

	time.Sleep(time.Second * 3)

	thin := NewThinClient()
	t.Log("thin client Dialing")
	err = thin.Dial()
	require.NoError(t, err)
	require.Nil(t, err)
	t.Log("thin client connected")

	t.Log("thin client getting PKI doc")
	doc := thin.PKIDocument()
	require.NotNil(t, doc)
	require.NotEqual(t, doc.LambdaP, 0.0)

	pingTargets := []*cpki.MixDescriptor{}
	for i := 0; i < len(doc.Providers); i++ {
		_, ok := doc.Providers[i].Kaetzchen["echo"]
		if ok {
			pingTargets = append(pingTargets, doc.Providers[i])
		}
	}
	require.True(t, len(pingTargets) > 0)
	message1 := []byte("hello alice, this is bob.")
	nodeIdKey := pingTargets[0].IdentityKey.Sum256()

	t.Log("thin client send ping")
	thin.SendMessage(message1, &nodeIdKey, []byte("testdest"))

	time.Sleep(time.Second * 3)

	message2 := thin.ReceiveMessage()

	require.NoError(t, err)
	require.NotEqual(t, message1, []byte{})
	require.NotEqual(t, message2, []byte{})
	require.Equal(t, message1, message2[:len(message1)])

	d.Halt()
}