// decoy.go - Katzenpost server decoy traffic.
// Copyright (C) 2018  Yawning Angel.
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

// Package decoy implements the decoy traffic source and sink.
package decoy

import (
	"crypto/hmac"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	mRand "math/rand"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
	"gitlab.com/yawning/avl.git"
	"gopkg.in/op/go-logging.v1"

	"github.com/katzenpost/hpqc/hash"
	"github.com/katzenpost/hpqc/rand"

	"github.com/katzenpost/katzenpost/core/epochtime"
	"github.com/katzenpost/katzenpost/core/monotime"
	"github.com/katzenpost/katzenpost/core/pki"
	"github.com/katzenpost/katzenpost/core/sphinx"
	"github.com/katzenpost/katzenpost/core/sphinx/commands"
	sConstants "github.com/katzenpost/katzenpost/core/sphinx/constants"
	"github.com/katzenpost/katzenpost/core/sphinx/geo"
	"github.com/katzenpost/katzenpost/core/sphinx/path"
	"github.com/katzenpost/katzenpost/core/worker"
	"github.com/katzenpost/katzenpost/server/internal/glue"
	"github.com/katzenpost/katzenpost/server/internal/instrument"
	"github.com/katzenpost/katzenpost/server/internal/packet"
	"github.com/katzenpost/katzenpost/server/internal/pkicache"
	"github.com/katzenpost/katzenpost/server/internal/provider/kaetzchen"
	"github.com/katzenpost/katzenpost/server/loops"
)

const maxAttempts = 3

var errMaxAttempts = errors.New("decoy: max path selection attempts exceeded")

type surbCtx struct {
	id      uint64
	eta     time.Duration
	sprpKey []byte

	epoch uint64

	etaNode *avl.Node
}

type trialLoop struct {
	epoch       uint64
	forwardPath []*sphinx.PathHop
	replyPath   []*sphinx.PathHop
	sentTime    time.Time
	isSuccess   bool
}

type decoy struct {
	worker.Worker
	sync.Mutex

	sphinx *sphinx.Sphinx
	geo    *geo.Geometry

	glue glue.Glue
	log  *logging.Logger

	recipient []byte
	rng       *mRand.Rand
	docCh     chan *pkicache.Entry

	surbETAs   *avl.Tree
	surbStore  map[uint64]*surbCtx
	surbIDBase uint64

	loopStatsLock *sync.RWMutex
	trialLoops    map[uint64]*trialLoop                 // surbCtx.id -> *trialLoop
	loopStats     map[uint64]map[uint64]*loops.LoopStat // epoch id -> surbCtx.id -> *LoopStat
}

func (d *decoy) OnNewDocument(ent *pkicache.Entry) {
	d.docCh <- ent
}

func (d *decoy) OnPacket(pkt *packet.Packet) {
	// Note: This is called from the crypto worker context, which is "fine".
	defer pkt.Dispose()

	if !pkt.IsSURBReply() {
		panic("BUG: OnPacket called with non-SURB Reply")
	}

	// Ensure that the SURB Reply is destined for the correct recipient,
	// and that it was generated by this decoy instance.  Note that neither
	// fields are visible to any other party involved.
	if !hmac.Equal(pkt.Recipient.ID[:], d.recipient) {
		d.log.Debugf("Dropping packet: %v (Invalid recipient)", pkt.ID)
		instrument.PacketsDropped()
		return
	}

	idBase, id := binary.BigEndian.Uint64(pkt.SurbReply.ID[0:]), binary.BigEndian.Uint64(pkt.SurbReply.ID[8:])
	if idBase != d.surbIDBase {
		d.log.Debugf("Dropping packet: %v (Invalid SURB ID base: %v)", pkt.ID, idBase)
		instrument.PacketsDropped()
		return
	}

	d.log.Debugf("Response packet: %v", pkt.ID)

	ctx := d.loadAndDeleteSURBCtx(id)
	if ctx == nil {
		d.log.Debugf("Dropping packet: %v (Unknown SURB ID: 0x%08x)", pkt.ID, id)
		instrument.PacketsDropped()
		return
	}

	if _, err := d.sphinx.DecryptSURBPayload(pkt.Payload, ctx.sprpKey); err != nil {
		d.log.Debugf("Dropping packet: %v (SURB ID: 0x08x%): %v", pkt.ID, id, err)
		instrument.PacketsDropped()
		return
	}

	// remove loop from trialLoops and add it to
	// loopStats map
	d.loopStatsLock.Lock()
	defer d.loopStatsLock.Unlock()
	trial, ok := d.trialLoops[ctx.id]
	if !ok {
		d.log.Error("failed to find decoy loop entry in trialLoops map")
		return
	}
	result := &loops.LoopStat{
		ForwardPath: trial.forwardPath,
		ReplyPath:   trial.replyPath,
		SentTime:    trial.sentTime,
		IsSuccess:   true,
	}
	_, ok = d.loopStats[ctx.epoch]
	if !ok {
		d.loopStats[ctx.epoch] = make(map[uint64]*loops.LoopStat)
	}
	d.loopStats[ctx.epoch][ctx.id] = result
	delete(d.trialLoops, ctx.id)
}

func (d *decoy) worker() {
	const maxDuration = math.MaxInt64

	wakeInterval := time.Duration(maxDuration)
	timer := time.NewTimer(wakeInterval)
	defer timer.Stop()

	var docCache *pkicache.Entry
	for {
		var timerFired bool
		select {
		case <-d.HaltCh():
			d.log.Debugf("Terminating gracefully.")
			return
		case newEnt := <-d.docCh:
			if !d.glue.Config().Debug.SendDecoyTraffic {
				d.log.Debugf("Received PKI document but decoy traffic is disabled, ignoring.")
				instrument.IgnoredPKIDocs()
				continue
			}

			now, _, _ := epochtime.Now()
			if entEpoch := newEnt.Epoch(); entEpoch != now {
				d.log.Debugf("Received PKI document for non-current epoch, ignoring: %v", entEpoch)
				instrument.IgnoredPKIDocs()
				continue
			}
			if d.glue.Config().Server.IsProvider {
				d.log.Debugf("Received PKI document when Provider, ignoring (not supported yet).")
				instrument.IgnoredPKIDocs()
				continue
			}
			d.log.Debugf("Received new PKI document for epoch: %v", now)
			instrument.PKIDocs(fmt.Sprintf("%v", now))
			docCache = newEnt

			d.sendDecoyLoopStats(docCache)
		case <-timer.C:
			timerFired = true
		}

		now, _, _ := epochtime.Now()
		if docCache == nil || docCache.Epoch() != now {
			d.log.Debugf("Suspending operation till the next PKI document.")
			wakeInterval = time.Duration(maxDuration)
		} else {
			// The timer fired, and there is a valid document for this epoch.
			if timerFired {
				d.sendDecoyPacket(docCache)
			}

			// Schedule the next decoy packet.
			//
			// This closely follows how the mailproxy worker schedules
			// outgoing sends, except that the SendShift value is ignored.
			//
			// TODO: Eventually this should use separate parameters.
			doc := docCache.Document()
			wakeMsec := uint64(rand.Exp(d.rng, doc.LambdaM))
			if wakeMsec > doc.LambdaMMaxDelay {
				wakeMsec = doc.LambdaMMaxDelay
			}
			wakeInterval = time.Duration(wakeMsec) * time.Millisecond
			d.log.Debugf("Next wakeInterval: %v", wakeInterval)

			d.sweepSURBCtxs()
		}
		if !timerFired && !timer.Stop() {
			select {
			case <-d.HaltCh():
				d.log.Debugf("Terminating gracefully.")
				return
			case <-timer.C:
			}
		}
		timer.Reset(wakeInterval)
	}
}

func findService(rng *mRand.Rand, service string, doc *pki.Document) (string, *pki.MixDescriptor, error) {
	var providerDesc *pki.MixDescriptor
	var serviceName string
	for _, idx := range rng.Perm(len(doc.Providers)) {
		desc := doc.Providers[idx]
		params, ok := desc.Kaetzchen[service]
		if !ok {
			continue
		}
		serviceName, ok = params["endpoint"].(string)
		if !ok {
			continue
		}
		providerDesc = desc
		break
	}
	if providerDesc == nil {
		return "", nil, errors.New("Failed to find suitable provider")
	}

	return serviceName, providerDesc, nil
}

func (d *decoy) sendDecoyPacket(ent *pkicache.Entry) {
	// TODO: (#52) Do nothing if the rate limiter would discard the packet(?).

	selfDesc := ent.Self()
	if selfDesc.Provider {
		// The code doesn't handle this correctly yet.  It does need to
		// happen eventually though.
		panic("BUG: Provider generated decoy traffic not supported yet")
	}
	doc := ent.Document()

	// TODO: The path selection maybe should be more strategic/systematic
	// rather than randomized, but this is obviously correct and leak proof.

	// Find a random Provider that is running a loop/discard service.
	loopRecip, providerDesc, err := findService(d.rng, kaetzchen.EchoCapability, doc)
	if err != nil {
		d.log.Debugf("Failed to find suitable provider")
		return
	}

	d.sendLoopPacket(doc, []byte(loopRecip), selfDesc, providerDesc)
	return
}

func (d *decoy) sendDecoyLoopStats(ent *pkicache.Entry) {
	selfDesc := ent.Self()
	if selfDesc.Provider {
		// The code doesn't handle this correctly yet.  It does need to
		// happen eventually though.
		panic("BUG: Provider generated decoy traffic not supported yet")
	}
	doc := ent.Document()
	// Find a random Provider that is running a loop/discard service.
	var providerDesc *pki.MixDescriptor
	var destRecip string
	for _, idx := range d.rng.Perm(len(doc.Providers)) {
		desc := doc.Providers[idx]
		params, ok := desc.Kaetzchen[kaetzchen.StatsCapability]
		if !ok {
			continue
		}
		destRecip, ok = params["endpoint"].(string)
		if !ok {
			continue
		}
		providerDesc = desc
		break
	}
	if providerDesc == nil {
		d.log.Warningf("Failed to find suitable provider")
		return
	}

	d.sendLoopStatsPacket(doc, []byte(destRecip), selfDesc, providerDesc)
	return
}

func (d *decoy) sendLoopStatsPacket(doc *pki.Document, recipient []byte, src, dst *pki.MixDescriptor) {
	var surbID [sConstants.SURBIDLength]byte
	d.makeSURBID(&surbID)

	for attempts := 0; attempts < maxAttempts; attempts++ {
		now := time.Now()

		fwdPath, then, err := path.New(d.rng, d.geo, doc, recipient, src, dst, &surbID, time.Now(), false, true)
		if err != nil {
			d.log.Debugf("Failed to select forward path: %v", err)
			return
		}

		revPath, then, err := path.New(d.rng, d.geo, doc, d.recipient, dst, src, &surbID, then, false, false)
		if err != nil {
			d.log.Debugf("Failed to select reverse path: %v", err)
			return
		}

		if deltaT := then.Sub(now); deltaT < epochtime.Period*2 {
			zeroBytes := make([]byte, d.geo.UserForwardPayloadLength)
			payload := make([]byte, 2, 2+d.geo.SURBLength+d.geo.UserForwardPayloadLength)
			payload[0] = 1 // Packet has a SURB.

			surb, k, err := d.sphinx.NewSURB(rand.Reader, revPath)
			if err != nil {
				d.log.Debugf("Failed to generate SURB: %v", err)
			}

			epoch, _, _ := epochtime.Now()
			myEpoch := epoch - 1

			d.loopStatsLock.Lock()
			defer d.loopStatsLock.Unlock()
			loopList := make([]*loops.LoopStat, len(d.loopStats[myEpoch]))
			i := 0
			for _, loop := range d.loopStats[myEpoch] {
				loopList[i] = loop
				i++
			}
			mixid := hash.Sum256From(d.glue.IdentityPublicKey())
			loopstats := &loops.LoopStats{
				Epoch:           myEpoch,
				MixIdentityHash: &mixid,
				Stats:           loopList,
			}

			blob, err := cbor.Marshal(loopstats)
			if err != nil {
				d.log.Errorf("failed to marshal cbor blob: %s", err)
				return
			}

			payload = append(payload, surb...)
			payload = append(payload, blob...)
			payload = append(payload, zeroBytes[len(blob):]...)

			// TODO: This should probably also store path information,
			// so that it's possible to figure out which links/nodes
			// are causing issues.
			ctx := &surbCtx{
				id:      binary.BigEndian.Uint64(surbID[8:]),
				eta:     monotime.Now() + deltaT,
				sprpKey: k,
			}
			d.storeSURBCtx(ctx)

			pkt, err := d.sphinx.NewPacket(rand.Reader, fwdPath, payload)
			if err != nil {
				d.log.Debugf("Failed to generate Sphinx packet: %v", err)
				return
			}

			d.logPath(doc, fwdPath)
			d.logPath(doc, revPath)
			d.log.Debugf("Dispatching loop packet: SURB ID: 0x%08x", binary.BigEndian.Uint64(surbID[8:]))

			d.dispatchPacket(fwdPath, pkt)
			return
		}
	}

	d.log.Debugf("Failed to generate loop packet: %v", errMaxAttempts)
}

func (d *decoy) sendLoopPacket(doc *pki.Document, recipient []byte, src, dst *pki.MixDescriptor) {
	var surbID [sConstants.SURBIDLength]byte
	d.makeSURBID(&surbID)

	for attempts := 0; attempts < maxAttempts; attempts++ {
		now := time.Now()

		fwdPath, then, err := path.New(d.rng, d.geo, doc, recipient, src, dst, &surbID, time.Now(), false, true)
		if err != nil {
			d.log.Debugf("Failed to select forward path: %v", err)
			return
		}

		revPath, then, err := path.New(d.rng, d.geo, doc, d.recipient, dst, src, &surbID, then, false, false)
		if err != nil {
			d.log.Debugf("Failed to select reverse path: %v", err)
			return
		}

		if deltaT := then.Sub(now); deltaT < epochtime.Period*2 {
			zeroBytes := make([]byte, d.geo.UserForwardPayloadLength)
			payload := make([]byte, 2, 2+d.geo.SURBLength+d.geo.UserForwardPayloadLength)
			payload[0] = 1 // Packet has a SURB.

			surb, k, err := d.sphinx.NewSURB(rand.Reader, revPath)
			if err != nil {
				d.log.Debugf("Failed to generate SURB: %v", err)
			}
			payload = append(payload, surb...)
			payload = append(payload, zeroBytes...)

			epoch, _, _ := epochtime.Now()

			ctx := &surbCtx{
				id:      binary.BigEndian.Uint64(surbID[8:]),
				eta:     monotime.Now() + deltaT,
				sprpKey: k,
				epoch:   epoch,
			}
			d.storeSURBCtx(ctx)

			pkt, err := d.sphinx.NewPacket(rand.Reader, fwdPath, payload)
			if err != nil {
				d.log.Debugf("Failed to generate Sphinx packet: %v", err)
				return
			}

			d.logPath(doc, fwdPath)
			d.logPath(doc, revPath)
			d.log.Debugf("Dispatching loop packet: SURB ID: 0x%08x", binary.BigEndian.Uint64(surbID[8:]))

			d.dispatchPacket(fwdPath, pkt)

			trialStat := &trialLoop{
				epoch:       epoch,
				forwardPath: fwdPath,
				replyPath:   revPath,
				sentTime:    time.Now(),
				isSuccess:   false,
			}

			d.loopStatsLock.Lock()
			d.trialLoops[ctx.id] = trialStat
			d.loopStatsLock.Unlock()

			return
		}
	}

	d.log.Debugf("Failed to generate loop packet: %v", errMaxAttempts)
}

func (d *decoy) sendDiscardPacket(doc *pki.Document, recipient []byte, src, dst *pki.MixDescriptor) {
	payload := make([]byte, 2+d.geo.SURBLength+d.geo.UserForwardPayloadLength)

	for attempts := 0; attempts < maxAttempts; attempts++ {
		now := time.Now()

		fwdPath, then, err := path.New(d.rng, d.geo, doc, recipient, src, dst, nil, time.Now(), false, true)
		if err != nil {
			d.log.Debugf("Failed to select forward path: %v", err)
			return
		}

		if then.Sub(now) < epochtime.Period*2 {
			pkt, err := d.sphinx.NewPacket(rand.Reader, fwdPath, payload)
			if err != nil {
				d.log.Debugf("Failed to generate Sphinx packet: %v", err)
				return
			}
			d.logPath(doc, fwdPath)
			d.dispatchPacket(fwdPath, pkt)
			return
		}
	}

	d.log.Debugf("Failed to generate discard decoy packet: %v", errMaxAttempts)
}

func (d *decoy) dispatchPacket(fwdPath []*sphinx.PathHop, raw []byte) {
	pkt, err := packet.New(raw, d.geo)
	if err != nil {
		d.log.Debugf("Failed to allocate packet: %v", err)
		return
	}
	pkt.NextNodeHop = &commands.NextNodeHop{}
	copy(pkt.NextNodeHop.ID[:], fwdPath[0].ID[:])
	pkt.DispatchAt = monotime.Now()

	d.log.Debugf("Dispatching packet: %v", pkt.ID)
	d.glue.Connector().DispatchPacket(pkt)
}

func (d *decoy) makeSURBID(surbID *[sConstants.SURBIDLength]byte) {
	// Generate a random SURB ID, prefixed with the time that the decoy
	// instance was initialized.

	binary.BigEndian.PutUint64(surbID[0:], d.surbIDBase)
	binary.BigEndian.PutUint64(surbID[8:], d.rng.Uint64())
}

func (d *decoy) logPath(doc *pki.Document, p []*sphinx.PathHop) error {
	s, err := path.ToString(doc, p)
	if err != nil {
		return err
	}

	for _, v := range s {
		d.log.Debug(v)
	}
	return nil
}

func (d *decoy) storeSURBCtx(ctx *surbCtx) {
	d.Lock()
	defer d.Unlock()

	ctx.etaNode = d.surbETAs.Insert(ctx)
	if ctx.etaNode.Value.(*surbCtx) != ctx {
		panic("inserting surbCtx failed, duplicate eta+id?")
	}

	d.surbStore[ctx.id] = ctx
}

func (d *decoy) loadAndDeleteSURBCtx(id uint64) *surbCtx {
	d.Lock()
	defer d.Unlock()

	ctx := d.surbStore[id]
	if ctx == nil {
		return nil
	}
	delete(d.surbStore, id)

	d.surbETAs.Remove(ctx.etaNode)
	ctx.etaNode = nil

	return ctx
}

func (d *decoy) sweepSURBCtxs() {
	d.Lock()
	defer d.Unlock()

	if d.surbETAs.Len() == 0 {
		d.log.Debugf("Sweep: No outstanding SURBs.")
		return
	}

	now := monotime.Now()
	slack := time.Duration(d.glue.Config().Debug.DecoySlack) * time.Millisecond
	// instead of if ctx.eta + slack > now { break } in each loop iteration
	// we precompute it:
	now_minus_slack := now - slack

	var swept int
	iter := d.surbETAs.Iterator(avl.Forward)
	for node := iter.First(); node != nil; node = iter.Next() {
		ctx := node.Value.(*surbCtx)
		if ctx.eta > now_minus_slack {
			break
		}

		delete(d.surbStore, ctx.id)

		// TODO: At some point, this should do more than just log.
		d.log.Debugf("Sweep: Lost SURB ID: 0x%08x ETA: %v (DeltaT: %v)", ctx.id, ctx.eta, now-ctx.eta)
		swept++
		// modification is unsupported EXCEPT "removing the current
		// Node", see godoc for avl/avl.go:Iterator
		d.surbETAs.Remove(node)

		// remove trial loop stat from trialLoops map
		// and add the entry as a failed loop into our loopStats map
		d.loopStatsLock.Lock()
		trial, ok := d.trialLoops[ctx.id]
		if !ok {
			d.loopStatsLock.Unlock()
			return
		}
		delete(d.trialLoops, ctx.id)
		var isSuccess bool
		_, ok = d.loopStats[ctx.epoch]
		if !ok {
			d.loopStats[ctx.epoch] = make(map[uint64]*loops.LoopStat)
			isSuccess = false
		} else {
			_, isSuccess = d.loopStats[ctx.epoch][ctx.id]
		}
		if !isSuccess {
			result := &loops.LoopStat{
				ForwardPath: trial.forwardPath,
				ReplyPath:   trial.replyPath,
				SentTime:    trial.sentTime,
				IsSuccess:   isSuccess,
			}
			d.loopStats[ctx.epoch][ctx.id] = result
		}
		d.loopStatsLock.Unlock()
	}

	d.log.Debugf("Sweep: Count: %v (Removed: %v, Elapsed: %v)", len(d.surbStore), swept, monotime.Now()-now)
}

// New constructs a new decoy instance.
func New(glue glue.Glue) (glue.Decoy, error) {
	s, err := sphinx.FromGeometry(glue.Config().SphinxGeometry)
	if err != nil {
		return nil, err
	}
	d := &decoy{
		geo:       glue.Config().SphinxGeometry,
		sphinx:    s,
		glue:      glue,
		log:       glue.LogBackend().GetLogger("decoy"),
		recipient: make([]byte, sConstants.RecipientIDLength),
		rng:       rand.NewMath(),
		docCh:     make(chan *pkicache.Entry),
		surbETAs: avl.New(func(a, b interface{}) int {
			surbCtxA, surbCtxB := a.(*surbCtx), b.(*surbCtx)
			switch {
			case surbCtxA.eta < surbCtxB.eta:
				return -1
			case surbCtxA.eta > surbCtxB.eta:
				return 1
			case surbCtxA.id < surbCtxB.id:
				return -1
			case surbCtxA.id > surbCtxB.id:
				return 1
			default:
				return 0
			}
		}),
		surbStore:  make(map[uint64]*surbCtx),
		surbIDBase: uint64(time.Now().Unix()),
		trialLoops: make(map[uint64]*trialLoop),
		loopStats:  make(map[uint64]map[uint64]*loops.LoopStat),
	}
	if _, err := io.ReadFull(rand.Reader, d.recipient); err != nil {
		return nil, err
	}

	d.Go(d.worker)
	return d, nil
}
