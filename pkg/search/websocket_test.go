/*
Copyright 2026 The Perkeep Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package search

import (
	"context"
	"fmt"
	"testing"
	"time"

	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/index"
	"perkeep.org/pkg/index/indextest"
	"perkeep.org/pkg/schema"
	"perkeep.org/pkg/test"
)

// wsTest is a search handler on an in-memory index with a corpus, for
// tests of the handler's websocket hub.
type wsTest struct {
	t   *testing.T
	id  *indextest.IndexDeps
	h   *Handler
	seq int
}

func newWSTest(t *testing.T) *wsTest {
	idx := index.NewMemoryIndex()
	idx.Logger = test.NewLogger(t, "")
	id := indextest.NewIndexDeps(idx)
	id.Fataler = t
	corpus, err := idx.KeepInMemory()
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(idx, index.NewOwner(indextest.KeyID, indextest.PubKey.BlobRef()))
	h.SetCorpus(corpus)
	return &wsTest{t: t, id: id, h: h}
}

// conn registers a websocket connection with the hub, with no socket
// and no writePump: what the hub sends it stays in its send channel
// until the test receives it. It watches all permanodes.
func (wt *wsTest) conn() *wsConn {
	c := &wsConn{
		send:    make(chan []byte, 256),
		sh:      wt.h,
		queries: make(map[string]*watchedQuery),
	}
	wt.h.wsHub.register <- c
	wt.h.wsHub.watchReq <- watchReq{conn: c, tag: "q", q: &SearchQuery{
		Constraint: &Constraint{Permanode: &PermanodeConstraint{}},
		Limit:      50,
	}}
	return c
}

// fill fills c's send channel, as a client that has stopped reading
// from its socket leaves it.
func fill(c *wsConn) {
	for {
		select {
		case c.send <- []byte("{}"):
		default:
			return
		}
	}
}

// receive has the index receive a new permanode and a claim of its
// title, each through blobserver.Receive, which tells the index's blob
// hub, and so the search handler, as an upload does. It fails the test
// if the index hasn't received one of them within 10 seconds.
func (wt *wsTest) receive() {
	wt.t.Helper()
	wt.seq++
	pn := wt.id.Sign(schema.NewUnsignedPermanode())
	claim := schema.NewSetAttributeClaim(pn.BlobRef(), "title", fmt.Sprint(wt.seq))
	claim.SetClaimDate(time.Unix(int64(1e9+wt.seq), 0))
	ctx := context.Background()
	for _, tb := range []*test.Blob{pn, wt.id.Sign(claim)} {
		if _, err := wt.id.BlobSource.ReceiveBlob(ctx, tb.BlobRef(), tb.Reader()); err != nil {
			wt.t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, err := blobserver.Receive(ctx, wt.id.Index, tb.BlobRef(), tb.Reader())
			done <- err
		}()
		select {
		case err := <-done:
			if err != nil {
				wt.t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			wt.t.Fatalf("the index is still receiving a blob of permanode %d after 10s", wt.seq)
		}
	}
}

// TestStalledWebsocketDoesntLockIndex checks that a websocket client
// that has stopped reading its search updates doesn't stop the index
// receiving blobs, nor searches of it.
func TestStalledWebsocketDoesntLockIndex(t *testing.T) {
	wt := newWSTest(t)
	fill(wt.conn())

	// More than the hub's buffer of received blobs holds, so that the
	// notification of the hub blocks, if the hub is blocked.
	for range 3 * buffered {
		wt.receive()
	}

	searched := make(chan error, 1)
	go func() {
		_, err := wt.h.Query(context.Background(), &SearchQuery{
			Constraint: &Constraint{Permanode: &PermanodeConstraint{}},
			Limit:      1,
		})
		searched <- err
	}()
	select {
	case err := <-searched:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a search is still waiting after 10s")
	}
}
