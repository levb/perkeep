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
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"perkeep.org/pkg/blob"
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
// and no writePump: what the hub has for it waits in it until the test
// takes it. It watches all permanodes, and conn returns once the hub has
// given it the search's first result.
func (wt *wsTest) conn() *wsConn {
	wt.t.Helper()
	c := newWSConn(wt.h, nil)
	wt.h.wsHub.register <- c
	wt.h.wsHub.watchReq <- watchReq{conn: c, tag: "q", q: &SearchQuery{
		Constraint: &Constraint{Permanode: &PermanodeConstraint{}},
		Limit:      50,
	}}
	wt.result(c, "")
	return c
}

// result takes what the hub has for c, as c's writePump would, until a
// search result that contains want, and fails the test if there is none
// within 10 seconds.
func (wt *wsTest) result(c *wsConn, want string) {
	wt.t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case <-c.ready:
			out, closed := c.take()
			if closed {
				wt.t.Fatalf("the connection was closed; want a search result with %q", want)
			}
			for _, m := range out {
				if m.tag == "q" && bytes.Contains(m.msg, []byte(want)) {
					return
				}
			}
		case <-timeout:
			wt.t.Fatalf("no search result with %q after 10s", want)
		}
	}
}

// pending returns a copy of what the hub has for c that c hasn't taken.
func pending(c *wsConn) []outMsg {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.out)
}

// receive has the index receive a new permanode and a claim of its
// title. It fails the test if the index hasn't received one of them
// within 10 seconds. It returns the permanode.
func (wt *wsTest) receive() blob.Ref {
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
	return pn.BlobRef()
}

// TestStalledWebsocketDoesntLockIndex checks that a websocket client
// that has stopped reading its search updates doesn't stop the index
// receiving blobs, nor searches of it: nothing on the index's path, and
// nothing in the hub, waits for a client to read. When the hub waited on
// a client with a full queue, a stalled client could stop the index for
// good.
func TestStalledWebsocketDoesntLockIndex(t *testing.T) {
	wt := newWSTest(t)
	wt.conn() // and never take from it again

	// More results than the old per-client queue held (256): with
	// it, the hub blocked on this client, and through it the index.
	for range 600 {
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

// TestWebsocketSlowClientGetsLatest checks that the hub keeps only the
// newest result of a search for a client that isn't reading, rather
// than queue them or wait for it, goes on updating the others, and that
// the slow client gets the newest result once it reads again.
func TestWebsocketSlowClientGetsLatest(t *testing.T) {
	wt := newWSTest(t)
	reading := wt.conn()
	slow := wt.conn()

	var last blob.Ref
	for range 30 {
		last = wt.receive()
		if n := len(pending(slow)); n > 2 {
			t.Fatalf("the hub has %d messages for the slow client; want at most one per tag, 2", n)
		}
	}
	wt.result(reading, last.String())

	// The slow client's search runs on its own, so its newest result
	// can come after the reading client's.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var q []outMsg
		for _, m := range pending(slow) {
			if m.tag == "q" {
				q = append(q, m)
			}
		}
		if len(q) > 1 {
			t.Fatalf("the hub has %d results of the slow client's search; want 1", len(q))
		}
		if len(q) == 1 && bytes.Contains(q[0].msg, []byte(last.String())) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the slow client's pending result doesn't have the newest permanode %v after 10s", last)
		}
		time.Sleep(10 * time.Millisecond)
	}
	wt.result(slow, last.String())
}

// TestWebsocketUnsubscribeWhileWriting has a client subscribe and
// unsubscribe while what the hub has for it is being taken, as
// writePump does, so that -race checks the hub's handling of a
// subscription against the client's writer.
func TestWebsocketUnsubscribeWhileWriting(t *testing.T) {
	wt := newWSTest(t)
	c := wt.conn()
	stop := make(chan struct{})
	taken := make(chan struct{})
	go func() {
		defer close(taken)
		for {
			select {
			case <-c.ready:
				c.take()
			case <-stop:
				return
			}
		}
	}()
	for i := range 20 {
		tag := fmt.Sprint("t", i)
		wt.h.wsHub.watchReq <- watchReq{conn: c, tag: tag, q: &SearchQuery{
			Constraint: &Constraint{Permanode: &PermanodeConstraint{}},
		}}
		wt.receive()
		wt.h.wsHub.watchReq <- watchReq{conn: c, tag: tag} // unsubscribe
	}
	close(stop)
	<-taken
}

// closeNotifyListener is a net.Listener whose accepted connections
// have small socket buffers, so that writes to a client that doesn't
// read back up soon, and that closes closed when the server closes one.
type closeNotifyListener struct {
	net.Listener
	closed chan struct{}
	once   sync.Once
}

func (ln *closeNotifyListener) Accept() (net.Conn, error) {
	c, err := ln.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetWriteBuffer(4 << 10)
	}
	return &closeNotifyConn{Conn: c, ln: ln}, nil
}

type closeNotifyConn struct {
	net.Conn
	ln *closeNotifyListener
}

func (c *closeNotifyConn) Close() error {
	c.ln.once.Do(func() { close(c.ln.closed) })
	return c.Conn.Close()
}

// TestWebsocketStalledClientDisconnected checks that the server closes
// the connection of a websocket client that has stopped reading, once
// a write to it has waited longer than the hub's write timeout.
func TestWebsocketStalledClientDisconnected(t *testing.T) {
	wt := newWSTest(t)
	wt.h.wsHub.writeTimeout = 100 * time.Millisecond
	wt.h.wsHub.searchInterval = 0 // a result per blob, to back the writes up soon

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cln := &closeNotifyListener{Listener: ln, closed: make(chan struct{})}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(wt.h.serveWebSocket))
	srv.Listener = cln
	srv.Start()
	defer srv.Close()

	dialer := websocket.Dialer{
		NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := new(net.Dialer).DialContext(ctx, network, addr)
			if tc, ok := c.(*net.TCPConn); ok {
				tc.SetReadBuffer(4 << 10)
			}
			return c, err
		},
	}
	ws, _, err := dialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	const sub = `{"tag":"q","query":{"sort":"-created","constraint":{"permanode":{}},"limit":50,"describe":{"depth":1}}}`
	if err := ws.WriteMessage(websocket.TextMessage, []byte(sub)); err != nil {
		t.Fatal(err)
	}
	ws.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, _, err := ws.ReadMessage(); err != nil {
		t.Fatalf("reading the search's first result: %v", err)
	}

	// Stop reading. Each blob changes the result, so the server has
	// more to write, until its writes back up.
	for range 300 {
		select {
		case <-cln.closed:
			return
		default:
		}
		wt.receive()
	}
	select {
	case <-cln.closed:
	case <-time.After(10 * time.Second):
		t.Fatal("the server hasn't closed the connection of a client that stopped reading")
	}
}

// TestWebsocketSearchInterval checks that a subscription's searches
// start at least the hub's search interval apart, so that a burst of
// blobs shares searches, and that the last search still sees the last
// blob.
func TestWebsocketSearchInterval(t *testing.T) {
	wt := newWSTest(t)
	const interval = 100 * time.Millisecond
	wt.h.wsHub.searchInterval = interval
	c := wt.conn()

	// Take results as they come, as writePump does, and count them
	// until one has the last blob.
	lastc := make(chan string, 1)
	counted := make(chan int, 1) // or -1 after 10s without one
	go func() {
		n, want := 0, ""
		var newest []byte
		for {
			select {
			case want = <-lastc:
			case <-c.ready:
				out, _ := c.take()
				for _, m := range out {
					if m.tag == "q" {
						n++
						newest = m.msg
					}
				}
			case <-time.After(10 * time.Second):
				counted <- -1
				return
			}
			if want != "" && bytes.Contains(newest, []byte(want)) {
				counted <- n
				return
			}
		}
	}()

	start := time.Now()
	var last blob.Ref
	for range 30 {
		last = wt.receive()
	}
	lastc <- last.String()
	n := <-counted
	elapsed := time.Since(start)
	if n < 0 {
		t.Fatalf("no search result with the last permanode %v", last)
	}
	// One search can start at once, then one per interval.
	if max := 2 + int(elapsed/interval); n > max {
		t.Errorf("got %d results in %v; want at most %d with searches %v apart", n, elapsed, max, interval)
	}
}
