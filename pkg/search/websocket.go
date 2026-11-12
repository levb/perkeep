/*
Copyright 2013 The Perkeep Authors

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
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 10 << 10
)

var debug, _ = strconv.ParseBool(os.Getenv("CAMLI_DEBUG"))

type wsHub struct {
	sh             *Handler
	register       chan *wsConn
	unregister     chan *wsConn
	watchReq       chan watchReq
	changed        <-chan struct{} // the index has indexed a schema blob; see Index.SchemaBlobIndexed
	updatedResults chan *watchedQuery
	statusUpdate   chan json.RawMessage

	// writeTimeout is how long a client's writePump waits to write a
	// message before giving up on the client: writeWait, but tests
	// change it before the hub has clients.
	writeTimeout time.Duration

	// Owned by func run:
	conns map[*wsConn]bool
}

func newWebsocketHub(sh *Handler, changed <-chan struct{}) *wsHub {
	return &wsHub{
		sh:             sh,
		register:       make(chan *wsConn), // unbuffered; issue 563
		unregister:     make(chan *wsConn), // unbuffered; issue 563
		conns:          make(map[*wsConn]bool),
		watchReq:       make(chan watchReq, buffered),
		changed:        changed,
		updatedResults: make(chan *watchedQuery, buffered),
		statusUpdate:   make(chan json.RawMessage, buffered),
		writeTimeout:   writeWait,
	}
}

func (h *wsHub) run() {
	var lastStatusMsg []byte
	for {
		select {
		case st := <-h.statusUpdate:
			const prefix = `{"tag":"_status","status":`
			lastStatusMsg = make([]byte, 0, len(prefix)+len(st)+1)
			lastStatusMsg = append(lastStatusMsg, prefix...)
			lastStatusMsg = append(lastStatusMsg, st...)
			lastStatusMsg = append(lastStatusMsg, '}')
			for c := range h.conns {
				c.put(statusTag, lastStatusMsg)
			}
		case c := <-h.register:
			h.conns[c] = true
			if lastStatusMsg != nil {
				// The web UI parses the first message as JSON.
				c.put(statusTag, lastStatusMsg)
			}
		case c := <-h.unregister:
			delete(h.conns, c)
			c.close()
		case <-h.changed:
			// The index has indexed a schema blob. Kick off standing
			// search queries to see if any changed.
			//
			// TODO: something smarter. Some queries might care about
			// all blobs, but the index only tells us of schema blobs.
			// We should track per watchedQuery which blob types the
			// search cares about.
			for conn := range h.conns {
				for _, wq := range conn.queries {
					go h.redoSearch(wq)
				}
			}
		case wr := <-h.watchReq:
			// Unsubscribe
			if wr.q == nil {
				delete(wr.conn.queries, wr.tag)
				log.Printf("websocket: removed subscription %q for %p", wr.tag, wr.conn)
				continue
			}
			// Very similar type, but semantically
			// different, so separate for now:
			wq := &watchedQuery{
				conn: wr.conn,
				tag:  wr.tag,
				q:    wr.q,
			}
			wr.conn.queries[wr.tag] = wq
			if debug {
				log.Printf("websocket: added/updated search subscription for tag %q", wr.tag)
			}
			go h.doSearch(wq)

		case wq := <-h.updatedResults:
			if !h.conns[wq.conn] || wq.conn.queries[wq.tag] == nil {
				// Client has since disconnected or unsubscribed.
				continue
			}
			wq.mu.Lock()
			lastres := wq.lastres
			wq.mu.Unlock()
			resb, err := json.Marshal(wsUpdateMessage{
				Tag:    wq.tag,
				Result: lastres,
			})
			if err != nil {
				panic(err)
			}
			wq.conn.put(wq.tag, resb)
		}
	}
}

// redoSearch is called (in its own goroutine) when a new schema blob
// arrives to note that wq might now have new results and we should
// re-run it.  But because a search can take awhile, don't run more
// than one refresh at a time.
func (h *wsHub) redoSearch(wq *watchedQuery) {
	wq.mu.Lock()
	defer wq.mu.Unlock()
	wq.dirty = true
	if wq.refreshing {
		// Somebody else is already refreshing.
		// One's enough.
		return
	}
	for wq.dirty {
		wq.refreshing = true
		wq.dirty = false
		wq.mu.Unlock() // release lock while running query; might become dirty meanwhile
		h.doSearch(wq)
		wq.mu.Lock() // before checking wq.dirty
	}
	wq.refreshing = false
}

func (h *wsHub) doSearch(wq *watchedQuery) {
	// Make our own copy, in case
	q := new(SearchQuery)
	*q = *wq.q // shallow copy, since Query will mutate its internal state fields
	if q.Describe != nil {
		q.Describe = wq.q.Describe.Clone()
	}

	res, err := h.sh.Query(context.TODO(), q)
	if err != nil {
		log.Printf("Query error: %v", err)
		return
	}
	resj, _ := json.Marshal(res)

	wq.mu.Lock()
	eq := bytes.Equal(wq.lastresj, resj)
	wq.lastres = res
	wq.lastresj = resj
	wq.mu.Unlock()
	if eq {
		// No change in search. Ignore.
		return
	}
	h.updatedResults <- wq
}

// statusTag is the tag of status messages, as the web UI knows it.
const statusTag = "_status"

// wsConn is a websocket client of the hub.
type wsConn struct {
	ws *websocket.Conn
	sh *Handler

	// queries is owned by the wsHub.run goroutine.
	queries map[string]*watchedQuery // tag -> subscription

	// ready has a value when out has messages for writePump, or the
	// connection is closed.
	ready chan struct{}

	mu     sync.Mutex // guards following
	out    []outMsg   // see put
	closed bool       // by close
}

// outMsg is a message to a websocket client: a search's result or a
// status, the whole of it.
type outMsg struct {
	tag string
	msg []byte
}

func newWSConn(sh *Handler, ws *websocket.Conn) *wsConn {
	return &wsConn{
		ws:      ws,
		sh:      sh,
		queries: make(map[string]*watchedQuery),
		ready:   make(chan struct{}, 1),
	}
}

// put has c's writePump send msg, the newest message of tag, in place
// of any message of tag it hasn't sent yet. A message is a search's
// whole result, or the whole status, so a slow client still ends up
// with the newest, and c holds at most one message of each tag. put
// never waits for the client.
//
// It's called by the hub's run goroutine.
func (c *wsConn) put(tag string, msg []byte) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if i := slices.IndexFunc(c.out, func(m outMsg) bool { return m.tag == tag }); i >= 0 {
		c.out[i].msg = msg
	} else {
		c.out = append(c.out, outMsg{tag, msg})
	}
	c.mu.Unlock()
	c.wake()
}

// close has c's writePump close the connection, without sending what
// it hasn't sent yet. It's called by the hub's run goroutine once the
// client has gone.
func (c *wsConn) close() {
	c.mu.Lock()
	c.closed = true
	c.out = nil
	c.mu.Unlock()
	c.wake()
}

func (c *wsConn) wake() {
	select {
	case c.ready <- struct{}{}:
	default: // writePump has a wake-up to take already
	}
}

// take returns the messages to send, and whether c is closed.
func (c *wsConn) take() (out []outMsg, closed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out, c.out = c.out, nil
	return out, c.closed
}

type watchedQuery struct {
	conn *wsConn
	tag  string
	q    *SearchQuery

	mu         sync.Mutex // guards following
	refreshing bool       // search is currently running
	dirty      bool       // new schema blob arrived while refreshing; another refresh due
	lastres    *SearchResult
	lastresj   []byte // as JSON
}

// watchReq is a (un)subscribe request.
type watchReq struct {
	conn *wsConn
	tag  string       // required
	q    *SearchQuery // if nil, subscribe
}

// Client->Server subscription message.
type wsClientMessage struct {
	// Tag is required.
	Tag string `json:"tag"`
	// Query is required to subscribe. If absent, it means unsubscribe.
	Query *SearchQuery `json:"query,omitempty"`
}

type wsUpdateMessage struct {
	Tag    string        `json:"tag"`
	Result *SearchResult `json:"result,omitempty"`
}

// readPump pumps messages from the websocket connection to the hub.
func (c *wsConn) readPump() {
	defer func() {
		c.sh.wsHub.unregister <- c
		c.ws.Close()
	}()
	c.ws.SetReadLimit(maxMessageSize)
	c.ws.SetReadDeadline(time.Now().Add(pongWait))
	c.ws.SetPongHandler(func(string) error { c.ws.SetReadDeadline(time.Now().Add(pongWait)); return nil })
	for {
		_, message, err := c.ws.ReadMessage()
		if err != nil {
			break
		}
		if debug {
			log.Printf("websocket: got message %#q", message)
		}
		cm := new(wsClientMessage)
		if err := json.Unmarshal(message, cm); err != nil {
			log.Printf("Ignoring bogus websocket message. Err: %v", err)
			continue
		}
		c.sh.wsHub.watchReq <- watchReq{
			conn: c,
			tag:  cm.Tag,
			q:    cm.Query,
		}
	}
}

// write writes a message with the given message type and payload.
func (c *wsConn) write(mt int, payload []byte) error {
	c.ws.SetWriteDeadline(time.Now().Add(c.sh.wsHub.writeTimeout))
	return c.ws.WriteMessage(mt, payload)
}

// writePump pumps messages from the hub to the websocket connection.
func (c *wsConn) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.ws.Close()
	}()
	for {
		select {
		case <-c.ready:
			out, closed := c.take()
			if closed {
				c.write(websocket.CloseMessage, []byte{})
				return
			}
			for _, m := range out {
				if err := c.write(websocket.TextMessage, m.msg); err != nil {
					return
				}
			}
		case <-ticker.C:
			if err := c.write(websocket.PingMessage, []byte{}); err != nil {
				return
			}
		}
	}
}

// upgrader is used in serveWebSocket to construct websocket connections.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// uses a default origin check policy
}

func (h *Handler) serveWebSocket(rw http.ResponseWriter, req *http.Request) {
	ws, err := upgrader.Upgrade(rw, req, nil)
	var he websocket.HandshakeError
	if errors.As(err, &he) {
		http.Error(rw, "Not a websocket handshake", http.StatusBadRequest)
		return
	} else if err != nil {
		log.Println(err)
		return
	}
	c := newWSConn(h, ws)
	h.wsHub.register <- c
	go c.writePump()
	c.readPump()
}
