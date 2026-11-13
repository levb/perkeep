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

package main

// A minimal websocket client (RFC 6455, standard library only): it
// handles fragmented messages, answers pings, and can
// send unsolicited pongs so that a deliberately slow reader is not closed
// by the server's 60 s pong deadline (a slow reader's answer to a ping sits
// behind its backlog).

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

type wsClient struct {
	c   net.Conn
	br  *bufio.Reader
	wmu sync.Mutex // serializes frame writes
}

// wsDial connects and upgrades. The whole handshake has a deadline.
func wsDial(hostport, path string, timeout time.Duration) (*wsClient, error) {
	c, err := net.DialTimeout("tcp", hostport, timeout)
	if err != nil {
		return nil, err
	}
	c.SetDeadline(time.Now().Add(timeout))
	var key [16]byte
	rand.Read(key[:])
	if _, err := fmt.Fprintf(c, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n",
		path, hostport, base64.StdEncoding.EncodeToString(key[:])); err != nil {
		c.Close()
		return nil, err
	}
	br := bufio.NewReaderSize(c, 32<<10)
	status, err := br.ReadString('\n')
	if err != nil || !strings.Contains(status, " 101 ") {
		c.Close()
		return nil, fmt.Errorf("handshake: %q %v", status, err)
	}
	for {
		l, err := br.ReadString('\n')
		if err != nil {
			c.Close()
			return nil, err
		}
		if l == "\r\n" {
			break
		}
	}
	c.SetDeadline(time.Time{})
	return &wsClient{c: c, br: br}, nil
}

// send writes one masked frame.
func (w *wsClient) send(op byte, p []byte) error {
	f := make([]byte, 0, 14+len(p))
	f = append(f, 0x80|op)
	switch n := len(p); {
	case n < 126:
		f = append(f, 0x80|byte(n))
	case n <= 0xffff:
		f = append(f, 0x80|126, byte(n>>8), byte(n))
	default:
		f = append(f, 0x80|127)
		f = binary.BigEndian.AppendUint64(f, uint64(n))
	}
	var mask [4]byte
	rand.Read(mask[:])
	f = append(f, mask[:]...)
	for i, b := range p {
		f = append(f, b^mask[i%4])
	}
	w.wmu.Lock()
	defer w.wmu.Unlock()
	w.c.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := w.c.Write(f)
	return err
}

func (w *wsClient) subscribe(tag, query string) error {
	return w.send(1, []byte(`{"tag":"`+tag+`","query":`+query+`}`))
}

func (w *wsClient) unsubscribe(tag string) error {
	return w.send(1, []byte(`{"tag":"`+tag+`"}`))
}

// keepalive sends an unsolicited pong every 20 s until stop is closed.
func (w *wsClient) keepalive(stop <-chan struct{}) {
	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			if w.send(0xa, nil) != nil {
				return
			}
		}
	}
}

var errClosedByServer = errors.New("server sent a close frame")

// next returns the next complete data message. With keep, payload is the
// whole message; without, it is at most its first 64 bytes (enough for the
// tag). n is the message size. Pings are answered here. A close frame
// returns errClosedByServer.
func (w *wsClient) next(keep bool) (payload []byte, n int, err error) {
	for {
		var h [2]byte
		if _, err = io.ReadFull(w.br, h[:]); err != nil {
			return nil, n, err
		}
		fin := h[0]&0x80 != 0
		op := h[0] & 0x0f
		l := uint64(h[1] & 0x7f)
		switch l {
		case 126:
			var b [2]byte
			if _, err = io.ReadFull(w.br, b[:]); err != nil {
				return nil, n, err
			}
			l = uint64(binary.BigEndian.Uint16(b[:]))
		case 127:
			var b [8]byte
			if _, err = io.ReadFull(w.br, b[:]); err != nil {
				return nil, n, err
			}
			l = binary.BigEndian.Uint64(b[:])
		}
		if l > 64<<20 {
			return nil, n, fmt.Errorf("frame of %d bytes", l)
		}
		if op >= 8 { // control frame, may sit between fragments
			cp := make([]byte, l)
			if _, err = io.ReadFull(w.br, cp); err != nil {
				return nil, n, err
			}
			switch op {
			case 8:
				return nil, n, errClosedByServer
			case 9:
				w.send(0xa, cp)
			}
			continue
		}
		take := l
		if !keep {
			take = min(l, uint64(max(0, 64-len(payload))))
		}
		buf := make([]byte, take)
		if _, err = io.ReadFull(w.br, buf); err != nil {
			return nil, n, err
		}
		payload = append(payload, buf...)
		if _, err = io.CopyN(io.Discard, w.br, int64(l-take)); err != nil {
			return nil, n, err
		}
		n += int(l)
		if fin {
			return payload, n, nil
		}
	}
}

// msgTag extracts the tag from a hub message, which gorilla's JSON
// encoding of wsUpdateMessage always starts with: {"tag":"...".
func msgTag(p []byte) string {
	const pre = `{"tag":"`
	s := string(p)
	if !strings.HasPrefix(s, pre) {
		return ""
	}
	s = s[len(pre):]
	if i := strings.IndexByte(s, '"'); i >= 0 {
		return s[:i]
	}
	return ""
}
