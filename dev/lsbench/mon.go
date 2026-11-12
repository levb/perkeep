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

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// reader is one subscribed websocket client. delay > 0 makes it a slow
// reader; tr != nil makes it the tracer, which parses every result.
type reader struct {
	id    int
	delay time.Duration
	tr    *tracer
	w     *wsClient

	mu          sync.Mutex
	msgs        int // result messages (tag q1)
	status      int // status messages
	bytes       int
	closed      bool
	byServer    bool // the server ended it (close frame or EOF), not us
	closedAt    float64
	closeErr    string
	msgsAtClose int
	hist        []shown      // slow readers: what they displayed, and when
	lastAt      atomic.Int64 // ns since t0 of the last message
	drain       atomic.Bool  // stop pausing: read the backlog to see whether the server closed us
}

// shown is a result a client displayed (read) at time t.
type shown struct {
	t  float64
	st state
}

func (rd *reader) count() int {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	return rd.msgs
}

// shows reports whether the last result rd displayed is final.
func (rd *reader) shows(final state) bool {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	return len(rd.hist) > 0 && rd.hist[len(rd.hist)-1].st.equal(final)
}

func (rd *reader) isClosed() bool {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	return rd.closed
}

func (rd *reader) loop(r *run, stop <-chan struct{}) {
	kaStop := make(chan struct{})
	defer close(kaStop)
	go rd.w.keepalive(kaStop)
	for {
		p, n, err := rd.w.next(rd.tr != nil || rd.delay > 0)
		t := r.now()
		if err != nil {
			rd.mu.Lock()
			rd.closed, rd.closedAt, rd.closeErr, rd.msgsAtClose = true, t, err.Error(), rd.msgs
			select {
			case <-stop:
			default:
				rd.byServer = errors.Is(err, errClosedByServer) || !strings.Contains(err.Error(), "use of closed")
			}
			rd.mu.Unlock()
			return
		}
		tag := msgTag(p)
		rd.lastAt.Store(int64(time.Since(r.t0)))
		rd.mu.Lock()
		if tag == "q1" {
			rd.msgs++
			rd.bytes += n
		} else {
			rd.status++
		}
		rd.mu.Unlock()
		if tag == "q1" && rd.tr != nil {
			rd.tr.add(t, p, n)
			if *flagMsgCap > 0 && rd.count() >= *flagMsgCap {
				r.fail(fmt.Sprintf("the tracer got %d result messages (-msgcap): a slow reader's queue could fill on build P", *flagMsgCap))
				return
			}
		}
		if tag == "q1" && rd.delay > 0 {
			var m struct {
				Result *searchResult `json:"result"`
			}
			if json.Unmarshal(p, &m) == nil && m.Result != nil {
				st := m.Result.state()
				rd.mu.Lock()
				rd.hist = append(rd.hist, shown{t, st})
				rd.mu.Unlock()
			}
		}
		if rd.delay > 0 && !rd.drain.Load() {
			select {
			case <-time.After(rd.delay):
			case <-stop:
			}
		}
	}
}

// missing is how many of final's permanodes st does not show with content.
func missing(st, final state) int {
	have := map[string]bool{}
	for i, ref := range st.refs {
		if st.content[i] {
			have[ref] = true
		}
	}
	n := 0
	for i, ref := range final.refs {
		if final.content[i] && !have[ref] {
			n++
		}
	}
	return n
}

// summarizeReaders also says, for the slow readers, how stale what they
// display is: when pk-put exits (tPut), and at the end of the run (after
// the converge phase), as the number of the final result's permanodes
// they do not show; and when, if ever, they first displayed the final
// result. A client the hub dropped stays stale; one that is sent the
// latest result catches up.
func summarizeReaders(r *run, rds []*reader, final state, tPut, tEnd float64) {
	var fast, slow []float64
	drops, slowDrops := 0, 0
	var slowAt []float64
	var sb strings.Builder
	for _, rd := range rds {
		rd.mu.Lock()
		fmt.Fprintf(&sb, "%d\t%v\t%d\t%d\t%d\t%v\t%v\t%.3f\t%d\t%q\n", rd.id, rd.delay, rd.msgs, rd.status, rd.bytes, rd.closed, rd.byServer, rd.closedAt, rd.msgsAtClose, rd.closeErr)
		if rd.delay == 0 {
			fast = append(fast, float64(rd.msgs))
			if rd.byServer {
				drops++
			}
		} else {
			slow = append(slow, float64(rd.msgs))
			if rd.byServer {
				slowDrops++
				slowAt = append(slowAt, rd.closedAt)
			}
		}
		rd.mu.Unlock()
	}
	os.WriteFile(r.path("readers.tsv"), []byte(sb.String()), 0600)
	var stalePut, staleEnd, convAt []float64
	nconv := 0
	for _, rd := range rds {
		if rd.delay == 0 {
			continue
		}
		rd.mu.Lock()
		var atPut, atEnd state
		ca := -1.0
		for _, h := range rd.hist {
			if h.t > tEnd {
				break // read while draining, not displayed by a slow tab
			}
			if h.t <= tPut {
				atPut = h.st
			}
			atEnd = h.st
			if ca < 0 && h.st.equal(final) {
				ca = h.t
			}
		}
		rd.mu.Unlock()
		stalePut = append(stalePut, float64(missing(atPut, final)))
		staleEnd = append(staleEnd, float64(missing(atEnd, final)))
		if atEnd.equal(final) {
			nconv++
		}
		if ca >= 0 {
			convAt = append(convAt, ca-tPut)
		}
	}
	if len(stalePut) > 0 {
		r.sum["slow_missing_at_put_exit"] = pcts(stalePut)
		r.sum["slow_missing_at_end"] = pcts(staleEnd)
		r.sum["slow_converged"] = nconv
		r.sum["slow_final_shown_after_put_exit_s"] = pcts(convAt)
	}
	if len(fast) > 0 {
		r.sum["clients"] = len(fast)
		r.sum["client_msgs"] = pcts(fast)
		r.sum["client_dropped"] = drops
		if len(rds) > 0 && rds[0].delay == 0 && len(rds[0].closeErr) > 0 && rds[0].byServer {
			r.sum["tracer_dropped_at_s"] = round2(rds[0].closedAt)
		}
		var b float64
		for _, rd := range rds {
			b += float64(rd.bytes)
		}
		r.sum["client_bytes_total"] = b
	}
	if len(slow) > 0 {
		r.sum["slow"] = len(slow)
		r.sum["slow_msgs"] = pcts(slow)
		r.sum["slow_dropped"] = slowDrops
		if len(slowAt) > 0 {
			r.sum["slow_dropped_at_s"] = pcts(slowAt)
		}
	}
}

// tracer records, for client 0, every result: when, how big, and which
// permanodes it shows (with or without their content).
type tracer struct {
	r            *run
	mu           sync.Mutex
	n            int
	last         state
	lastT        float64
	firstAny     map[string]float64
	firstContent map[string]float64
	log          strings.Builder
	parseErrs    int
}

func (tr *tracer) add(t float64, p []byte, n int) {
	var m struct {
		Result *searchResult `json:"result"`
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if err := json.Unmarshal(p, &m); err != nil || m.Result == nil {
		tr.parseErrs++
		return
	}
	st := m.Result.state()
	tr.n++
	tr.last, tr.lastT = st, t
	nc := 0
	for i, ref := range st.refs {
		if _, ok := tr.firstAny[ref]; !ok {
			tr.firstAny[ref] = t
		}
		if st.content[i] {
			nc++
			if _, ok := tr.firstContent[ref]; !ok {
				tr.firstContent[ref] = t
			}
		}
	}
	first := ""
	if len(st.refs) > 0 {
		first = st.refs[0]
	}
	fmt.Fprintf(&tr.log, "%.4f\t%d\t%d\t%d\t%s\n", t, n, len(st.refs), nc, first)
}

func (tr *tracer) lastState() (state, float64) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.last, tr.lastT
}

// summarize computes the live-update latencies (pk-put printing a
// permanode's ref, to the tracer first showing it with its content) and
// whether, and when, the tracer reached the final result.
func (tr *tracer) summarize(final state, r *run, claimAck map[string][3]float64) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	os.WriteFile(r.path("tracer.tsv"), []byte(tr.log.String()), 0600)
	r.sum["tracer_msgs"] = tr.n
	r.sum["tracer_parse_errors"] = tr.parseErrs
	conv := tr.last.equal(final)
	r.sum["converged"] = conv
	r.acksMu.Lock()
	defer r.acksMu.Unlock()
	if conv {
		ref := 0.0 // when the last of the final result's permanodes was acknowledged
		for _, p := range final.refs {
			ref = max(ref, r.acks[p])
		}
		if *flagRestart {
			ref = r.sum["t_restarted"].(float64)
		}
		r.sum["converged_lag_s"] = round2(tr.lastT - ref)
	}
	if *flagRestart {
		return
	}
	// lat: from the camliContent claim's upload request arriving (at the
	// proxy) to the tracer first showing the permanode with its content;
	// latAck: the same from pk-put printing the claim's ref, which comes
	// after the server answered (can be negative: the update can beat the
	// upload's own response). latAny: from pk-put printing the permanode's
	// ref to its first showing, with or without content.
	var lat, latAck, latAny []float64
	nperm := 0
	var sb strings.Builder
	for ref, ta := range r.acks {
		t1, ok1 := tr.firstAny[ref]
		if !ok1 {
			continue // not a permanode, or never shown
		}
		nperm++
		latAny = append(latAny, 1000*(t1-ta))
		tc, okc := tr.firstContent[ref]
		cl, okcl := claimAck[ref]
		if okc && okcl {
			if cl[0] >= 0 {
				lat = append(lat, 1000*(tc-cl[0]))
			}
			latAck = append(latAck, 1000*(tc-cl[2]))
			fmt.Fprintf(&sb, "%s\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\n", ref, ta, cl[0], cl[1], cl[2], t1, tc)
		}
	}
	os.WriteFile(r.path("live-latency.tsv"), []byte(sb.String()), 0600)
	files, _ := r.sum["files"].(int)
	r.sum["live_shown"] = nperm
	r.sum["live_shown_with_content"] = len(latAck)
	if files > 0 {
		r.sum["live_shown_frac"] = round2(float64(len(latAck)) / float64(files))
	}
	r.sum["live_ms"] = pcts(lat)
	r.sum["live_from_ack_ms"] = pcts(latAck)
	r.sum["live_any_ms"] = pcts(latAny)
}

// prober sends the UI's query every 50 ms on one kept-alive connection.
type prober struct {
	r   *run
	mu  sync.Mutex
	rec [][3]float64 // start, ms, ok
}

func (p *prober) loop(stop <-chan struct{}) {
	u := p.r.searchURL()
	for {
		select {
		case <-stop:
			return
		case <-p.r.abortc:
			return
		default:
		}
		t0 := p.r.now()
		p.r.probeOut.Store(int64(time.Since(p.r.t0)))
		_, err := httpPost(u, uiQuery, 5*time.Minute)
		p.r.probeOut.Store(0)
		ms := 1000 * (p.r.now() - t0)
		ok := 1.0
		if err != nil {
			ok = 0
		}
		p.mu.Lock()
		p.rec = append(p.rec, [3]float64{t0, ms, ok})
		p.mu.Unlock()
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (p *prober) summarize(t0, t1 float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var idle, during, after []float64
	fails := 0
	var sb strings.Builder
	for _, x := range p.rec {
		fmt.Fprintf(&sb, "%.4f\t%.3f\t%.0f\n", x[0], x[1], x[2])
		if x[2] == 0 {
			fails++
			continue
		}
		switch {
		case x[0] < t0:
			idle = append(idle, x[1])
		case x[0] <= t1:
			during = append(during, x[1])
		default:
			after = append(after, x[1])
		}
	}
	os.WriteFile(p.r.path("probe.tsv"), []byte(sb.String()), 0600)
	p.r.sum["search_idle_ms"] = pcts(idle)
	p.r.sum["search_during_ms"] = pcts(during)
	p.r.sum["search_after_ms"] = pcts(after)
	p.r.sum["search_fails"] = fails
}

// churner opens and closes subscriptions while the upload runs.
type churner struct {
	r    *run
	mu   sync.Mutex
	lat  []float64 // ms from subscribe to the subscription's first result
	ok   int
	fail map[string]int
}

func (c *churner) note(ms float64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		if c.fail == nil {
			c.fail = map[string]int{}
		}
		s := err.Error()
		if len(s) > 60 {
			s = s[:60]
		}
		c.fail[s]++
		return
	}
	c.ok++
	c.lat = append(c.lat, ms)
}

// waitTag reads until a result message with tag, for at most 10 s.
func waitTag(w *wsClient, tag string) error {
	w.c.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer w.c.SetReadDeadline(time.Time{})
	for {
		p, _, err := w.next(false)
		if err != nil {
			return err
		}
		if msgTag(p) == tag {
			return nil
		}
	}
}

// connLoop: connect, subscribe, first result, close; at most rate per second.
func (c *churner) connLoop(rate float64, stop <-chan struct{}) {
	t := time.NewTicker(time.Duration(float64(time.Second) / rate))
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-c.r.abortc:
			return
		case <-t.C:
		}
		t0 := time.Now()
		w, err := wsDial(hostOf(c.r.base), c.r.searchRoot+"camli/search/ws", 10*time.Second)
		if err != nil {
			c.note(0, err)
			continue
		}
		if err = w.subscribe("c", uiQuery); err == nil {
			err = waitTag(w, "c")
		}
		c.note(1000*time.Since(t0).Seconds(), err)
		w.c.Close()
	}
}

// subLoop: on one connection, subscribe a new tag, wait for its first
// result, unsubscribe it; at most rate per second.
func (c *churner) subLoop(rate float64, stop <-chan struct{}) {
	w, err := wsDial(hostOf(c.r.base), c.r.searchRoot+"camli/search/ws", 10*time.Second)
	if err != nil {
		c.note(0, err)
		return
	}
	defer w.c.Close()
	t := time.NewTicker(time.Duration(float64(time.Second) / rate))
	defer t.Stop()
	for i := 0; ; i++ {
		select {
		case <-stop:
			return
		case <-c.r.abortc:
			return
		case <-t.C:
		}
		tag := "s" + strconv.Itoa(i)
		t0 := time.Now()
		if err = w.subscribe(tag, uiQuery); err == nil {
			err = waitTag(w, tag)
		}
		c.note(1000*time.Since(t0).Seconds(), err)
		if err == nil {
			err = w.unsubscribe(tag)
		}
		if err != nil {
			return
		}
	}
}

func (c *churner) summarize(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.r.sum[name+"_ok"] = c.ok
	c.r.sum[name+"_fail"] = c.fail
	c.r.sum[name+"_first_result_ms"] = pcts(c.lat)
}

// sampler records perkeepd's goroutines, RSS, CPU time, fds, and heap.
type sampler struct {
	r    *run
	mu   sync.Mutex
	rows [][7]float64 // t, goroutines, rss kB, cpu s, fds, heapalloc MB, phase index
}

var phases = []string{"start", "setup", "noclients", "connect", "idle", "upload", "settle", "converge", "slowwait", "disconnect"}

func (s *sampler) loop(stop <-chan struct{}) {
	t := time.NewTicker(*flagSample)
	defer t.Stop()
	for i := 0; ; i++ {
		select {
		case <-stop:
			return
		case <-s.r.abortc:
			return
		case <-t.C:
		}
		heap := -1.0
		if i%10 == 0 {
			heap = s.heap()
		}
		rss, cpu, fds := procStat(int(s.r.srvPid.Load()))
		ph, _ := s.r.phase.Load().(string)
		row := [7]float64{s.r.now(), float64(s.goroutines()), rss, cpu, fds, heap, float64(slices.Index(phases, ph))}
		s.mu.Lock()
		s.rows = append(s.rows, row)
		s.mu.Unlock()
	}
}

func (s *sampler) goroutines() int {
	b, err := httpGet(s.r.base+"/debug/pprof/goroutine?debug=1", 10*time.Second)
	if err != nil {
		return -1
	}
	l, _, _ := strings.Cut(string(b), "\n")
	if i := strings.Index(l, "total "); i >= 0 {
		n, _ := strconv.Atoi(strings.TrimSpace(l[i+6:]))
		return n
	}
	return -1
}

func (s *sampler) heap() float64 {
	b, err := httpGet(s.r.base+"/debug/pprof/heap?debug=1", 10*time.Second)
	if err != nil {
		return -1
	}
	sc := bufio.NewScanner(strings.NewReader(string(b)))
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), "# HeapAlloc = "); ok {
			n, _ := strconv.ParseFloat(v, 64)
			return round2(n / (1 << 20))
		}
	}
	return -1
}

func (s *sampler) cpu() float64 {
	_, c, _ := procStat(int(s.r.srvPid.Load()))
	return c
}

func (s *sampler) rss() float64 {
	r, _, _ := procStat(int(s.r.srvPid.Load()))
	return r
}

// procStat returns RSS in kB, CPU time in seconds, and open fds (-1 when
// unknown) of pid.
func procStat(pid int) (rss, cpu, fds float64) {
	if runtime.GOOS == "linux" {
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if err != nil {
			return -1, -1, -1
		}
		s := string(b)
		f := strings.Fields(s[strings.LastIndexByte(s, ')')+2:]) // fields from 3 on
		ut, _ := strconv.ParseFloat(f[11], 64)
		st, _ := strconv.ParseFloat(f[12], 64)
		rp, _ := strconv.ParseFloat(f[21], 64)
		ents, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
		fds = -1
		if err == nil {
			fds = float64(len(ents))
		}
		return rp * float64(os.Getpagesize()) / 1024, (ut + st) / 100, fds // USER_HZ is 100 on Linux
	}
	out, err := exec.Command("ps", "-o", "rss=,time=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return -1, -1, -1
	}
	f := strings.Fields(string(out))
	if len(f) < 2 {
		return -1, -1, -1
	}
	rss, _ = strconv.ParseFloat(f[0], 64)
	for _, part := range strings.Split(f[1], ":") {
		v, _ := strconv.ParseFloat(part, 64)
		cpu = cpu*60 + v
	}
	return rss, cpu, -1
}

func (s *sampler) summarize(t0, t1 float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var sb strings.Builder
	sb.WriteString("t\tgoroutines\trss_kb\tcpu_s\tfds\theap_mb\tphase\n")
	peakG, peakR := 0.0, 0.0
	var fd0, fd1 float64 = -1, -1
	var lr []([2]float64)
	var lg, lh []([2]float64)
	for _, x := range s.rows {
		fmt.Fprintf(&sb, "%.2f\t%.0f\t%.0f\t%.2f\t%.0f\t%.2f\t%s\n", x[0], x[1], x[2], x[3], x[4], x[5], phases[max(0, int(x[6]))])
		if x[0] >= t0 && x[0] <= t1 {
			peakG = max(peakG, x[1])
			peakR = max(peakR, x[2])
			lr = append(lr, [2]float64{x[0], x[2]})
			lg = append(lg, [2]float64{x[0], x[1]})
			if x[5] >= 0 {
				lh = append(lh, [2]float64{x[0], x[5]})
			}
			if fd0 < 0 {
				fd0 = x[4]
			}
			fd1 = x[4]
		}
	}
	os.WriteFile(s.r.path("samples.tsv"), []byte(sb.String()), 0600)
	s.r.sum["goroutines_peak_upload"] = peakG
	s.r.sum["rss_peak_upload_kb"] = peakR
	if fd0 >= 0 {
		s.r.sum["fds_upload_first"] = fd0
		s.r.sum["fds_upload_last"] = fd1
	}
	if *flagLongRun > 0 {
		// Growth over the second half of the load, per hour.
		half := func(v [][2]float64) [][2]float64 { return v[len(v)/2:] }
		s.r.sum["longrun_rss_kb_per_h"] = round2(slope(half(lr)) * 3600)
		s.r.sum["longrun_goroutines_per_h"] = round2(slope(half(lg)) * 3600)
		s.r.sum["longrun_heap_mb_per_h"] = round2(slope(half(lh)) * 3600)
	}
}

// slope is the least-squares slope of y over x.
func slope(v [][2]float64) float64 {
	if len(v) < 3 {
		return 0
	}
	var sx, sy, sxx, sxy float64
	for _, p := range v {
		sx += p[0]
		sy += p[1]
		sxx += p[0] * p[0]
		sxy += p[0] * p[1]
	}
	n := float64(len(v))
	d := n*sxx - sx*sx
	if d == 0 {
		return 0
	}
	return (n*sxy - sx*sy) / d
}
