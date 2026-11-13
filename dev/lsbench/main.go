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

// lsbench runs one benchmark run of Perkeep's live search against a fresh
// perkeepd with the default generated config: subscribed websocket clients
// (fast, slow, churning), a web-UI-style search probe, a timing proxy in
// front of the uploads, a resource sampler, and a watchdog that aborts a run
// that stops making progress and marks it INVALID instead of hanging.
// The suite around it is described in README.md.
//
//	lsbench -bin <dir with perkeepd, pk-put> -work <dir> -label <name> [flags]
//	lsbench -gen <dir> -n 1000 -size 4096     # small files, mtimes ascending
//
// One run writes <work>/<label>/: summary.json (one JSON object), summary.txt,
// and the raw series (*.tsv). Exit status: 0 valid run, 3 INVALID (watchdog),
// 1 harness failure.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"math"
	"math/rand/v2"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// The web UI's default query: newest 50 permanodes, described.
const uiQuery = `{"sort":"-created","constraint":{"permanode":{"skipHidden":true}},"limit":50,"describe":{"depth":1,"rules":[{"attrs":["camliContent","camliContentImage"]}]}}`

const countQuery = `{"constraint":{"permanode":{"skipHidden":true}},"limit":-1}`

type envList []string

func (e *envList) String() string     { return strings.Join(*e, " ") }
func (e *envList) Set(s string) error { *e = append(*e, s); return nil }

var (
	flagBin      = flag.String("bin", "", "directory with perkeepd and pk-put")
	flagWork     = flag.String("work", "", "work directory; the run goes to <work>/<label>")
	flagLabel    = flag.String("label", "", "run label")
	flagPort     = flag.Int("port", 3291, "perkeepd port; the proxy uses port+1")
	flagFiles    = flag.String("files", "", "directory to upload with pk-put file --filenodes")
	flagClients  = flag.Int("clients", 1, "subscribed websocket clients that read as fast as they can; client 0 parses every result (the tracer)")
	flagSlow     = flag.Int("slow", 0, "slow readers: subscribed clients that read one message per -slowdelay")
	flagSlowD    = flag.Duration("slowdelay", 200*time.Millisecond, "a slow reader's pause after each message")
	flagChurn    = flag.Float64("churn", 0, "connect, subscribe, wait for the first result, close: cycles per second (0: off)")
	flagSubChurn = flag.Float64("subchurn", 0, "subscribe, wait for the result, unsubscribe on one connection: cycles per second (0: off)")
	flagIdle     = flag.Duration("idle", 3*time.Second, "idle time with the clients connected, before the upload")
	flagProxy    = flag.Bool("proxy", true, "send pk-put through a timing proxy")
	flagProbe    = flag.Bool("probe", true, "run the search probe (UI query every 50 ms)")
	flagGiveup   = flag.Duration("giveup", 60*time.Second, "watchdog: no pk-put progress, or a search outstanding, this long makes the run INVALID")
	flagDeadline = flag.Duration("deadline", 30*time.Minute, "watchdog: the whole run")
	flagRestart  = flag.Bool("restart", false, "upload first with no clients, stop, delete the index, start again with the clients connected: the index catches up through the sync only")
	flagLongRun  = flag.Duration("longrun", 0, "instead of one upload: upload a batch of new small files every -every for this long")
	flagBatch    = flag.Int("batch", 10, "long run: files per batch")
	flagEvery    = flag.Duration("every", 5*time.Second, "long run: batch period")
	flagSample   = flag.Duration("sample", time.Second, "resource sampling period")
	flagConverge = flag.Duration("converge", 20*time.Second, "how long to wait for the tracer to show the final result")
	flagMsgCap   = flag.Int("msgcap", 0, "INVALID once the tracer has this many result messages (0: no cap); keeps a slow reader's 256-message queue from ever filling on build P")
	flagSlowWait = flag.Duration("slowwait", 0, "after that, how long the slow readers may keep reading at their pace until they show the final result (or are closed)")
	flagKeep     = flag.Bool("keep", false, "keep the server's var and cache directories")
	flagEnv      envList

	flagGen  = flag.String("gen", "", "generate small files into this directory and exit")
	flagN    = flag.Int("n", 1000, "-gen: number of files")
	flagSize = flag.Int("size", 4096, "-gen: bytes per file")
)

func main() {
	flag.Var(&flagEnv, "env", "K=V for perkeepd (repeatable)")
	flag.Parse()
	if *flagGen != "" {
		if err := genSmall(*flagGen, *flagN, *flagSize, time.Now().Add(-time.Duration(*flagN)*time.Second)); err != nil {
			log.Fatal(err)
		}
		return
	}
	if *flagBin == "" || *flagWork == "" || *flagLabel == "" || (*flagFiles == "" && *flagLongRun == 0 && *flagIdle == 0) {
		log.Fatal("need -bin, -work, -label, and -files or -longrun")
	}
	r := newRun()
	code := r.main()
	os.Exit(code)
}

// genSmall writes n files of size bytes with unique contents and mtimes one
// second apart from base, so each new file sorts first in the UI's query.
func genSmall(dir string, n, size int, base time.Time) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	rng := rand.New(rand.NewPCG(uint64(base.UnixNano()), 3))
	b := make([]byte, size)
	for i := range n {
		for j := range b {
			b[j] = 'a' + byte(rng.IntN(26))
		}
		copy(b, fmt.Sprintf("file %d of %d\n", i, n))
		name := filepath.Join(dir, fmt.Sprintf("f%06d.txt", i))
		if err := os.WriteFile(name, b, 0600); err != nil {
			return err
		}
		mt := base.Add(time.Duration(i) * time.Second)
		if err := os.Chtimes(name, mt, mt); err != nil {
			return err
		}
	}
	return nil
}

// run is one benchmark run. Times are seconds since t0.
type run struct {
	dir        string
	t0         time.Time
	base       string // perkeepd, direct
	putBase    string // what pk-put talks to (the proxy, or base)
	searchRoot string
	srv        *exec.Cmd    // owned by the main goroutine
	srvPid     atomic.Int64 // srv's pid, for the other goroutines (it changes on a restart)
	srvDone    chan struct{}

	abortc   chan struct{} // closed by fail
	failOnce sync.Once
	reason   string // why the run is INVALID; set before abortc closes

	phase    atomic.Value // string
	lastPut  atomic.Int64 // ns since t0 of the last pk-put line
	probeOut atomic.Int64 // ns since t0 at which the outstanding probe started; 0 if none
	putCmd   atomic.Pointer[exec.Cmd]

	acksMu  sync.Mutex
	acks    map[string]float64    // ref printed by pk-put -> time printed
	blobReq map[string][2]float64 // ref -> its upload request's start and end (proxy)
	lines   int

	sum map[string]any
}

func newRun() *run {
	r := &run{
		dir:     filepath.Join(*flagWork, *flagLabel),
		t0:      time.Now(),
		abortc:  make(chan struct{}),
		acks:    make(map[string]float64),
		blobReq: make(map[string][2]float64),
		sum:     make(map[string]any),
	}
	r.phase.Store("start")
	return r
}

func (r *run) now() float64 { return time.Since(r.t0).Seconds() }

func (r *run) path(name string) string { return filepath.Join(r.dir, name) }

// fail marks the run INVALID, saves a goroutine dump, and kills pk-put and
// perkeepd so that nothing waits on them.
func (r *run) fail(reason string) {
	r.failOnce.Do(func() {
		r.reason = reason
		log.Printf("INVALID: %s", reason)
		if b, err := httpGet(r.base+"/debug/pprof/goroutine?debug=2", 10*time.Second); err == nil {
			os.WriteFile(r.path("invalid-goroutines.txt"), b, 0600)
		}
		if c := r.putCmd.Load(); c != nil && c.Process != nil {
			c.Process.Kill()
		}
		if pid := r.srvPid.Load(); pid > 0 {
			syscall.Kill(int(pid), syscall.SIGKILL)
		}
		close(r.abortc)
	})
}

func (r *run) aborted() bool {
	select {
	case <-r.abortc:
		return true
	default:
		return false
	}
}

// sleep waits d, or until the run is aborted; it reports whether it slept.
func (r *run) sleep(d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-r.abortc:
		return false
	}
}

func (r *run) main() int {
	os.RemoveAll(r.dir)
	if err := os.MkdirAll(r.dir, 0700); err != nil {
		log.Print(err)
		return 1
	}
	logf, err := os.Create(r.path("lsbench.log"))
	if err != nil {
		log.Print(err)
		return 1
	}
	log.SetOutput(io.MultiWriter(os.Stderr, logf))
	log.SetFlags(log.Lmicroseconds)
	syscall.Umask(077) // perkeepd's secret store refuses a group-writable var dir

	r.sum["label"] = *flagLabel
	r.sum["bin"] = *flagBin
	r.sum["args"] = strings.Join(os.Args[1:], " ")
	r.sum["host"], _ = os.Hostname()
	r.sum["goos"] = runtime.GOOS
	r.sum["start"] = r.t0.UTC().Format(time.RFC3339)
	steal0 := readSteal()

	go r.watchdog()
	err = r.scenario()
	if err != nil && !r.aborted() {
		log.Printf("harness failure: %v", err)
		r.sum["harness_error"] = err.Error()
	}
	r.stopServer()
	if s1 := readSteal(); steal0 != nil && s1 != nil {
		r.sum["cpu_steal_pct"] = round2(100 * float64(s1[0]-steal0[0]) / float64(max(1, s1[1]-steal0[1])))
	}
	r.scanServerLog()
	if !*flagKeep {
		os.RemoveAll(r.path("var"))
		os.RemoveAll(r.path("cache"))
	}
	code := 0
	r.sum["valid"] = true
	switch {
	case r.aborted():
		r.sum["valid"] = false
		r.sum["invalid_reason"] = r.reason
		code = 3
	case err != nil:
		r.sum["valid"] = false
		code = 1
	}
	r.sum["wall_s"] = round2(r.now())
	b, _ := json.Marshal(r.sum)
	os.WriteFile(r.path("summary.json"), append(b, '\n'), 0600)
	txt := summaryText(r.sum)
	os.WriteFile(r.path("summary.txt"), []byte(txt), 0600)
	fmt.Print(txt)
	return code
}

func (r *run) watchdog() {
	deadline := time.After(*flagDeadline)
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-r.abortc:
			return
		case <-deadline:
			r.fail(fmt.Sprintf("the run took longer than %v", *flagDeadline))
			return
		case <-t.C:
		}
		now := time.Since(r.t0)
		if r.phase.Load() == "upload" {
			if last := time.Duration(r.lastPut.Load()); now-last > *flagGiveup {
				r.fail(fmt.Sprintf("pk-put printed nothing for %v (phase upload, %.1f s)", *flagGiveup, now.Seconds()))
				return
			}
		}
		if p := r.probeOut.Load(); p != 0 && now-time.Duration(p) > *flagGiveup {
			r.fail(fmt.Sprintf("a search was outstanding for %v (phase %v)", *flagGiveup, r.phase.Load()))
			return
		}
	}
}

func (r *run) setPhase(p string) {
	log.Printf("phase %s at %.2f s", p, r.now())
	r.phase.Store(p)
	r.sum["t_"+p] = round2(r.now())
}

// scenario is the body of a run.
func (r *run) scenario() error {
	if err := r.startServer(); err != nil {
		return err
	}
	if err := r.initPut(); err != nil {
		return err
	}
	nfiles, nbytes := 0, int64(0)
	if *flagFiles != "" {
		ents, err := os.ReadDir(*flagFiles)
		if err != nil {
			return err
		}
		for _, e := range ents {
			if fi, err := e.Info(); err == nil && fi.Mode().IsRegular() {
				nfiles++
				nbytes += fi.Size()
			}
		}
		r.sum["files"] = nfiles
		r.sum["bytes"] = nbytes
	}
	if *flagRestart {
		if err := r.restartSetup(nfiles); err != nil {
			return err
		}
	}

	stopc := make(chan struct{})
	var wg sync.WaitGroup
	defer func() { // on every path, the clients and samplers end
		select {
		case <-stopc:
		default:
			close(stopc)
		}
		wg.Wait()
	}()
	samp := &sampler{r: r}
	wg.Go(func() { samp.loop(stopc) })
	r.setPhase("noclients")
	if !*flagRestart && !r.sleep(2*time.Second) { // after a restart, connect at once
		return nil
	}
	r.sum["goroutines_noclients"] = samp.goroutines()

	// Subscribed clients.
	r.setPhase("connect")
	clientStop := make(chan struct{})
	var cwg sync.WaitGroup
	tr := &tracer{r: r, firstAny: map[string]float64{}, firstContent: map[string]float64{}}
	var readers []*reader
	for i := range *flagClients + *flagSlow {
		rd := &reader{id: i, tr: nil}
		if i == 0 && *flagClients > 0 {
			rd.tr = tr
		}
		if i >= *flagClients {
			rd.delay = *flagSlowD
		}
		readers = append(readers, rd)
	}
	for _, rd := range readers {
		w, err := wsDial(hostOf(r.base), r.searchRoot+"camli/search/ws", 10*time.Second)
		if err != nil {
			return fmt.Errorf("client %d: %v", rd.id, err)
		}
		rd.w = w
		if err := w.subscribe("q1", uiQuery); err != nil {
			return fmt.Errorf("client %d subscribe: %v", rd.id, err)
		}
		cwg.Go(func() { rd.loop(r, clientStop) })
	}
	// Wait for every client's first result.
	for _, rd := range readers {
		deadline := time.Now().Add(30 * time.Second)
		for rd.count() == 0 && !rd.isClosed() {
			if time.Now().After(deadline) {
				return fmt.Errorf("client %d got no first result in 30 s", rd.id)
			}
			if !r.sleep(20 * time.Millisecond) {
				return nil
			}
		}
	}
	r.sum["t_clients_ready"] = round2(r.now())

	var pr *prober
	if *flagProbe {
		pr = &prober{r: r}
		wg.Go(func() { pr.loop(stopc) })
	}
	r.setPhase("idle")
	cpu0 := samp.cpu()
	if !r.sleep(*flagIdle) {
		return nil
	}
	r.sum["goroutines_idle"] = samp.goroutines()
	if cpu1 := samp.cpu(); cpu0 >= 0 && cpu1 >= 0 && *flagIdle > 0 {
		r.sum["idle_cpu_pct"] = round2(100 * (cpu1 - cpu0) / flagIdle.Seconds())
	}
	r.sum["rss_idle_kb"] = samp.rss()

	churnStop := make(chan struct{})
	var churnWG sync.WaitGroup
	ch := &churner{r: r}
	sc := &churner{r: r}
	if *flagChurn > 0 {
		churnWG.Go(func() { ch.connLoop(*flagChurn, churnStop) })
	}
	if *flagSubChurn > 0 {
		churnWG.Go(func() { sc.subLoop(*flagSubChurn, churnStop) })
	}

	// The load.
	cpuU0 := samp.cpu()
	r.setPhase("upload")
	r.lastPut.Store(int64(time.Since(r.t0)))
	var putErr error
	tUp0 := r.now()
	switch {
	case *flagLongRun > 0:
		nfiles, putErr = r.longRun(stopc)
		r.sum["files"] = nfiles
	case *flagRestart:
		// The load is the sync to the index, already running.
	case *flagFiles != "":
		putErr = r.put(*flagFiles)
	}
	tUp1 := r.now()
	if r.aborted() {
		return nil
	}
	if putErr != nil {
		r.sum["put_error"] = putErr.Error()
		return fmt.Errorf("pk-put failed: %v; see %s", putErr, r.path("pk-put.err"))
	}
	if *flagFiles != "" && !*flagRestart && *flagLongRun == 0 {
		r.sum["put_s"] = round2(tUp1 - tUp0)
		r.sum["put_ok"] = putErr == nil
		if putErr != nil {
			r.sum["put_error"] = putErr.Error()
		}
		r.sum["put_files_per_s"] = round2(float64(nfiles) / (tUp1 - tUp0))
		r.sum["put_mb_per_s"] = round2(float64(nbytes) / (1 << 20) / (tUp1 - tUp0))
	}
	if cpuU1 := samp.cpu(); cpuU0 >= 0 && cpuU1 >= 0 {
		r.sum["upload_cpu_s"] = round2(cpuU1 - cpuU0)
	}
	close(churnStop)
	churnWG.Wait()

	// Until the index has every permanode, then the final result.
	r.setPhase("settle")
	want := nfiles
	tIdx, err := r.waitIndexed(want)
	if err != nil {
		return err
	}
	if *flagRestart {
		r.sum["restart_indexed_s"] = round2(tIdx - r.sum["t_restarted"].(float64))
	} else {
		r.sum["indexed_after_put_s"] = round2(tIdx - tUp1)
	}
	final, err := r.query()
	if err != nil {
		return err
	}
	// Let the live searches catch up, then compare.
	r.setPhase("converge")
	convDeadline := time.Now().Add(*flagConverge)
	for len(readers) > 0 && readers[0].tr != nil && time.Now().Before(convDeadline) {
		if st, _ := tr.lastState(); st.equal(final) {
			break
		}
		if !r.sleep(50 * time.Millisecond) {
			return nil
		}
	}
	if *flagClients > 0 {
		tr.summarize(final, r, r.claimAcks())
	}
	if !r.sleep(3 * time.Second) {
		return nil
	}
	r.sum["goroutines_after"] = samp.goroutines()
	r.sum["rss_after_kb"] = samp.rss()

	// Can the slow readers, still at their pace, catch up with the final
	// result? A queue-everything hub needs them to read their backlog; a
	// latest-result hub needs one message; a dropped client never does.
	if *flagSlow > 0 && *flagSlowWait > 0 {
		r.setPhase("slowwait")
		deadline := time.Now().Add(*flagSlowWait)
		for _, rd := range readers {
			for rd.delay > 0 && !rd.isClosed() && !rd.shows(final) && time.Now().Before(deadline) {
				if !r.sleep(50 * time.Millisecond) {
					return nil
				}
			}
		}
	}

	// The slow readers now read their backlog at full speed, to find
	// whether the server has closed them (a close frame waits behind
	// whatever was queued before it); what they display is judged up to
	// tEnd.
	tEnd := r.now()
	for _, rd := range readers {
		rd.drain.Store(true)
	}
	drainEnd := time.Now().Add(15 * time.Second)
	for _, rd := range readers {
		for rd.delay > 0 && !rd.isClosed() && time.Now().Before(drainEnd) &&
			time.Since(r.t0)-time.Duration(rd.lastAt.Load()) < 2*time.Second {
			if !r.sleep(50 * time.Millisecond) {
				return nil
			}
		}
	}

	// Clients go; goroutines should go back to where they were.
	r.setPhase("disconnect")
	close(clientStop)
	for _, rd := range readers {
		rd.w.c.Close()
	}
	cwg.Wait()
	if !r.sleep(3 * time.Second) {
		return nil
	}
	r.sum["goroutines_end"] = samp.goroutines()
	close(stopc)
	wg.Wait()

	summarizeReaders(r, readers, final, tUp1, tEnd)
	if pr != nil {
		pr.summarize(tUp0, tUp1)
	}
	if *flagChurn > 0 {
		ch.summarize("churn")
	}
	if *flagSubChurn > 0 {
		sc.summarize("subchurn")
	}
	samp.summarize(tUp0, tUp1)
	r.summarizeProxy(tUp0, tUp1)
	r.writeAcks()
	return nil
}

// startServer starts perkeepd on a fresh (or, after a restart, the
// existing) config and var directory, and waits until it answers.
func (r *run) startServer() error {
	if c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", *flagPort), time.Second); err == nil {
		c.Close()
		return fmt.Errorf("port %d is already in use", *flagPort)
	}
	for _, d := range []string{"config", "var", "cache"} {
		os.MkdirAll(r.path(d), 0700)
	}
	lf, err := os.OpenFile(r.path("perkeepd.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	cmd := exec.Command(filepath.Join(*flagBin, "perkeepd"), "-openbrowser=false", fmt.Sprintf("-listen=127.0.0.1:%d", *flagPort))
	cmd.Env = append(os.Environ(), r.serverEnv()...)
	cmd.Env = append(cmd.Env, flagEnv...)
	cmd.Stdout, cmd.Stderr = lf, lf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	r.srv = cmd
	r.srvPid.Store(int64(cmd.Process.Pid))
	r.srvDone = make(chan struct{})
	go func() { cmd.Wait(); lf.Close(); close(r.srvDone) }()
	r.base = fmt.Sprintf("http://127.0.0.1:%d", *flagPort)
	deadline := time.Now().Add(60 * time.Second)
	for {
		b, err := httpGet(r.base+"/?camli.mode=config", 2*time.Second)
		if err == nil {
			var disc struct {
				SearchRoot string `json:"searchRoot"`
			}
			if json.Unmarshal(b, &disc) == nil && disc.SearchRoot != "" {
				r.searchRoot = disc.SearchRoot
				break
			}
		}
		select {
		case <-r.srvDone:
			return fmt.Errorf("perkeepd exited; see %s", r.path("perkeepd.log"))
		case <-time.After(100 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("perkeepd did not answer in 60 s")
		}
	}
	r.sum["pid"] = cmd.Process.Pid
	r.putBase = r.base
	if *flagProxy && r.sum["proxy"] == nil {
		if err := r.startProxy(); err != nil {
			return err
		}
	} else if *flagProxy {
		r.putBase = fmt.Sprintf("http://127.0.0.1:%d", *flagPort+1)
	}
	return nil
}

func (r *run) serverEnv() []string {
	return []string{
		"CAMLI_CONFIG_DIR=" + r.path("config"),
		"CAMLI_VAR_DIR=" + r.path("var"),
		"PERKEEP_CACHE_DIR=" + r.path("cache"),
	}
}

func (r *run) stopServer() {
	if r.srv == nil || r.srv.Process == nil {
		return
	}
	r.srv.Process.Signal(syscall.SIGTERM)
	select {
	case <-r.srvDone:
	case <-time.After(15 * time.Second):
		r.srv.Process.Kill()
		<-r.srvDone
	}
	syscall.Kill(-r.srv.Process.Pid, syscall.SIGKILL) // anything it started
}

func (r *run) initPut() error {
	b, err := os.ReadFile(r.path("config/server-config.json"))
	if err != nil {
		return err
	}
	var cfg struct {
		Identity string `json:"identity"`
	}
	if err := json.Unmarshal(b, &cfg); err != nil || cfg.Identity == "" {
		return fmt.Errorf("no identity in server-config.json: %v", err)
	}
	cmd := exec.Command(filepath.Join(*flagBin, "pk-put"), "init", "-gpgkey", cfg.Identity)
	cmd.Env = append(os.Environ(), r.serverEnv()...)
	cmd.Env = append(cmd.Env, "CAMLI_SERVER="+r.putBase)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("pk-put init: %v: %s", err, out)
	}
	return nil
}

// put runs pk-put file --filenodes on dir, timestamping each line it prints
// (per file: the permanode's ref, then its camliContent claim's).
func (r *run) put(dir string) error {
	cmd := exec.Command(filepath.Join(*flagBin, "pk-put"), "file", "--filenodes", dir)
	cmd.Env = append(os.Environ(), r.serverEnv()...)
	cmd.Env = append(cmd.Env, "CAMLI_SERVER="+r.putBase)
	ef, err := os.OpenFile(r.path("pk-put.err"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer ef.Close()
	cmd.Stderr = ef
	out, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	r.putCmd.Store(cmd)
	sc := bufio.NewScanner(out)
	for sc.Scan() {
		t := r.now()
		r.lastPut.Store(int64(time.Since(r.t0)))
		ref := strings.TrimSpace(sc.Text())
		r.acksMu.Lock()
		if _, ok := r.acks[ref]; !ok {
			r.acks[ref] = t
		}
		r.lines++
		r.acksMu.Unlock()
	}
	err = cmd.Wait()
	r.putCmd.Store(nil)
	return err
}

// longRun uploads a batch of new small files every -every for -longrun,
// and returns how many files it uploaded.
func (r *run) longRun(stop <-chan struct{}) (int, error) {
	end := time.Now().Add(*flagLongRun)
	t := time.NewTicker(*flagEvery)
	defer t.Stop()
	i := 0
	defer func() { r.sum["longrun_batches"] = i }()
	for time.Now().Before(end) {
		d := r.path(fmt.Sprintf("lr/b%05d", i))
		if err := genSmall(d, *flagBatch, *flagSize, time.Now()); err != nil {
			return i * *flagBatch, err
		}
		if err := r.put(d); err != nil {
			return i * *flagBatch, err
		}
		os.RemoveAll(d)
		i++
		select {
		case <-t.C:
		case <-r.abortc:
			return i * *flagBatch, nil
		case <-stop:
			return i * *flagBatch, nil
		}
	}
	return i * *flagBatch, nil
}

// restartSetup uploads -files with no client, waits until it is indexed,
// stops the server, deletes the index, and starts the server again with
// the sync's validation on. The index then gets every blob from the sync.
func (r *run) restartSetup(nfiles int) error {
	r.setPhase("setup")
	if err := r.put(*flagFiles); err != nil {
		return fmt.Errorf("setup upload: %v", err)
	}
	if _, err := r.waitIndexed(nfiles); err != nil {
		return err
	}
	r.stopServer()
	ms, _ := filepath.Glob(r.path("var/index.sqlite*"))
	if len(ms) == 0 {
		return fmt.Errorf("no var/index.sqlite* to delete")
	}
	for _, m := range ms {
		os.Remove(m)
	}
	r.sum["t_restarted"] = round2(r.now())
	// The sync's validation finds the blobs the index lacks and sends
	// them to it: every blob then reaches the index only through the sync.
	flagEnv = append(flagEnv, "CAMLI_SYNC_VALIDATE=1")
	return r.startServer()
}

// waitIndexed polls until a search finds want permanodes. No growth for
// -giveup makes the run INVALID.
func (r *run) waitIndexed(want int) (float64, error) {
	last, lastChange := -1, time.Now()
	for {
		n, err := r.count()
		if err == nil && n >= want {
			return r.now(), nil
		}
		if n != last {
			last, lastChange = n, time.Now()
		}
		if time.Since(lastChange) > *flagGiveup {
			r.fail(fmt.Sprintf("the index stayed at %d of %d permanodes for %v", last, want, *flagGiveup))
			return 0, nil
		}
		if !r.sleep(200 * time.Millisecond) {
			return 0, nil
		}
	}
}

func (r *run) searchURL() string { return r.base + r.searchRoot + "camli/search/query" }

func (r *run) count() (int, error) {
	b, err := httpPost(r.searchURL(), countQuery, 30*time.Second)
	if err != nil {
		return -1, err
	}
	var res searchResult
	if err := json.Unmarshal(b, &res); err != nil {
		return -1, err
	}
	return len(res.Blobs), nil
}

func (r *run) query() (state, error) {
	b, err := httpPost(r.searchURL(), uiQuery, 30*time.Second)
	if err != nil {
		return state{}, err
	}
	var res searchResult
	if err := json.Unmarshal(b, &res); err != nil {
		return state{}, err
	}
	return res.state(), nil
}

func (r *run) scanServerLog() {
	b, _ := os.ReadFile(r.path("perkeepd.log"))
	s := string(b)
	r.sum["log_ws_drops"] = strings.Count(s, "dropping a websocket client")
	r.sum["log_lock_waits"] = strings.Count(s, "Lock has waited")
	r.sum["log_panics"] = strings.Count(s, "panic:")
}

// claimAcks maps each permanode to its camliContent claim's times: the
// claim's upload request arriving at the proxy, its response, and pk-put
// printing the claim's ref. It finds the claims by fetching every blob
// pk-put printed.
func (r *run) claimAcks() map[string][3]float64 {
	r.acksMu.Lock()
	acks := maps.Clone(r.acks)
	req := maps.Clone(r.blobReq)
	r.acksMu.Unlock()
	m := make(map[string][3]float64)
	for ref, t := range acks {
		b, err := httpGet(r.base+"/bs/camli/"+ref, 10*time.Second)
		if err != nil {
			continue
		}
		var cl struct {
			CamliType string `json:"camliType"`
			Attribute string `json:"attribute"`
			PermaNode string `json:"permaNode"`
		}
		if json.Unmarshal(b, &cl) == nil && cl.CamliType == "claim" && cl.Attribute == "camliContent" {
			q, ok := req[ref]
			if !ok {
				q = [2]float64{-1, -1}
			}
			m[cl.PermaNode] = [3]float64{q[0], q[1], t}
		}
	}
	r.sum["claims_found"] = len(m)
	return m
}

func (r *run) writeAcks() {
	r.acksMu.Lock()
	defer r.acksMu.Unlock()
	var sb strings.Builder
	for ref, t := range r.acks {
		fmt.Fprintf(&sb, "%.4f\t%s\n", t, ref)
	}
	os.WriteFile(r.path("acks.tsv"), []byte(sb.String()), 0600)
	r.sum["put_lines"] = r.lines
}

// searchResult is the part of a search result the harness reads.
type searchResult struct {
	Blobs []struct {
		Blob string `json:"blob"`
	} `json:"blobs"`
	Description *struct {
		Meta map[string]struct {
			Permanode *struct {
				Attr map[string][]string `json:"attr"`
			} `json:"permanode"`
		} `json:"meta"`
	} `json:"description"`
}

// state is what a UI tab shows: the ordered permanodes, and which of them
// have their content.
type state struct {
	refs    []string
	content []bool
}

func (res *searchResult) state() state {
	var s state
	for _, b := range res.Blobs {
		s.refs = append(s.refs, b.Blob)
		has := false
		if res.Description != nil {
			if m, ok := res.Description.Meta[b.Blob]; ok && m.Permanode != nil && len(m.Permanode.Attr["camliContent"]) > 0 {
				has = true
			}
		}
		s.content = append(s.content, has)
	}
	return s
}

func (s state) equal(o state) bool {
	return slices.Equal(s.refs, o.refs) && slices.Equal(s.content, o.content)
}

func httpGet(u string, timeout time.Duration) ([]byte, error) {
	c := &http.Client{Timeout: timeout}
	res, err := c.Get(u)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		return b, fmt.Errorf("%s: %s", u, res.Status)
	}
	return b, err
}

var postClient = &http.Client{}

func httpPost(u, body string, timeout time.Duration) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", u, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res, err := postClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		return b, fmt.Errorf("%s: %s", u, res.Status)
	}
	return b, err
}

func hostOf(base string) string {
	u, _ := url.Parse(base)
	return u.Host
}

// startProxy puts a reverse proxy on port+1 that records every request's
// latency (from the request arriving to the response being written).
func (r *run) startProxy() error {
	target, _ := url.Parse(r.base)
	rp := httputil.NewSingleHostReverseProxy(target)
	dir := rp.Director
	rp.Director = func(req *http.Request) {
		dir(req)
		// perkeepd's localhost auth on Linux looks up the owner of
		// the TCP connection named by RemoteAddr and Host: Host must
		// be the server's address, as on the proxied connection.
		req.Host = target.Host
	}
	rp.Transport = &http.Transport{MaxIdleConnsPerHost: 64, DisableCompression: true}
	f, err := os.Create(r.path("proxy.tsv"))
	if err != nil {
		return err
	}
	var mu sync.Mutex
	w := bufio.NewWriter(f)
	h := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		t0 := r.now()
		var refs []string
		if req.Method == "POST" && strings.HasSuffix(req.URL.Path, "/camli/upload") {
			// Which blobs this upload carries: the multipart field names.
			body, err := io.ReadAll(req.Body)
			if err != nil {
				http.Error(rw, err.Error(), 500)
				return
			}
			req.Body = io.NopCloser(bytes.NewReader(body))
			if _, ps, err := mime.ParseMediaType(req.Header.Get("Content-Type")); err == nil {
				mr := multipart.NewReader(bytes.NewReader(body), ps["boundary"])
				for {
					part, err := mr.NextPart()
					if err != nil {
						break
					}
					refs = append(refs, part.FormName())
				}
			}
		}
		cw := &countingWriter{ResponseWriter: rw, status: 200}
		rp.ServeHTTP(cw, req)
		t1 := r.now()
		d := t1 - t0
		if len(refs) > 0 {
			r.acksMu.Lock()
			for _, ref := range refs {
				r.blobReq[ref] = [2]float64{t0, t1}
			}
			r.acksMu.Unlock()
		}
		p := req.URL.Path
		if i := strings.Index(p, "/camli/"); i >= 0 {
			p = p[i:]
		}
		if len(p) > 40 {
			p = p[:40]
		}
		mu.Lock()
		fmt.Fprintf(w, "%.4f\t%.3f\t%s\t%s\t%d\t%d\n", t0, d*1000, req.Method, p, cw.status, req.ContentLength)
		w.Flush()
		mu.Unlock()
	})
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *flagPort+1))
	if err != nil {
		return err
	}
	go http.Serve(ln, h)
	r.putBase = fmt.Sprintf("http://127.0.0.1:%d", *flagPort+1)
	r.sum["proxy"] = true
	return nil
}

type countingWriter struct {
	http.ResponseWriter
	status int
}

func (c *countingWriter) WriteHeader(s int) { c.status = s; c.ResponseWriter.WriteHeader(s) }

func (r *run) summarizeProxy(t0, t1 float64) {
	b, err := os.ReadFile(r.path("proxy.tsv"))
	if err != nil {
		return
	}
	var post []float64
	n5xx := 0
	for _, l := range strings.Split(string(b), "\n") {
		f := strings.Split(l, "\t")
		if len(f) < 6 {
			continue
		}
		t, _ := strconv.ParseFloat(f[0], 64)
		ms, _ := strconv.ParseFloat(f[1], 64)
		if t < t0 || t > t1 {
			continue
		}
		if f[2] == "POST" || f[2] == "PUT" {
			post = append(post, ms)
		}
		if strings.HasPrefix(f[4], "5") {
			n5xx++
		}
	}
	r.sum["upload_req_n"] = len(post)
	r.sum["upload_req_ms"] = pcts(post)
	r.sum["upload_req_5xx"] = n5xx
}

// pcts returns n, p50, p90, p99, max (nearest rank), rounded.
func pcts(v []float64) map[string]float64 {
	if len(v) == 0 {
		return map[string]float64{"n": 0}
	}
	s := slices.Clone(v)
	slices.Sort(s)
	p := func(q float64) float64 { return round2(s[min(len(s)-1, int(q*float64(len(s))))]) }
	return map[string]float64{"n": float64(len(s)), "p50": p(0.5), "p90": p(0.9), "p99": p(0.99), "max": round2(s[len(s)-1])}
}

func round2(f float64) float64 { return math.Round(f*100) / 100 }

func summaryText(m map[string]any) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var sb strings.Builder
	fmt.Fprintf(&sb, "== %v valid=%v\n", m["label"], m["valid"])
	for _, k := range keys {
		v, _ := json.Marshal(m[k])
		fmt.Fprintf(&sb, "   %s: %s\n", k, v)
	}
	return sb.String()
}

// readSteal returns the steal and total jiffies from /proc/stat (Linux).
func readSteal() []int64 {
	b, err := os.ReadFile("/proc/stat")
	if err != nil {
		return nil
	}
	l, _, _ := bytes.Cut(b, []byte("\n"))
	f := strings.Fields(string(l))
	if len(f) < 9 || f[0] != "cpu" {
		return nil
	}
	var tot int64
	for _, x := range f[1:] {
		v, _ := strconv.ParseInt(x, 10, 64)
		tot += v
	}
	st, _ := strconv.ParseInt(f[8], 10, 64)
	return []int64{st, tot}
}

var errStopped = errors.New("stopped")
