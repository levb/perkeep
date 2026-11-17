# lsbench: the live-search benchmark suite

Optional tooling: a separate Go module (standard library only), so it adds
nothing to perkeepd or to `go build ./...` and `go test ./...` at the
repository root. It measures the web UI's live search (websocket
subscriptions that are sent new results while blobs are uploaded) end to end
against unmodified binaries of four builds:

| build | what | revision |
|---|---|---|
| P | before live search broke (Oct 2026) | e69259ad9 |
| M | master: no live updates | 0e6925347 |
| A | master with live search restored the old way | 1fd04a4a7 |
| L | the change under test | default: the newest commit on HEAD outside dev/lsbench |

## Files

- `main.go`, `ws.go`, `mon.go`: `lsbench`, one run. A fresh perkeepd with
  the default generated config, subscribed websocket clients (fast, slow,
  churning), a search probe, a timing proxy in front of `pk-put`, a resource
  sampler, and a watchdog that marks a stuck run INVALID (exit 3) instead of
  waiting. `lsbench --gen DIR --n 1000 --size 4096` makes small files;
  `lsbench --help` lists the flags.
- `genphotos/`: noise JPEGs as a stand-in for photos.
- `lsbench_index_test.go.txt`: a Go benchmark of the index's write path
  with live search attached (scenario W). mktrees.sh installs it in every
  tree as `pkg/search/lsbench_index_test.go`; it compiles on all four.
- `mktrees.sh`: makes the four trees from commits with `git archive`, and
  packs them and this directory (as of HEAD) for the benchmark machine.
- `vm-bench.sh`: the whole suite, one run at a time, resumable, under
  nohup. `SMOKE=1` runs every scenario once at a tiny size.
- `check-smoke.py`: the pass/fail checks of a `SMOKE=1` pass.
- `report.py`: tables of median [min - max] per build over valid runs.

## Running it

On the machine with the clone (read-only use of the clone; commit any
change to this directory first, as the harness ships from HEAD):

    bash dev/lsbench/mktrees.sh /tmp/lsship    # optional 2nd arg: L's revision

On the benchmark machine (Linux; vm-bench.sh uses `timeout` and /proc),
with the tarballs in `~/perkeep-bench/ls`:

    cd ~/perkeep-bench/ls
    for b in P M A L; do rm -rf trees/$b bin/$b; mkdir -p trees/$b; tar -xzf tree-$b.tgz -C trees/$b; done
    rm -rf h && mkdir h && tar -xzf lsbench.tgz -C h
    SMOKE=1 nohup bash h/dev/lsbench/vm-bench.sh >/dev/null 2>&1 </dev/null &
    # when out/ALL.done exists:
    python3 h/dev/lsbench/check-smoke.py out
    mv out out-smoke
    nohup bash h/dev/lsbench/vm-bench.sh >/dev/null 2>&1 </dev/null &

Progress is in `out/progress.log`; `out/ALL.done` marks the end. Then
`python3 dev/lsbench/report.py <copy of out>` prints the tables.
`LSBASE` and `LSROOT` move the layout elsewhere, for a smoke pass on a Mac.
