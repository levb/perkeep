#!/bin/bash
# vm-bench.sh runs the live-search benchmark suite (README.md) on the Linux
# VM, one run at a time, resumable: a run whose summary.json exists is
# skipped. Layout, all under ~/perkeep-bench/ls (never outside):
#
#	trees/{P,M,A,L}       scratch trees from mktrees.sh's tarballs
#	bin/{P,M,A,L}         perkeepd, pk-put, pk per build; bin/lsbench, bin/genphotos
#	data/                 the files to upload (generated once)
#	out/<scenario>/<build>-r<round>/   one run (summary.json and raw series)
#	out/W/<build>.txt     the Go benchmark of the index's write path
#	h/dev/lsbench/        the harness (lsbench.tgz); this script is h/dev/lsbench/vm-bench.sh
#
#   nohup bash ~/perkeep-bench/ls/h/dev/lsbench/vm-bench.sh [scenario ...] >/dev/null 2>&1 &
#
# With no arguments it runs every scenario in priority order. Progress:
# out/progress.log; out/ALL.done at the end. SMOKE=1: one round, 200 files.
set -u
here=$(cd "$(dirname "$0")" && pwd) # the harness's own directory
B=${LSBASE:-$HOME/perkeep-bench} # LSBASE: run elsewhere (e.g. a Mac smoke test)
L=${LSROOT:-$B/ls}
if [ -d "$B/go/bin" ]; then # the VM's Go and caches
	export PATH="$B/go/bin:$PATH"
	export GOPATH="$B/gopath" GOMODCACHE="$B/gopath/pkg/mod" GOCACHE="$B/gocache"
	export GOTOOLCHAIN=local GOFLAGS=-buildvcs=false
	export TMPDIR="$B/tmp" XDG_CONFIG_HOME="$B/xdg-config"
fi
mkdir -p "$L/out" "$L/data" "${TMPDIR:-/tmp}"
exec >>"$L/out/progress.log" 2>&1
umask 077
echo "== $(date -u +%FT%TZ) start: $* (SMOKE=${SMOKE:-})"
TO=""; command -v timeout >/dev/null && TO="timeout 2700" # an outer bound on lsbench itself

# Builds.
for b in P M A L; do
	if [ ! -x "$L/bin/$b/perkeepd" ]; then
		mkdir -p "$L/bin/$b"
		(cd "$L/trees/$b" && go build -o "$L/bin/$b/" ./server/perkeepd ./cmd/pk-put ./cmd/pk) || { echo "build $b failed"; exit 1; }
	fi
done
(cd "$here" && go build -o "$L/bin/" . ./genphotos) || { echo "build lsbench failed"; exit 1; }
LS=$L/bin/lsbench

# Data, generated once; identical for every build and run.
[ -d "$L/data/small200" ] || "$LS" -gen "$L/data/small200" -n 200 -size 4096
[ -d "$L/data/small1000" ] || "$LS" -gen "$L/data/small1000" -n 1000 -size 4096
[ -d "$L/data/photos100" ] || "$L/bin/genphotos" -dir "$L/data/photos100" -n 100 -mb 3
[ -d "$L/data/photos10" ] || "$L/bin/genphotos" -dir "$L/data/photos10" -n 10 -mb 1
cat "$L"/data/*/* >/dev/null # in the page cache, so reads do not compete with the server's writes

SMALL=$L/data/small1000; PHOTOS=$L/data/photos100; MCIDLE=30s; LRD=10m; WN=10
ROUNDS=5; R3=3
if [ -n "${SMOKE:-}" ]; then # every scenario once, small
	SMALL=$L/data/small200; PHOTOS=$L/data/photos10; MCIDLE=5s; LRD=1m; WN=1; ROUNDS=1; R3=1
fi

# one <scenario> <build> <round> <lsbench args...>
one() {
	sc=$1; b=$2; k=$3; shift 3
	bin=$b; [ "$b" = L2 ] && bin=L # the same build twice: the noise floor
	d=$L/out/$sc
	# Done already, unless it failed for the harness's sake (port in use,
	# pk-put error): that one runs again.
	[ -s "$d/$b-r$k/summary.json" ] && ! grep -q harness_error "$d/$b-r$k/summary.json" && return
	mkdir -p "$d"
	sync; sleep 5
	t0=$(date +%s)
	$TO "$LS" -bin "$L/bin/$bin" -work "$d" -label "$b-r$k" -port 3291 "$@" >"$d/$b-r$k.out" 2>&1
	rc=$?
	echo "$(date -u +%T) $sc $b r$k rc=$rc $(( $(date +%s) - t0 ))s"
	if [ $rc = 124 ]; then # the outer timeout: lsbench itself hung
		mkdir -p "$d/$b-r$k"
		echo '{"label":"'"$b-r$k"'","valid":false,"invalid_reason":"outer timeout 2700 s"}' >"$d/$b-r$k/summary.json"
		pkill -f "listen=127.0.0.1:3291" || true
	fi
}

# rounds <scenario> <rounds> "<builds>" <lsbench args...>: the build order rotates each round.
rounds() {
	sc=$1; n=$2; builds=($3); shift 3
	for k in $(seq 1 "$n"); do
		m=${#builds[@]}
		for i in $(seq 0 $((m - 1))); do
			one "$sc" "${builds[$(( (i + k) % m ))]}" "$k" "$@"
		done
	done
}

W() { # the Go benchmark of the index's write path
	mkdir -p "$L/out/W"
	for b in A L P M; do
		f=$L/out/W/$b.txt
		[ -s "$f" ] && grep -q '^ok' "$f" && continue
		(cd "$L/trees/$b" && go test -run '^$' -bench BenchmarkLiveIndexWrite -benchtime 1000x -count $WN -timeout 45m ./pkg/search/) >"$f" 2>&1
		echo "$(date -u +%T) W $b rc=$?"
	done
}

all="U1 U0 U4 RS W CH MC SL SLAL PH LR"
[ $# -gt 0 ] && all="$*"
for sc in $all; do
	case $sc in
	U0) rounds U0 $ROUNDS "P M A L" -files "$SMALL" -clients 0 ;;
	U1) rounds U1 $ROUNDS "P M A L L2" -files "$SMALL" -clients 1 ;;
	U4) rounds U4 $ROUNDS "P M A L" -files "$SMALL" -clients 4 ;;
	RS) rounds RS $R3 "P M A L" -files "$SMALL" -clients 1 -restart -idle 0 ;;
	W) W ;;
	CH) rounds CH $R3 "P M A L" -files "$SMALL" -clients 1 -churn 5 -subchurn 5 ;;
	MC) rounds MC $R3 "P M A L" -files "$L/data/small200" -clients 100 -idle $MCIDLE ;;
	SL) rounds SL $R3 "P M A L" -files "$L/data/small200" -clients 1 -slow 1 -slowdelay 200ms -slowwait 60s -msgcap 250 ;;
	SLAL) rounds SLAL $R3 "A L" -files "$SMALL" -clients 1 -slow 1 -slowdelay 200ms -slowwait 60s ;;
	PH) rounds PH0 $R3 "P M A L" -files "$PHOTOS" -clients 0
	    rounds PH1 $R3 "P M A L" -files "$PHOTOS" -clients 1 ;;
	LR) rounds LR 1 "P M A L" -longrun $LRD -batch 10 -every 5s -clients 10 -churn 1 -sample 5s -deadline 40m ;;
	*) echo "unknown scenario $sc" ;;
	esac
done
echo "== $(date -u +%FT%TZ) done: $all"
touch "$L/out/ALL.done"
