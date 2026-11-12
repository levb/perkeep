#!/bin/bash
# mktrees.sh builds the four scratch trees of the live-search benchmark from
# commits of a Perkeep clone, with read-only git commands only (git
# archive), and packs them, with the harness, for the machine that runs
# the suite (see README.md).
#
#   mktrees.sh <out dir> [<L rev>]
#
# P = e69259ad9 (before Oct 18), M = master 0e6925347, A = koz1-live-search
# 1fd04a4a7, L = <L rev>. L defaults to the newest commit on HEAD that
# changes anything outside dev/lsbench: the branch's last change commit,
# not the harness. L must descend from M, so L is M plus exactly the
# change's commits.
#
# The harness is taken from HEAD's commit, not the working tree; commit it
# first. Each tree gets HEAD's dev/lsbench/lsbench_index_test.go.txt as
# pkg/search/lsbench_index_test.go. Writes <out dir>/trees/{P,M,A,L},
# <out dir>/tree-{P,M,A,L}.tgz, <out dir>/lsbench.tgz (dev/lsbench at HEAD),
# <out dir>/REVS (build, rev, full hash; H is the harness's commit) and
# <out dir>/SHA256 (of git diff M L, and of every tarball).
set -eu
[ $# -ge 1 ] && [ $# -le 2 ] || { echo "usage: mktrees.sh <out dir> [<L rev>]" >&2; exit 2; }
out=$1
clone=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
G() { git -C "$clone" "$@"; }
H=$(G rev-parse --verify HEAD^{commit})
L=${2:-$(G log -1 --format=%H HEAD -- . ':(exclude)dev/lsbench')}
L=$(G rev-parse --verify "$L^{commit}")
M=$(G rev-parse --verify 0e6925347^{commit})
G merge-base --is-ancestor "$M" "$L" || { echo "L $L does not descend from M $M" >&2; exit 1; }
if [ -n "$(G status --porcelain -- dev/lsbench)" ]; then
	echo "warning: dev/lsbench has uncommitted changes; shipping HEAD's" >&2
fi
mkdir -p "$out/trees"
: >"$out/REVS"
for spec in P:e69259ad9 M:$M A:1fd04a4a7 L:$L H:$H; do
	b=${spec%%:*}; rev=${spec#*:}
	echo "$b $rev $(G rev-parse --verify "$rev^{commit}")" >>"$out/REVS"
	[ "$b" = H ] && continue
	rm -rf "$out/trees/$b"; mkdir -p "$out/trees/$b"
	G archive "$rev" | tar -x -C "$out/trees/$b"
done
for b in P M A L; do
	G show "$H:dev/lsbench/lsbench_index_test.go.txt" >"$out/trees/$b/pkg/search/lsbench_index_test.go"
	COPYFILE_DISABLE=1 tar --no-xattrs -czf "$out/tree-$b.tgz" -C "$out/trees/$b" .
done
G archive --format=tar.gz -o "$out/lsbench.tgz" "$H" dev/lsbench
{
	echo "$(G diff "$M" "$L" | shasum -a 256 | cut -d' ' -f1)  git-diff-M-L"
	cd "$out" && shasum -a 256 tree-*.tgz lsbench.tgz
} >"$out/SHA256"
G diff --stat "$M" "$L" | tail -1
cat "$out/REVS" "$out/SHA256"
