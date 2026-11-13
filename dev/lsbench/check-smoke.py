#!/usr/bin/env python3
# check-smoke.py <out dir> checks what a SMOKE=1 pass of vm-bench.sh must
# show for builds P, M, A and L (the live-search change: each client gets
# only its newest results, so it never drops a slow reader), scenario by
# scenario, from the summaries alone. It prints one line per check and
# exits 1 if any fails. These are harness checks, not performance ones.
import glob, json, os, sys

out = sys.argv[1]
fails = 0

def runs(sc):
    r = {}
    for f in glob.glob(os.path.join(out, sc, "*", "summary.json")):
        r[os.path.basename(os.path.dirname(f)).split("-r")[0]] = json.load(open(f))
    return r

def check(name, ok, detail=""):
    global fails
    if not ok:
        fails += 1
    print("%s %s %s" % ("ok  " if ok else "FAIL", name, detail))

def g(d, path):
    for k in path.split("."):
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d

live = {"P": True, "M": False, "A": True, "L": True}
for sc in ["U0", "U1", "U4", "RS", "CH", "MC", "SL", "SLAL", "PH0", "PH1", "LR"]:
    rs = runs(sc)
    want = {"SLAL": ["A", "L"], "U1": ["P", "M", "A", "L", "L2"]}.get(sc, ["P", "M", "A", "L"])
    check(sc + ": every build ran", sorted(rs) == sorted(want), str(sorted(rs)))
    for b, s in sorted(rs.items()):
        bb = "L" if b == "L2" else b
        tag = "%s/%s" % (sc, b)
        check(tag + " valid", s.get("valid") is True, s.get("invalid_reason") or s.get("harness_error") or "")
        if not s.get("valid"):
            continue
        check(tag + " no panics in perkeepd.log", s.get("log_panics") == 0)
        check(tag + " goroutines back after clients left (within 10)",
              s.get("goroutines_end", 1e9) - s.get("goroutines_noclients", 0) <= 10,
              "%s -> %s" % (s.get("goroutines_noclients"), s.get("goroutines_end")))
        if sc in ("U0", "PH0"):
            check(tag + " upload timed", (s.get("put_s") or 0) > 0 and g(s, "upload_req_ms.n") > 0)
        if s.get("clients", 0) > 0 and sc != "RS":
            check(tag + " fast clients not dropped", s.get("client_dropped") == 0)
            if live[bb]:
                check(tag + " tracer converged", s.get("converged") is True)
                check(tag + " live latencies measured", g(s, "live_ms.n") > 0, "n=%s" % g(s, "live_ms.n"))
            else:
                check(tag + " no live update (M)", s.get("tracer_msgs") == 1 and s.get("converged") is False)
        if sc == "RS":
            check(tag + " converged only on L", s.get("converged") is (bb == "L"), "converged=%s msgs=%s" % (s.get("converged"), s.get("tracer_msgs")))
        if sc == "CH":
            check(tag + " churn without failures", s.get("churn_fail") is None and s.get("subchurn_fail") is None and s.get("churn_ok", 0) > 0 and s.get("subchurn_ok", 0) > 0)
        if sc == "MC":
            check(tag + " 100 clients, none dropped", s.get("clients") == 100 and s.get("client_dropped") == 0)
        if sc == "SL":
            check(tag + " slow reader not dropped", s.get("slow_dropped") == 0)
            if live[bb]:
                check(tag + " slow reader caught up", g(s, "slow_final_shown_after_put_exit_s.n") == 1, str(s.get("slow_final_shown_after_put_exit_s")))
        if sc in ("SL", "SLAL") and bb == "L":
            check(tag + " L keeps the slow reader and shows it the final result",
                  s.get("slow_dropped") == 0 and g(s, "slow_missing_at_end.max") == 0 and s.get("log_ws_drops") == 0,
                  "dropped=%s missing_at_end=%s log_ws_drops=%s" % (s.get("slow_dropped"), g(s, "slow_missing_at_end.max"), s.get("log_ws_drops")))
        if sc == "LR":
            check(tag + " files = batches x 10", s.get("files") == 10 * s.get("longrun_batches", -1), "%s files, %s batches" % (s.get("files"), s.get("longrun_batches")))
            check(tag + " fds sampled (Linux only)", True, "first=%s last=%s" % (s.get("fds_upload_first"), s.get("fds_upload_last")))

for b in ["P", "M", "A", "L"]:
    f = os.path.join(out, "W", b + ".txt")
    t = open(f).read() if os.path.exists(f) else ""
    check("W/%s go test ok" % b, "\nok" in t)
    has_hook_ws1 = "route=hook/subs=ws1" in t
    check("W/%s hook/ws1 present only where the hook route announces" % b, has_hook_ws1 == (b in ("A", "L")))
sys.exit(1 if fails else 0)
