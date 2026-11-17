#!/usr/bin/env python3
# report.py <out dir> prints the tables of the live-search benchmark from
# the runs under <out dir>/<scenario>/<build>-r<k>/summary.json and the Go
# benchmark output in <out dir>/W/<build>.txt. Every cell is
# "median [min - max]" over the valid runs; n is valid/total runs.
# A cell for L is marked "<" or ">" when L's whole range lies below or above
# the comparator's whole range (ranges do not overlap); otherwise the
# difference is inside the run-to-run spread. No mark with fewer than 3
# valid runs on either side; cells with 1 or 2 runs list the values.
import glob, json, os, re, statistics, sys

out = sys.argv[1]
BUILDS = ["P", "M", "A", "L", "L2"]

def get(d, path):
    for k in path.split("."):
        if not isinstance(d, dict) or k not in d:
            return None
        d = d[k]
    return d

def load(sc):
    runs = {}
    for f in sorted(glob.glob(os.path.join(out, sc, "*", "summary.json"))):
        b = os.path.basename(os.path.dirname(f)).split("-r")[0]
        try:
            runs.setdefault(b, []).append(json.load(open(f)))
        except Exception:
            pass
    return runs

def fmt(v):
    if v is None:
        return "-"
    if isinstance(v, bool):
        return str(v)
    if abs(v) >= 100:
        return "%.0f" % v
    if abs(v) >= 10:
        return "%.1f" % v
    return "%.2f" % v

def cell(vals):
    vals = [v for v in vals if isinstance(v, (int, float)) and not isinstance(v, bool)]
    if not vals:
        return "-", None
    m = statistics.median(vals)
    if len(vals) < 3:  # too few runs to call a difference
        return " / ".join(fmt(v) for v in vals), None
    return "%s [%s - %s]" % (fmt(m), fmt(min(vals)), fmt(max(vals))), (min(vals), max(vals))

def table(sc, metrics, builds=BUILDS, title=None):
    runs = load(sc)
    if not runs:
        print("(no runs for %s)\n" % sc)
        return
    bs = [b for b in builds if b in runs]
    print("**%s**%s\n" % (sc, (" - " + title) if title else ""))
    print("| metric | " + " | ".join("%s (n=%d/%d)" % (b, sum(1 for r in runs[b] if r.get("valid")), len(runs[b])) for b in bs) + " |")
    print("|---" * (len(bs) + 1) + "|")
    for name, path in metrics:
        cells, ranges = [], {}
        for b in bs:
            valid = [r for r in runs[b] if r.get("valid")]
            if path.startswith("count:"):
                p = path[6:]
                c = sum(1 for r in valid if get(r, p) is True)
                cells.append("%d/%d" % (c, len(valid)))
                continue
            s, rg = cell([get(r, path) for r in valid])
            ranges[b] = rg
            cells.append(s)
        if "L" in ranges and ranges["L"]:
            lo, hi = ranges["L"]
            marks = []
            for c in ("A", "P"):
                if ranges.get(c):
                    if hi < ranges[c][0]:
                        marks.append("<" + c)
                    elif lo > ranges[c][1]:
                        marks.append(">" + c)
            if marks:
                i = bs.index("L")
                cells[i] += " (" + ",".join(marks) + ")"
        print("| %s | %s |" % (name, " | ".join(cells)))
    inval = [(b, r.get("label"), r.get("invalid_reason")) for b in bs for r in runs[b] if not r.get("valid")]
    print()
    for b, l, why in inval:
        print("INVALID: %s: %s" % (l, why))
    if inval:
        print()

UP = [
    ("pk-put wall time, s", "put_s"),
    ("files/s", "put_files_per_s"),
    ("upload request p50, ms", "upload_req_ms.p50"),
    ("upload request p99, ms", "upload_req_ms.p99"),
    ("search during upload p50, ms", "search_during_ms.p50"),
    ("search during upload p90, ms", "search_during_ms.p90"),
    ("search during upload p99, ms", "search_during_ms.p99"),
    ("search during upload max, ms", "search_during_ms.max"),
    ("search idle p50, ms", "search_idle_ms.p50"),
    ("perkeepd CPU during upload, s", "upload_cpu_s"),
    ("goroutines peak during upload", "goroutines_peak_upload"),
    ("goroutines before clients", "goroutines_noclients"),
    ("goroutines at end (clients gone)", "goroutines_end"),
    ("RSS peak during upload, kB", "rss_peak_upload_kb"),
    ("cpu steal, %", "cpu_steal_pct"),
]
LIVE = [
    ("result messages to the tracer", "tracer_msgs"),
    ("files shown live (fraction)", "live_shown_frac"),
    ("live latency p50, ms (claim request -> client)", "live_ms.p50"),
    ("live latency p90, ms", "live_ms.p90"),
    ("live latency p99, ms", "live_ms.p99"),
    ("live latency max, ms", "live_ms.max"),
    ("same, from pk-put's ack, p50 ms", "live_from_ack_ms.p50"),
    ("converged to the final result", "count:converged"),
    ("last update after last ack, s", "converged_lag_s"),
    ("clients dropped", "client_dropped"),
    ("hub drop log lines", "log_ws_drops"),
]

def W():
    files = sorted(glob.glob(os.path.join(out, "W", "*.txt")))
    if not files:
        print("(no W runs)\n")
        return
    res = {}
    for f in files:
        b = os.path.basename(f)[:-4]
        for l in open(f):
            m = re.match(r"BenchmarkLiveIndexWrite/(\S+?)-\d+\s+\d+\s+(.*)", l)
            if not m:
                continue
            name, rest = m.group(1), m.group(2)
            vals = dict((u, float(v)) for v, u in re.findall(r"([\d.]+) (\S+/op)", rest))
            for u, v in vals.items():
                res.setdefault((name, u), {}).setdefault(b, []).append(v)
    bs = [b for b in ["P", "M", "A", "L"] if any(b in v for v in res.values())]
    for unit in ["cpu-ns/op", "ns/op", "allocs/op", "B/op", "msgs/op"]:
        print("**W: %s** (median [min - max] of -count runs; '-' = no live update on that route, skipped)\n" % unit)
        print("| sub-benchmark | " + " | ".join(bs) + " |")
        print("|---" * (len(bs) + 1) + "|")
        for name in sorted(set(n for n, u in res if u == unit), key=lambda s: (s.split("/")[0], ["subs=none", "subs=handler", "subs=ws1"].index(s.split("/")[1]))):
            row = []
            rg = {}
            for b in bs:
                s, r = cell(res.get((name, unit), {}).get(b, []))
                rg[b] = r
                row.append(s)
            if rg.get("L"):
                for c in ("A",):
                    if rg.get(c):
                        i = bs.index("L")
                        if rg["L"][1] < rg[c][0]:
                            row[i] += " (<A)"
                        elif rg["L"][0] > rg[c][1]:
                            row[i] += " (>A)"
            print("| %s | %s |" % (name, " | ".join(row)))
        print()

def main():
    scs = sys.argv[2:] or ["U0", "U1", "U4", "PH0", "PH1", "RS", "W", "CH", "MC", "SL", "SLAL", "LR"]
    for sc in scs:
        if sc == "W":
            W()
        elif sc in ("U0", "PH0"):
            table(sc, UP)
        elif sc in ("U1", "U4", "PH1"):
            table(sc, UP + LIVE)
        elif sc == "RS":
            table(sc, [("index complete after restart, s", "restart_indexed_s"),
                       ("result messages to the client", "tracer_msgs"),
                       ("client converged to the final result", "count:converged"),
                       ("client's last update after restart, s", "converged_lag_s"),
                       ("search p50 during catch-up, ms", "search_after_ms.p50"),
                       ("search p99 during catch-up, ms", "search_after_ms.p99"),
                       ("goroutines at end", "goroutines_end")])
        elif sc == "CH":
            table(sc, UP + LIVE + [
                ("connect churn cycles ok", "churn_ok"),
                ("connect churn: first result p50, ms", "churn_first_result_ms.p50"),
                ("connect churn: first result p99, ms", "churn_first_result_ms.p99"),
                ("subscribe churn cycles ok", "subchurn_ok"),
                ("subscribe churn: first result p50, ms", "subchurn_first_result_ms.p50"),
                ("subscribe churn: first result p99, ms", "subchurn_first_result_ms.p99")])
        elif sc == "MC":
            table(sc, [("idle CPU with 100 clients, %", "idle_cpu_pct"),
                       ("goroutines idle with 100 clients", "goroutines_idle"),
                       ("RSS idle with 100 clients, kB", "rss_idle_kb")] + UP + LIVE + [
                       ("messages per client, median", "client_msgs.p50"),
                       ("messages per client, max", "client_msgs.max")])
        elif sc in ("SL", "SLAL"):
            table(sc, UP + LIVE + [("slow reader dropped", "slow_dropped"),
                                   ("slow reader: messages read", "slow_msgs.p50"),
                                   ("slow reader: close frame read at, s", "slow_dropped_at_s.p50"),
                                   ("slow reader: final items missing at pk-put exit", "slow_missing_at_put_exit.p50"),
                                   ("slow reader: final items missing at end", "slow_missing_at_end.p50"),
                                   ("slow reader showed the final result at end", "slow_converged"),
                                   ("slow reader: final result shown after pk-put exit, s", "slow_final_shown_after_put_exit_s.p50")],
                  builds=BUILDS if sc == "SL" else ["A", "L"])
        elif sc == "LR":
            table(sc, [("files uploaded", "files"),
                       ("goroutines/h, 2nd half", "longrun_goroutines_per_h"),
                       ("RSS kB/h, 2nd half", "longrun_rss_kb_per_h"),
                       ("heap MB/h, 2nd half", "longrun_heap_mb_per_h"),
                       ("goroutines at end (clients gone)", "goroutines_end"),
                       ("goroutines before clients", "goroutines_noclients"),
                       ("fds at the start of the load", "fds_upload_first"),
                       ("fds at the end of the load", "fds_upload_last"),
                       ("search p99 during, ms", "search_during_ms.p99"),
                       ("live latency p50, ms", "live_ms.p50"),
                       ("live latency p99, ms", "live_ms.p99"),
                       ("churn cycles ok", "churn_ok"),
                       ("clients dropped", "client_dropped"),
                       ("converged", "count:converged")])

main()
