#!/usr/bin/env python3
"""Freeze the accepted Phase A test baseline (Step B0.1).

Parses the JUnit XML embedded in a full-suite run log (default:
``/usr/src/phzyx/full-ts-run.txt``) and emits, under ``doc/untwist/baseline/``:

  * ``baseline_phaseA.json``  - every testcase and its status (all rows)
  * ``baseline_phaseA.csv``   - same, flat CSV for eyeballing/diffing
  * ``known_failures.json``   - the accepted pre-existing failures, each with a
                                one-line classification

Phase B's "no regression" check diffs a fresh run against ``baseline_phaseA.json``:
the ONLY allowed differences are the accepted known failures.  Any *new* failure
or *new* skip fails the check.

The run log mixes a console transcript with a trailing ``<testsuites>`` XML
document; we parse from the first ``<testsuites>`` marker onward.

Usage:
    python3 doc/untwist/freeze_baseline.py [RUN_LOG] [--out DIR]

Reproducibility: running twice against the same log produces byte-identical
output (sorted keys, stable ordering).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import xml.etree.ElementTree as ET

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_LOG = "/usr/src/phzyx/full-ts-run.txt"
DEFAULT_OUT = os.path.join(HERE, "baseline")


# One-line classification for each accepted pre-existing failure.  Keys are
# "classname::name".  Every failure the parser finds MUST have an entry here or
# the script errors out - that guarantees the known-failures list stays honest
# and a newly-appearing failure cannot silently be absorbed into the baseline.
KNOWN_FAILURE_REASONS = {
    # bridge transfer / park / automixmon feature family (17) - these fail
    # identically on the pre-migration Twisted baseline; feature/timing issues,
    # unrelated to the asyncio runtime.
    "bridge::atxfer_fail_blonde": "bridge attended-transfer feature test; pre-existing failure on Twisted baseline (transfer timing).",
    "bridge::atxfer_nominal": "bridge attended-transfer feature test; pre-existing failure on Twisted baseline (transfer timing).",
    "bridge::atxfer_setup": "bridge attended-transfer feature test; pre-existing failure on Twisted baseline (transfer timing).",
    "bridge::atxfer_threeway_nominal": "bridge attended-transfer feature test; pre-existing failure on Twisted baseline (transfer timing).",
    "bridge::automixmon": "bridge automixmon feature test; pre-existing failure on Twisted baseline.",
    "bridge::automixmon_bridgefeatures": "bridge automixmon feature test; pre-existing failure on Twisted baseline.",
    "bridge::blindxfer_nominal": "bridge blind-transfer feature test; pre-existing failure on Twisted baseline (transfer timing).",
    "bridge::blindxfer_setup": "bridge blind-transfer feature test; pre-existing failure on Twisted baseline (transfer timing).",
    "bridge::blonde_nominal": "bridge blonde-transfer feature test; pre-existing failure on Twisted baseline (transfer timing).",
    "bridge::disconnect": "bridge disconnect feature test; pre-existing failure on Twisted baseline.",
    "bridge::parkcall": "bridge park-call feature test; pre-existing failure on Twisted baseline.",
    "bridge::parkcall_blindxfer": "bridge park-call feature test; pre-existing failure on Twisted baseline.",
    "bridge::parkcall_bridgefeatures": "bridge park-call feature test; pre-existing failure on Twisted baseline.",
    "bridge::simple_bridge": "bridge simple-bridge feature test; pre-existing failure on Twisted baseline.",
    "bridge::transfer_capabilities": "bridge transfer-capabilities feature test; pre-existing failure on Twisted baseline.",
    "bridge::transfer_capabilities_bridgefeatures": "bridge transfer-capabilities feature test; pre-existing failure on Twisted baseline.",
    "bridge::transfer_failure": "bridge transfer-failure feature test; pre-existing failure on Twisted baseline.",
    # CDR (2)
    "cdr.cdr_manipulation::nocdr": "CDR nocdr feature test; pre-existing failure on Twisted baseline (CDR record behavior).",
    "cdr::console_dial_sip_transfer": "CDR console-dial-transfer feature test; pre-existing failure on Twisted baseline.",
    # PJSIP channel family (3)
    "channels.pjsip.dtmf_sdp::dtmf_sdp_recognition": "PJSIP DTMF/SDP negotiation test; pre-existing environment/codec failure on baseline.",
    "channels.pjsip::non_negotiated_frame_SSRC_change": "PJSIP SSRC-change frame test; pre-existing failure on baseline.",
    "channels.pjsip.registration.inbound.nominal.contact_acl::ipv6": "PJSIP IPv6 contact-ACL test; pre-existing IPv6/env failure on baseline.",
    # HEP (3)
    "hep::pjsip": "HEP capture PJSIP test; pre-existing failure on baseline (HEP/packet-capture env).",
    "hep::pjsip_auth": "HEP capture PJSIP auth test; pre-existing failure on baseline (HEP/packet-capture env).",
    "hep::pjsip_ipv6": "HEP capture PJSIP IPv6 test; pre-existing failure on baseline (HEP/packet-capture env).",
    # misc singletons (3)
    "::masquerade": "masquerade test; pre-existing failure on Twisted baseline.",
    "redirecting::forwardername": "redirecting forwardername test; pre-existing failure on Twisted baseline.",
    "rest_api.channels.redirect::nominal": "ARI channel-redirect nominal test; pre-existing failure on baseline.",
    # sorcery cache-expire (2) - waitfullybooted timeout visible in run log
    "sorcery::memory_cache_expire": "sorcery memory-cache-expire test; pre-existing timing failure (waitfullybooted timeout in log).",
    "sorcery::memory_cache_expire_object": "sorcery memory-cache-expire test; pre-existing timing failure (waitfullybooted timeout in log).",
}


def key(tc: ET.Element) -> str:
    return f"{tc.get('classname') or ''}::{tc.get('name') or ''}"


def status_of(tc: ET.Element) -> tuple[str, str]:
    """Return (status, first_message_line)."""
    for tag, status in (("failure", "fail"), ("error", "error"), ("skipped", "skip")):
        el = tc.find(tag)
        if el is not None:
            msg = el.get("message") or ""
            if not msg and el.text:
                msg = el.text.strip().splitlines()[0] if el.text.strip() else ""
            return status, msg[:200]
    return "pass", ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("log", nargs="?", default=DEFAULT_LOG,
                    help=f"full-suite run log (default: {DEFAULT_LOG})")
    ap.add_argument("--out", default=DEFAULT_OUT, help="output directory")
    args = ap.parse_args()

    with open(args.log, encoding="utf-8", errors="replace") as fh:
        data = fh.read()
    marker = data.find("<testsuites")
    if marker < 0:
        print(f"error: no <testsuites> XML found in {args.log}", file=sys.stderr)
        return 2
    root = ET.fromstring(data[marker:])
    suite = root.find(".//testsuite")

    cases = root.findall(".//testcase")
    rows = []
    counts = {"pass": 0, "fail": 0, "skip": 0, "error": 0}
    failures = {}
    for tc in cases:
        st, msg = status_of(tc)
        counts[st] += 1
        row = {
            "key": key(tc),
            "classname": tc.get("classname") or "",
            "name": tc.get("name") or "",
            "status": st,
            "time": tc.get("time") or "",
            "message": msg,
        }
        rows.append(row)
        if st in ("fail", "error"):
            failures[key(tc)] = msg
    rows.sort(key=lambda r: r["key"])

    # Guard: every failure must be pre-classified; nothing new sneaks in.
    unclassified = sorted(k for k in failures if k not in KNOWN_FAILURE_REASONS)
    if unclassified:
        print("error: failures without a KNOWN_FAILURE_REASONS entry:",
              file=sys.stderr)
        for k in unclassified:
            print(f"  {k}", file=sys.stderr)
        return 3
    stale = sorted(k for k in KNOWN_FAILURE_REASONS if k not in failures)
    if stale:
        print("warning: KNOWN_FAILURE_REASONS entries not seen in this run:",
              file=sys.stderr)
        for k in stale:
            print(f"  {k}", file=sys.stderr)

    os.makedirs(args.out, exist_ok=True)

    baseline = {
        "source_log": os.path.abspath(args.log),
        "suite": {
            "name": suite.get("name") if suite is not None else None,
            "timestamp": suite.get("timestamp") if suite is not None else None,
            "reported_tests": suite.get("tests") if suite is not None else None,
            "reported_failures": suite.get("failures") if suite is not None else None,
            "reported_skipped": suite.get("skipped") if suite is not None else None,
        },
        "totals": {
            "testcases": len(rows),
            **counts,
            "executed": counts["pass"] + counts["fail"] + counts["error"],
        },
        "cases": rows,
    }
    with open(os.path.join(args.out, "baseline_phaseA.json"), "w") as fh:
        json.dump(baseline, fh, indent=2, sort_keys=True)
        fh.write("\n")

    with open(os.path.join(args.out, "baseline_phaseA.csv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["key", "classname", "name",
                                           "status", "time", "message"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    known = {
        "count": len(failures),
        "note": ("Accepted pre-existing failures confirmed against the Twisted "
                 "baseline. Phase B must reproduce EXACTLY this set - no new "
                 "failures, no new skips."),
        "failures": {k: KNOWN_FAILURE_REASONS[k] for k in sorted(failures)},
    }
    with open(os.path.join(args.out, "known_failures.json"), "w") as fh:
        json.dump(known, fh, indent=2, sort_keys=True)
        fh.write("\n")

    print(f"wrote baseline to {args.out}")
    print(f"  testcases={len(rows)}  pass={counts['pass']}  "
          f"fail={counts['fail']}  skip={counts['skip']}  error={counts['error']}")
    print(f"  executed (pass+fail)={counts['pass']+counts['fail']}  "
          f"known_failures={len(failures)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
