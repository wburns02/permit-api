#!/usr/bin/env python3
"""Fetch files from RRC's Axway MFT public links (mft.rrc.texas.gov).

The MFT "link" pages are PrimeFaces JSF apps: each file download is a form
postback carrying javax.faces.ViewState plus the file row's commandlink id.

Usage:
  rrc_mft_fetch.py LINK_UUID --list
  rrc_mft_fetch.py LINK_UUID --get daf420.dat --out /path/daf420.dat
  rrc_mft_fetch.py LINK_UUID --get-all --match 'daf420' --outdir /path/

Datasets (see docs/permit-intelligence-blueprint-2026-06-09.md section 2.6):
  5f07cc72-2e79-4df8-ade1-9aeb792e03fc  drilling permit master+trailer daily (daf420)
  f5dfea9c-bb39-4a5e-a44e-fb522e088cba  drilling permit master+trailer end-of-month
  1f5ddb8d-329a-4459-b7f8-177b4f5ee60d  production data query dump (CSV)
  701db9a3-32b5-488d-812b-cd6ff7d0fe85  statewide API data (ASCII)
  650649b7-e019-4d77-a8e0-d118d6455381  wellbore query data (ASCII)
  ed7ab066-879f-40b6-8144-2ae4b6810c04  completion information (ASCII)
  04652169-eed6-4396-9019-2e270e790f6c  P-5 organizations
  d2438c05-b42f-45a8-b0c6-edceb0912767  UIC database
"""
import argparse
import html as htmllib
import os
import re
import sys
import time

import requests

BASE = "https://mft.rrc.texas.gov"
UA = "Mozilla/5.0 (X11; Linux x86_64) permits.ecbtx.com data pipeline"
CHUNK = 1 << 20


def get_page(sess: requests.Session, uuid: str) -> str:
    r = sess.get(f"{BASE}/link/{uuid}", timeout=60)
    r.raise_for_status()
    return r.text


def parse_files(page: str):
    """Return list of (link_id, filename, size_text) from the file table."""
    out = []
    # <a id="fileTable:0:j_id_2f" href="#" ...>daf420.dat</a>
    for m in re.finditer(
        r'<a id="(fileTable:\d+:[^"]+)"[^>]*>([^<]+)</a>', page
    ):
        out.append((m.group(1), htmllib.unescape(m.group(2)).strip()))
    return out


def parse_form_fields(page: str, form_id: str = "fileList") -> dict:
    """All hidden inputs scoped to one JSF form (each form has its own
    ViewState; using another form's ViewState makes the postback a no-op)."""
    m = re.search(
        rf'<form id="{form_id}".*?</form>', page, re.DOTALL
    )
    if not m:
        raise RuntimeError(f"form {form_id} not found; page layout changed?")
    block = m.group(0)
    fields = {}
    for im in re.finditer(r'<input[^>]*type="hidden"[^>]*>', block):
        tag = im.group(0)
        name = re.search(r'name="([^"]+)"', tag)
        value = re.search(r'value="([^"]*)"', tag)
        if name:
            fields[name.group(1)] = htmllib.unescape(
                value.group(1)) if value else ""
    if "javax.faces.ViewState" not in fields:
        raise RuntimeError(f"no ViewState inside form {form_id}")
    return fields


def download(sess: requests.Session, uuid: str, link_id: str, fname: str,
             dest: str) -> int:
    """One fresh page-load per download: ViewState is single-use-ish."""
    page = get_page(sess, uuid)
    data = parse_form_fields(page, "fileList")
    data[link_id] = link_id
    with sess.post(
        f"{BASE}/webclient/godrive/PublicGoDrive.xhtml",
        data=data, stream=True, timeout=600,
    ) as r:
        r.raise_for_status()
        ctype = r.headers.get("Content-Type", "")
        if "text/html" in ctype:
            raise RuntimeError(
                f"{fname}: got HTML back, not a file (postback failed)"
            )
        tmp = dest + ".part"
        n = 0
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(CHUNK):
                f.write(chunk)
                n += len(chunk)
        os.replace(tmp, dest)
        return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("uuid")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--get", help="exact filename to download")
    ap.add_argument("--get-all", action="store_true")
    ap.add_argument("--match", help="substring filter for --list/--get-all")
    ap.add_argument("--out", help="output path for --get")
    ap.add_argument("--outdir", default=".", help="output dir for --get-all")
    args = ap.parse_args()

    sess = requests.Session()
    sess.headers["User-Agent"] = UA
    page = get_page(sess, args.uuid)
    files = parse_files(page)
    if args.match:
        files = [f for f in files if args.match in f[1]]

    if args.list or not (args.get or args.get_all):
        for link_id, fname in files:
            print(f"{fname}\t{link_id}")
        return

    targets = []
    if args.get:
        targets = [(lid, fn) for lid, fn in files if fn == args.get]
        if not targets:
            sys.exit(f"file not found on page: {args.get}")
    elif args.get_all:
        targets = files

    os.makedirs(args.outdir, exist_ok=True)
    for link_id, fname in targets:
        dest = args.out if (args.get and args.out) else os.path.join(
            args.outdir, fname)
        if os.path.exists(dest) and os.path.getsize(dest) > 0:
            print(f"skip (exists): {dest}", flush=True)
            continue
        t0 = time.time()
        n = download(sess, args.uuid, link_id, fname, dest)
        print(f"ok: {dest} {n:,}B in {time.time()-t0:.0f}s", flush=True)
        time.sleep(2)  # be polite between postbacks


if __name__ == "__main__":
    main()
