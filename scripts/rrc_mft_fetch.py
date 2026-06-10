#!/usr/bin/env python3
"""Fetch files from RRC's Axway MFT public links (mft.rrc.texas.gov).

The MFT "link" pages are PrimeFaces JSF apps: each file download is a form
postback carrying javax.faces.ViewState plus the file row's commandlink id.
The file table paginates at 250 rows; pagination is a PrimeFaces AJAX
partial request, and the ViewState evolves per request, so listing and
downloading happen inside one stateful session (MftSession).

Usage:
  rrc_mft_fetch.py LINK_UUID --list
  rrc_mft_fetch.py LINK_UUID --get daf420.dat --out /path/daf420.dat
  rrc_mft_fetch.py LINK_UUID --get-all --match 'daf420' --outdir /path/

Datasets (see docs/permit-intelligence-blueprint-2026-06-09.md section 2.6):
  5f07cc72-2e79-4df8-ade1-9aeb792e03fc  drilling permit master+trailer daily (daf420)
  f5dfea9c-bb39-4a5e-a44e-fb522e088cba  drilling permit master+trailer end-of-month
  beeeab0c-7d07-4111-af88-783c93677b2c  drilling permit full master (daf802)
  1f5ddb8d-329a-4459-b7f8-177b4f5ee60d  production data query dump (CSV)
  650649b7-e019-4d77-a8e0-d118d6455381  wellbore query data (ASCII)
  ed7ab066-879f-40b6-8144-2ae4b6810c04  completion information (ASCII)
  04652169-eed6-4396-9019-2e270e790f6c  P-5 organizations
  d2438c05-b42f-45a8-b0c6-edceb0912767  UIC database
  d551fb20-442e-4b67-84fa-ac3f23ecabb4  well shapefiles by county
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
PAGE_SIZE = 250


class MftSession:
    """Stateful JSF session for one MFT link page."""

    def __init__(self, uuid: str):
        self.uuid = uuid
        self.sess = requests.Session()
        self.sess.headers["User-Agent"] = UA
        self.fields: dict = {}
        self._load()

    def _load(self):
        r = self.sess.get(f"{BASE}/link/{self.uuid}", timeout=60)
        r.raise_for_status()
        self.fields = self._parse_form_fields(r.text, "fileList")
        self.first = 0
        self.rows = self._parse_rows(r.text)

    @staticmethod
    def _parse_form_fields(page: str, form_id: str) -> dict:
        """Hidden inputs scoped to one JSF form (each form has its own
        ViewState; another form's ViewState makes the postback a no-op)."""
        m = re.search(rf'<form id="{form_id}".*?</form>', page, re.DOTALL)
        if not m:
            raise RuntimeError(f"form {form_id} not found; layout changed?")
        fields = {}
        for im in re.finditer(r'<input[^>]*type="hidden"[^>]*>', m.group(0)):
            tag = im.group(0)
            name = re.search(r'name="([^"]+)"', tag)
            value = re.search(r'value="([^"]*)"', tag)
            if name:
                fields[name.group(1)] = htmllib.unescape(
                    value.group(1)) if value else ""
        if "javax.faces.ViewState" not in fields:
            raise RuntimeError(f"no ViewState inside form {form_id}")
        return fields

    @staticmethod
    def _parse_rows(html: str):
        """(link_id, filename) pairs from rendered table rows."""
        return [
            (m.group(1), htmllib.unescape(m.group(2)).strip())
            for m in re.finditer(
                r'<a id="(fileTable:\d+:[^"]+)"[^>]*>([^<]+)</a>', html)
        ]

    def paginate(self, first: int):
        """PrimeFaces AJAX pagination; updates rows and ViewState."""
        data = dict(self.fields)
        data.update({
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": "fileTable",
            "javax.faces.partial.execute": "fileTable",
            "javax.faces.partial.render": "fileTable",
            "fileTable": "fileTable",
            "fileTable_pagination": "true",
            "fileTable_first": str(first),
            "fileTable_rows": str(PAGE_SIZE),
            "fileTable_skipChildren": "true",
            "fileTable_encodeFeature": "true",
        })
        r = self.sess.post(
            f"{BASE}/webclient/godrive/PublicGoDrive.xhtml",
            data=data, timeout=60,
            headers={"Faces-Request": "partial/ajax",
                     "X-Requested-With": "XMLHttpRequest"},
        )
        r.raise_for_status()
        body = r.text
        vs = re.search(
            r'<update id="[^"]*javax\.faces\.ViewState[^"]*">\s*<!\[CDATA\[(.*?)\]\]>',
            body, re.DOTALL)
        if vs:
            self.fields["javax.faces.ViewState"] = vs.group(1).strip()
        self.first = first
        self.rows = self._parse_rows(body)

    def list_all(self):
        """Walk all pages; yields (link_id, filename) across the table."""
        first = 0
        if self.first != 0:
            self._load()
        while True:
            if not self.rows:
                break
            yield from self.rows
            if len(self.rows) < PAGE_SIZE:
                break
            first += PAGE_SIZE
            self.paginate(first)
            time.sleep(1)

    def download(self, link_id: str, fname: str, dest: str) -> int:
        """Postback the row commandlink using current session ViewState.

        The link's row index must be on the currently-rendered page, so
        callers iterate page by page and download within each page.
        """
        data = dict(self.fields)
        data["fileList"] = "fileList"
        data[link_id] = link_id
        with self.sess.post(
            f"{BASE}/webclient/godrive/PublicGoDrive.xhtml",
            data=data, stream=True, timeout=600,
        ) as r:
            r.raise_for_status()
            if "text/html" in r.headers.get("Content-Type", ""):
                raise RuntimeError(
                    f"{fname}: got HTML back, not a file (postback failed)")
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
    ap.add_argument("--match", help="substring filter")
    ap.add_argument("--out", help="output path for --get")
    ap.add_argument("--outdir", default=".", help="output dir for --get-all")
    args = ap.parse_args()

    ms = MftSession(args.uuid)

    if args.list or not (args.get or args.get_all):
        for link_id, fname in ms.list_all():
            if args.match and args.match not in fname:
                continue
            print(f"{fname}\t{link_id}")
        return

    os.makedirs(args.outdir, exist_ok=True)
    wanted_done = False
    first = 0
    while True:
        page_rows = list(ms.rows)
        for link_id, fname in page_rows:
            if args.get and fname != args.get:
                continue
            if args.get_all and args.match and args.match not in fname:
                continue
            dest = (args.out if (args.get and args.out)
                    else os.path.join(args.outdir, fname))
            if os.path.exists(dest) and os.path.getsize(dest) > 0:
                print(f"skip (exists): {dest}", flush=True)
            else:
                t0 = time.time()
                n = ms.download(link_id, fname, dest)
                print(f"ok: {dest} {n:,}B in {time.time()-t0:.0f}s",
                      flush=True)
                time.sleep(2)
            if args.get:
                wanted_done = True
                break
        if wanted_done or len(page_rows) < PAGE_SIZE:
            break
        first += PAGE_SIZE
        ms.paginate(first)

    if args.get and not wanted_done:
        sys.exit(f"file not found on any page: {args.get}")


if __name__ == "__main__":
    main()
