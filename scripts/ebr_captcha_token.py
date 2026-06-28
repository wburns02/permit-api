#!/usr/bin/env python3
"""2Captcha token provider for the EBR (East Baton Rouge) SmartCAMA scraper.

SmartCAMA (`eastbatonrouge.smartcama.com`) gates its assessment search behind
reCAPTCHA **Enterprise v2 CHECKBOX** ("I'm not a robot"):

    site key : 6Le9Gb8eAAAAADRJkJ3Lt5aryjSL8dj8wQKVc7gm
    page URL : https://eastbatonrouge.smartcama.com/Assessments/Search
    type     : reCAPTCHA Enterprise v2 checkbox (size=normal, enterprise.js,
               grecaptcha.enterprise present, data-callback=_globalCaptchaCallback)
    NOT a v3 score token, NOT invisible. An image fallback may appear but
    2Captcha workers solve it; there is NO score gate / data-action.

This script submits the captcha job to 2Captcha, polls for the solved
`g-recaptcha-response` token, and prints **only the token** to stdout (so it can
be used directly as the scraper's `--token-cmd`). Diagnostics go to stderr.

The 2Captcha API key is a SECRET. It is read from (in order):
    1. env  EBR_2CAPTCHA_KEY
    2. env  TWOCAPTCHA_KEY
    3. file ~/.config/permitlookup/2captcha.key  (chmod 600)
    4. --key  (discouraged; visible in process list)
The key is NEVER printed or logged.

Usage:
    python3 scrape_ebr_improvements.py \
        --token-cmd "python3 scripts/ebr_captcha_token.py"

Exit codes: 0 token printed | 2 no key | 3 submit failed | 4 solve timeout/err.
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import requests

SITE_KEY = "6Le9Gb8eAAAAADRJkJ3Lt5aryjSL8dj8wQKVc7gm"
PAGE_URL = "https://eastbatonrouge.smartcama.com/Assessments/Search"

IN_URL = "https://2captcha.com/in.php"
RES_URL = "https://2captcha.com/res.php"

KEY_FILE_DEFAULT = os.path.expanduser("~/.config/permitlookup/2captcha.key")


def _log(*a):
    print("[ebr-captcha]", *a, file=sys.stderr, flush=True)


def load_key(cli_key: str | None) -> str | None:
    for env in ("EBR_2CAPTCHA_KEY", "TWOCAPTCHA_KEY"):
        v = os.environ.get(env)
        if v and v.strip():
            return v.strip()
    path = os.environ.get("EBR_2CAPTCHA_KEY_FILE", KEY_FILE_DEFAULT)
    if path and os.path.exists(path):
        try:
            with open(path) as fh:
                v = fh.read().strip()
            if v:
                return v
        except OSError as e:  # pragma: no cover
            _log(f"key file read failed: {e}")
    if cli_key and cli_key.strip():
        return cli_key.strip()
    return None


def solve(key: str, *, page_url: str, site_key: str, action: str | None,
          poll_timeout: int, poll_interval: int, proxy: str | None) -> str:
    """Submit + poll one reCAPTCHA Enterprise v2 job. Returns the token or raises."""
    payload = {
        "key": key,
        "method": "userrecaptcha",
        "googlekey": site_key,
        "pageurl": page_url,
        "enterprise": 1,          # this site is reCAPTCHA ENTERPRISE
        "json": 1,
    }
    if action:
        payload["action"] = action  # only meaningful for score/v3
    if proxy:
        ptype, _, rest = proxy.partition(":")
        payload["proxytype"] = ptype.upper()
        payload["proxy"] = rest
    r = requests.post(IN_URL, data=payload, timeout=30)
    r.raise_for_status()
    j = r.json()
    if str(j.get("status")) != "1":
        raise RuntimeError(f"2captcha submit error: {j.get('request')}")
    cap_id = j["request"]
    _log(f"submitted job id={cap_id} (enterprise v2 checkbox); polling...")

    deadline = time.time() + poll_timeout
    time.sleep(min(poll_interval, 15))  # workers need ~15s minimum
    while time.time() < deadline:
        rr = requests.get(RES_URL, params={
            "key": key, "action": "get", "id": cap_id, "json": 1}, timeout=30)
        rr.raise_for_status()
        jj = rr.json()
        st = str(jj.get("status"))
        req = jj.get("request")
        if st == "1":
            _log(f"solved job id={cap_id} token_len={len(req)}")
            return req
        if req == "CAPCHA_NOT_READY":
            time.sleep(poll_interval)
            continue
        raise RuntimeError(f"2captcha solve error: {req}")
    raise TimeoutError(f"2captcha solve timed out after {poll_timeout}s")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--page-url", default=PAGE_URL)
    ap.add_argument("--site-key", default=SITE_KEY)
    ap.add_argument("--action", default=os.environ.get("EBR_CAPTCHA_ACTION", ""),
                    help="reCAPTCHA action (only for score/v3; blank for v2 checkbox)")
    ap.add_argument("--poll-timeout", type=int, default=180)
    ap.add_argument("--poll-interval", type=int, default=5)
    ap.add_argument("--retries", type=int, default=2,
                    help="re-submit on solve error/timeout this many extra times")
    ap.add_argument("--proxy", default=os.environ.get("EBR_2CAPTCHA_PROXY", ""),
                    help="optional proxy type:user:pass@host:port")
    ap.add_argument("--key", default="", help="2captcha key (prefer env/file)")
    args = ap.parse_args()

    key = load_key(args.key or None)
    if not key:
        _log("NO 2CAPTCHA KEY. Set EBR_2CAPTCHA_KEY env, or write "
             f"{KEY_FILE_DEFAULT} (chmod 600).")
        sys.exit(2)

    last_err: Exception | None = None
    for attempt in range(args.retries + 1):
        try:
            tok = solve(
                key, page_url=args.page_url, site_key=args.site_key,
                action=args.action or None, poll_timeout=args.poll_timeout,
                poll_interval=args.poll_interval, proxy=args.proxy or None)
            print(tok)  # ONLY the token to stdout
            return
        except (RuntimeError, TimeoutError, requests.RequestException) as e:
            last_err = e
            _log(f"attempt {attempt + 1}/{args.retries + 1} failed: {e}")
            time.sleep(2)
    _log(f"GAVE UP: {last_err}")
    sys.exit(4 if isinstance(last_err, TimeoutError) else 3)


if __name__ == "__main__":
    main()
