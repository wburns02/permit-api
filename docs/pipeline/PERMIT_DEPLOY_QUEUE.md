# R730 Deploy Queue

Entries below are QUEUED for manual execution by the owner.
Do NOT run these from CI or any automated system. SSH into R730 and follow the steps.

---

## 2026-06-15 — Phase 1 Security Close-Out

**Merge commit:** `d1f68a3cdd17cab99540dfce173d0858ded4a5ee`
**Branch merged:** `auto/permit-to-a-2026-06-15`
**Status:** QUEUED (not yet applied to R730)

### Steps

```bash
ssh R730
cd permit-api-live
git fetch
git merge origin/main
systemctl --user restart permit-api.service
# Verify:
curl -s https://permits.ecbtx.com/health | python3 -m json.tool
```

Expected: `{"status": "ok"}` (or equivalent healthy response).
If the health check fails, check `journalctl --user -u permit-api.service -n 50`.
