# Production incident runbook (server `usa`, 199.244.48.218)

Deployment root: `/opt/sub2api-deploy`. Compose services: `sub2api`, `postgres`, `redis`.
Access rule: SSH only through Electerm MCP bookmark `usa` (id `MEPW34u`). Never direct ssh/plink.
Prefer `mcp__electerm_execute_electerm_command` (structured stdout/stderr/exitCode) over terminal send + idle-wait.
Electerm MCP calls time out around 30 s, so keep every remote command under ~25 s and poll instead of sleeping long.

## Incident 2026-09-01: login returns 503 `SERVICE_UNAVAILABLE`

### Symptom set

- Web login fails with HTTP 503, body `{"code":503,"message":"service temporarily unavailable","reason":"SERVICE_UNAVAILABLE"}`.
- `GET /health` still returns 200 `{"status":"ok"}`, and the `sub2api` container still reports healthy.
  The app health probe does not depend on Postgres, so a healthy app never proves a healthy stack.
- `docker compose ps` shows `sub2api-postgres` in `Restarting (1)`.

### Root cause chain

1. `xray.service` ran with `log.loglevel: "info"` and access logging enabled, writing every dial and
   every accepted connection into `/var/log/syslog` at roughly 1.2 GB/day.
2. Distribution rotation for syslog is weekly with no size cap, so `syslog` reached 2.2 GB and
   `syslog.1` reached 1.4 GB.
3. Combined with a 6.9 GB Docker build cache, the 50 GB root filesystem hit 100 percent.
4. Postgres could not create `postmaster.pid` (`FATAL: could not write lock file "postmaster.pid": No space left on device`),
   so it crash-looped, and every DB-backed route including login returned 503.

### Diagnosis commands

```bash
cd /opt/sub2api-deploy && docker compose ps
curl -sS -o /dev/null -w 'health=%{http_code}\n' http://127.0.0.1:8080/health
curl -sS -X POST http://127.0.0.1:8080/api/v1/auth/login \
  -H 'Content-Type: application/json' -d '{"email":"probe@invalid.invalid","password":"x"}'
docker compose logs --tail 60 postgres
df -h /
du -xh --max-depth=1 / | sort -rh | head -15
docker system df
find /var/log -maxdepth 1 -type f -size +50M -printf '%s %p\n' | sort -rn
```

Interpretation: bad-credential login returning 401 means the auth path and DB are healthy.
A 503 on the same probe means a backing dependency is down.

### Remediation applied

```bash
truncate -s 0 /var/log/syslog
rm -f /var/log/syslog.1 /var/log/syslog.[2-4].gz /var/log/btmp.1
journalctl --vacuum-size=200M
docker builder prune -af
docker image prune -f
cd /opt/sub2api-deploy && docker compose restart postgres
timeout 120 sh -c 'until docker compose exec -T postgres pg_isready -q; do sleep 3; done'
```

Postgres performed automatic WAL recovery (`redo starts at 9/280BFB28`, `redo done`, `database system is ready`).
No data loss and no restore was needed. Disk went 100 percent used to 75 percent used, 12 GB free.

Never delete `/opt/sub2api-deploy/postgres_data`. Reclaim only logs, journal, Docker build cache, and dangling images.

### Preventive changes applied

1. `/usr/local/etc/xray/config.json` log block set to `{"loglevel":"warning","access":"none"}`.
   Backup kept at `/root/xray-config.backup-20260901.json`.
   Validate before restart: `/usr/local/bin/xray run -test -config /usr/local/etc/xray/config.json`.
   Effect measured: syslog growth fell from about 1.2 GB/day to about 18 MB/day.
   Setting only `loglevel` is insufficient; the access log is separate and still emits `accepted tcp:` lines.
2. `/etc/logrotate.d/zz-syslog-sizecap`: daily rotation, `rotate 3`, `maxsize 200M`, `copytruncate`
   for `syslog`, `auth.log`, `kern.log`, `user.log`.
3. `/etc/systemd/journald.conf.d/99-size-cap.conf`: `SystemMaxUse=300M`, `SystemKeepFree=2G`.
4. Docker daemon already had `json-file` with `max-size 50m`, `max-file 3`; left unchanged.

### Verification evidence

- `df -h /` → 36 G used, 12 G available, 75 percent.
- All three compose services healthy; Postgres healthy after restart.
- `GET /health` → 200. `POST /api/v1/auth/login` with bad credentials → 401 on loopback and on
  `http://199.244.48.218:8080`. Frontend root → 200.
- Measured syslog growth after the fix: 4.6 KB per 20 s, about 18 MB/day.

### Standing disk-pressure watchlist

Large paths unrelated to sub2api that were left untouched and need owner approval before cleanup:
`/opt/scout-cloud-worker` 6.1 GB, `/opt/warp-panel.bak.2026-07-05_1221` 1.5 GB, `/opt/warp-panel` 1.5 GB,
`/swapfile` and `/swapfile2` 4 GB each. `/var/lib/containerd` holds the Docker image content.

Recommended periodic check: `df -h /` plus `docker system df`; act when root exceeds 85 percent.
