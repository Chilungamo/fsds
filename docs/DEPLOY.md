# Deploying Restatement Radar

One Docker image runs everything: the dashboard (API + web app, `radar serve`), the EDGAR
watcher (`radar watch`, or embedded in the web process with `RADAR_EMBEDDED_WATCHER=1`)
and batch jobs (`radar reconcile`, `radar backfill`, `radar enrich`). It needs one Postgres
16 database. Files for each host:

| Host | Files |
|---|---|
| Render (recommended to start) | `render.yaml` (Blueprint) |
| Fly.io | `fly.toml` |
| A VM (DigitalOcean, Hetzner, ...) | `docker-compose.yml` + `deploy/vm/docker-compose.vm.yml` (HTTPS via Caddy, daily backups) |

## What the app actually needs (measured, 10 Nov 2026)

| Measure | Result |
|---|---|
| Memory, one web process | 72 MB idle, 92 MB under load, +26 MB for 500 open live streams (~50 KB each) |
| Throughput, one web process (one CPU core) | ~400 API requests/s ≈ 80 full page views/s; p95 141 ms at 32 concurrent users |
| Live stream fan-out | one event to 500 open browsers in 114 ms; database connections stay flat (one listener per process) |
| Transfer per first visit (gzip) | ~233 KB (JS 214 KB, CSS 5 KB, data ~14 KB); returning visitors ~14 KB (assets cached as immutable) |
| Transfer per further case viewed | ~8 KB |
| Database | 152 MB for all cases 2019–2026; a compressed dump is 7 MB |

On a 0.5-CPU instance (Render Starter) expect roughly half the throughput, still ~200
requests/s. **CPU is not what grows the bill; bandwidth and the database tier are.**

## Traffic scenarios

Assumes ~170 KB per visit (60% first-time visitors, 3 cases viewed per visit).

| Scenario | Visits / month | Peak load | Bandwidth / month |
|---|---|---|---|
| A. Portfolio | 2,000 | a few requests/s | ~0.3 GB |
| B. Growing | 100,000 (~3,300/day) | ~5–10 requests/s | ~17 GB |
| C. Popular | 1,000,000 (~33,000/day) | ~20 requests/s, ~700 open live streams | ~170 GB |
| Spike (e.g. front page of a news site) | 50,000 in a few hours | ~100 requests/s | ~8 GB in a day |

## Monthly cost by host

Prices are from third-party pricing summaries found on 10 Nov 2026; the official pages could
not be opened from the build environment, and several sources disagree. **Check each
provider's pricing page before committing.** Bandwidth allowances in particular were not
confirmed.

| | A. Portfolio | B. Growing | C. Popular |
|---|---|---|---|
| **Render** | **~$13**: web Starter $7 (watcher embedded) + Postgres Basic-256MB $6 + cron (cents) | **~$33**: + separate worker $7, Postgres Basic-1GB $19 | **~$60–70**: 2 × Starter web $14 (redundancy) + worker $7 + Basic-1GB $19 + bandwidth over the allowance at ~$0.15/GB (~$25 if none included; much less behind a CDN) |
| **Fly.io** | **~$42** with Fly Managed Postgres ($38 Basic) + 512 MB machine $3.69; **~$4–6** with an external free-tier Postgres (e.g. Neon) | **~$45** (MPG) or **~$25** (external paid Postgres ~$19) | **~$55**: 2 × 1 GB machines ~$13 + MPG $38 + egress ~$0.02/GB (~$3) |
| **VM (DigitalOcean)** | **~$7**: 1 GB droplet $6 + weekly backups 20% | **~$14**: 2 GB droplet $12 + backups | **~$15–30**: 2–4 GB droplet + backups; transfer allowance included |
| **VM (Hetzner)** | ~€5.50 (CX23) where still orderable; 2026 price rises and limited availability | ~€20 (CPX22) | ~€20–35 |

### What the price does not show

* **Render:** no servers to run; deploys, TLS, database backups and restarts are handled.
  Free web services sleep after 15 minutes, which would stop the watcher and live streams,
  so start on Starter. Everything scales by changing a plan name in `render.yaml`.
* **Fly.io:** cheap machines, but the managed database ($38) dominates small bills; an
  external database brings it down but adds a second provider. More control over regions.
* **VM:** cheapest at every size and bandwidth is bundled, but you own OS updates, Docker
  upgrades, backups (copy them off the machine), monitoring and downtime when the one
  machine reboots. Budget 1–2 hours a month. Good if you want to learn operations.

### Recommendation

1. **Start on Render with `render.yaml` as is (~$13/month).** You already have an account,
   there is nothing to operate, and scenario B is reachable by changing plan names.
2. **If traffic grows, put Cloudflare's free proxy in front** (DNS only, no code change).
   The JavaScript bundle is ~90% of the bytes and is cached as immutable, and API responses
   carry `Cache-Control: public, max-age=15`, so most traffic never reaches Render and the
   bandwidth line stays small. Live alerts still arrive at once: after each event the app
   requests fresh data under a new cache key.
3. **Move to a VM only if the Render bill passes ~$50/month** and you are happy to run it.
   Fly.io is worth it mainly if you need several regions.

## Deploying on Render

1. Push this repository to GitHub (the Blueprint reads `render.yaml` from the branch you pick).
2. Render dashboard → **New → Blueprint** → choose the repository and branch.
3. Render lists the web service, the cron job and the database, and asks for
   `RADAR_SEC_USER_AGENT` (an application name plus a contact email, required by the SEC,
   e.g. `RestatementRadar/0.1 you@example.com`). Apply.
4. The first deploy runs `radar migrate`. The dashboard is live at the `onrender.com` URL; the
   embedded watcher starts polling EDGAR straight away.
5. Load the history (2019 onwards), either:
   * **restore the dump** (minutes): copy the database's *External Database URL* from the
     Render dashboard and run, with a Postgres 16 client,
     ```bash
     pg_restore --clean --if-exists --no-owner -d "EXTERNAL_DATABASE_URL" radar-2026-11-10.dump
     ```
   * or **rebuild from EDGAR** (about an hour, from the web service's Shell tab):
     ```bash
     radar backfill 2019-01-01 && radar process && radar enrich && radar tickers
     ```

To grow: change `plan:` values in `render.yaml` (web `standard`, database `basic-1gb`),
uncomment the worker and set `RADAR_EMBEDDED_WATCHER` to `0`, then push.

## Deploying on Fly.io

Fly runs the same Docker image on its own machines; `fly.toml` in the repository already
describes the app (one always-on 512 MB machine in `iad`, health check, `radar migrate` as
the release step, the watcher embedded in the web process). The command names below are
current as far as I know but could not be checked against Fly's documentation from the
build environment; `fly <command> --help` is authoritative.

**1. Install the CLI and sign in** (Fly asks for a payment card before the first deploy).

```bash
curl -L https://fly.io/install.sh | sh
```

```bash
fly auth login
```

**2. Create the app from `fly.toml`** without deploying yet. Pick a globally unique name if
`restatement-radar` is taken (Fly updates `app =` in the file).

```bash
fly launch --no-deploy --copy-config
```

If it offers to create a Postgres database or a Redis instance, say no: the database is
chosen in step 3, and Redis is not used.

**3. Choose the database** — the biggest line on a small Fly bill.

* *Fly Managed Postgres* (~$38/month for the smallest plan): create a cluster in the same
  region and attach it to the app, which stores the connection string as the `DATABASE_URL`
  secret (the app reads it).

  ```bash
  fly mpg create
  ```

  ```bash
  fly mpg attach
  ```

* *An external Postgres* (e.g. Neon or Supabase; their free tiers fit the 152 MB database):
  create a Postgres 16 database there, then store its connection string:

  ```bash
  fly secrets set RADAR_DATABASE_URL="postgresql://USER:PASSWORD@HOST/DB?sslmode=require"
  ```

**4. Set the SEC contact** (required by EDGAR):

```bash
fly secrets set RADAR_SEC_USER_AGENT="RestatementRadar/0.1 you@example.com"
```

**5. Deploy.** Fly builds the image, runs `radar migrate` in a temporary machine, then starts
the app at `https://<app-name>.fly.dev`.

```bash
fly deploy
```

Fly may start two machines for redundancy on the first deploy. The Postgres advisory lock
keeps the embedded watcher to one copy either way; to keep the bill at one machine:

```bash
fly scale count 1
```

**6. Load the history** from the dump (minutes):

* external database: run `pg_restore` against its connection string, as in the Render
  section;
* Fly Managed Postgres: open a local tunnel to the cluster in one terminal, then restore
  through it in another (the proxy command prints the local port and credentials to use):

  ```bash
  fly mpg proxy
  ```

  ```bash
  pg_restore --clean --if-exists --no-owner -d "postgresql://USER:PASSWORD@localhost:PORT/DB" radar-2026-11-10.dump
  ```

Or rebuild from EDGAR inside the app (about an hour):

```bash
fly ssh console -C "sh -c 'radar backfill 2019-01-01 && radar process && radar enrich && radar tickers'"
```

**7. Nightly reconciliation (optional but recommended).** Fly has no cron section in
`fly.toml`; a scheduled machine running the app's image does the same job:

```bash
fly machine run registry.fly.io/<app-name>:latest --schedule daily --region iad --vm-memory 512 -- radar reconcile --days 3
```

Scheduled machines inherit the app's secrets. Check `fly machine run --help` for the exact
flags in your CLI version.

**8. Custom domain (optional).** Point a CNAME at `<app-name>.fly.dev`, then:

```bash
fly certs add radar.example.com
```

**Operating it**

| Task | Command |
|---|---|
| Live logs | `fly logs` |
| Status and machines | `fly status` |
| Shell inside the app | `fly ssh console` |
| More memory | `fly scale memory 1024` |
| More machines | `fly scale count 2` |
| Separate watcher | add `[processes]` with `web` and `watcher` entries, set `RADAR_EMBEDDED_WATCHER = "0"` |

## Deploying on a VM

```bash
cp .env.example .env
```

Edit `.env` (User-Agent, database password) and the host name in `deploy/vm/Caddyfile`, point
the domain's DNS at the VM, then:

```bash
docker compose -f docker-compose.yml -f deploy/vm/docker-compose.vm.yml up -d --build
```

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `RADAR_DATABASE_URL` | `DATABASE_URL`, else local | Postgres connection string |
| `RADAR_SEC_USER_AGENT` | — (required) | application name + contact email for SEC EDGAR |
| `RADAR_EMBEDDED_WATCHER` | `0` | `1`: the web process polls EDGAR (one instance wins a Postgres advisory lock) |
| `RADAR_SEC_RATE` | `5` | EDGAR requests per second per process (SEC maximum 10 across everything) |
| `RADAR_DB_POOL` | `10` | database connections per web process |
| `RADAR_API_CACHE_SECONDS` | `15` | `Cache-Control` max-age on API reads |
| `RADAR_STREAM_MAX_SECONDS` | `300` | live streams recycle after this; browsers reconnect without losing events |
| `RADAR_RAW_DIR` | `~/.cache/restatement-radar/raw` | where downloaded filings are kept |
