# Emby-Simkl Suite

A self-hosted media management suite that connects your Emby media server with Simkl, MDBList, Radarr, Sonarr, and SABnzbd. Runs in Docker.

## What It Does

- **Smart Watch Queue** — builds a personalised "what to watch next" list from your Simkl watchlist, trending content, and calendar data, cross-referenced against your Emby library
- **ML Rating Predictor** — trains on your Simkl rating history and predicts scores for every unwatched item
- **Universe Discovery** — detects shared cinematic universes (MCU, Star Wars, etc.) and creates ordered Emby playlists
- **Watch Party** — synchronised playback across Emby devices with real-time chat and reactions
- **Scrobble & Sync** — automatic watch history tracking to Simkl and MDBList when you play something in Emby
- **Library Health** — finds gaps in your library: incomplete series, watched items not in Emby, related movies you're missing
- **Airing Soon** — upcoming episodes and movies from Sonarr/Radarr calendars with premiere/finale flags
- **Taste Profile** — rating bias analysis, genre breakdowns, hidden gem discovery
- **Watch History** — permanent local record of every watch with filtering, stats, and ratings
- **Rewatch Recommender** — suggests items worth watching again based on your history and ratings
- **Watchlist Sync** — bi-directional sync between Radarr/Sonarr and your Simkl/MDBList watchlist
- **Notifications** — alerts to Discord, Gotify, or any webhook for scrobbles, arrivals, premieres, and downloads

## Stack

- **Backend**: Python 3.11 / FastAPI / SQLAlchemy (async) / APScheduler
- **Frontend**: Server-rendered HTML + vanilla JS (Jinja2 templates)
- **Database**: PostgreSQL 16
- **Cache**: Redis 7
- **ML**: scikit-learn / pandas / numpy
- **Real-time**: python-socketio (WebSocket for Watch Party)
- **Container**: Docker + Docker Compose

## Prerequisites

- Docker and Docker Compose
- Emby server running (you'll need the URL and an API key)
- A Simkl account (free) — for scrobbling, watchlists, and ratings
- Optional: MDBList account, Radarr, Sonarr, SABnzbd, TMDB API key

## Quick Start

### 1. Clone and start

```bash
git clone <repo-url> emby-simkl-suite
cd emby-simkl-suite
docker-compose up -d
```

No `.env` file needed — the `docker-compose.yml` has sensible defaults for the database and Redis. All credentials are collected through the web-based setup wizard.

Wait 30 seconds for services to start, then check:

```bash
docker-compose ps
# All three services should show "healthy" or "running"
```

### 2. First-run setup wizard

Open `http://YOUR-IP:8000` in a browser. On first run you'll see the setup wizard.

**Step 1 — Emby connection**

Enter your Emby server URL (e.g. `http://192.168.1.100:8096`) and API key. Click **Test Connection** — the wizard fetches your Emby users and populates a dropdown. Pick your primary user.

To generate an Emby API key: Emby web UI → Settings → Advanced → API Key → Generate.

**Step 2 — Integration provider**

Choose which services the suite uses for scrobbling and watch history:

| Option | What it does |
|--------|-------------|
| **Simkl** (recommended) | All scrobbling, history, ratings, and watchlist via Simkl |
| **MDBList** | Uses MDBList instead |
| **Both** | Scrobbles to both services simultaneously |
| **None** | No external scrobbling — local features only |

If you chose Simkl or Both, enter your **Simkl V2 Client ID**. Get one from [simkl.com/settings/developer](https://simkl.com/settings/developer/) → Create new app → copy the Client ID. (You don't need a Client Secret for V2 auth.)

If you chose MDBList or Both, enter your MDBList Client ID and Secret.

**Step 3 — Review and launch**

Confirm your settings and click **Save & Launch**. The suite saves everything and redirects to the dashboard.

### 3. Link your Simkl account

On the dashboard, scroll to the **Link Simkl Account** section:

1. Enter your Emby User ID and username
2. Click **Get Code** — the suite requests a device code from Simkl
3. Go to the verification URL shown and enter the user code
4. The suite polls automatically — once Simkl confirms, you'll see "Linked as [your username]" ✅

Tokens are V2 OAuth2 (7-day access tokens with 180-day rolling refresh). The suite auto-refreshes tokens before they expire.

### 4. Emby webhook

The suite needs Emby to push playback events for scrobbling, activity logging, and the queue feedback loop.

1. Emby web UI → **Settings → Notifications → Webhooks** (or Server → Webhooks)
2. Add a new webhook:
   - **URL**: `http://YOUR-SUITE-IP:8000/webhook/emby`
   - **Events**: Playback Start, Pause, Unpause, Stop, Mark Played, New Media Added, Media Removed
3. Test by playing something briefly — the activity log on the dashboard should show the event

### 5. Build the library cache

Click **Rebuild Cache** on the dashboard. This indexes your Emby library into Redis (takes 2–5 minutes depending on library size). The cache rebuilds nightly but the first build needs a manual trigger.

### 6. Optional integrations (Settings page)

All configured from the Settings page — no `.env` editing needed:

- **Radarr / Sonarr** — up to 2 servers each. Enables "Send to Radarr/Sonarr" buttons throughout the UI and watchlist sync.
- **SABnzbd** — up to 2 servers. Gives real-time download progress on the Active Downloads card.
- **TMDB API key** — enables streaming service logos on Airing Soon items and auto-discovery of universes.
- **Sonarr webhook** — Sonarr → Settings → Connect → Webhook → URL: `http://YOUR-SUITE-IP:8000/webhook/sonarr/` — adds "Imported" badges to Airing Soon.
- **Notifications** — Discord, Gotify, or webhook endpoints for scrobble, arrival, premiere, download, prediction, and system events.

## Scheduled Background Jobs

All times are UTC and configurable from the Settings page:

| Job | Default Schedule | What it does |
|-----|-----------------|-------------|
| Library cache rebuild | Daily 1:30 AM | Re-indexes Emby library into Redis |
| Smart Queue refresh | Daily 2:00 AM | Rebuilds the watch queue |
| Watchlist sync | Every 6 hours | Bi-directional Radarr/Sonarr ↔ watchlist sync |
| Library Health scan | Weekly Wed 4:30 AM | Finds library gaps and missing items |
| Universe scan | Weekly Sun 3:00 AM | Matches and resolves shared universes |
| ML model retrain | Weekly Mon 4:00 AM | Retrains the rating predictor |
| Bias analysis | Weekly Tue 5:00 AM | Updates taste profile data |

## Simkl API Usage

The suite tracks daily Simkl API calls and respects the per-account limit:

| Account Tier | Daily Limit |
|-------------|------------|
| Free | 500 |
| VIP | 1,000 |
| VIP+ | 10,000 |

A built-in circuit breaker stops all calls when the daily limit is hit and resumes after the reset (midnight US Eastern / 5 AM UK). Background jobs reserve 25% of the daily budget for interactive page use. The Settings page shows a live usage meter.

Search results and metadata lookups are cached in Redis (7–30 days) to minimise repeat calls. Cloudflare-cached Simkl endpoints (numeric ID lookups) don't count against the daily limit.

## Project Structure

```
emby-simkl-suite/
├── app/
│   ├── api/              # FastAPI route modules
│   ├── models/           # SQLAlchemy models
│   ├── services/         # Business logic
│   │   ├── smart_queue/
│   │   ├── ml_predictor/
│   │   ├── universe_discovery/
│   │   ├── watch_party/
│   │   ├── scrobble_audit/
│   │   ├── rating_bias_detector/
│   │   ├── rewatch/
│   │   ├── watch_stats/
│   │   ├── watchlist_sync/
│   │   ├── airing_alerts/
│   │   ├── monitoring/
│   │   └── library_health_service.py
│   ├── utils/            # API clients (Emby, Simkl, MDBList, Radarr, Sonarr, etc.)
│   ├── middleware/        # Rate limiting
│   ├── security/          # JWT auth
│   └── main.py           # App entrypoint + scheduler
├── frontend/
│   ├── templates/         # Jinja2 HTML pages
│   └── static/            # CSS + JS
├── alembic/               # Database migrations
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## Troubleshooting

**Can't connect to Emby from inside the container:**
```bash
docker-compose exec emby-simkl-suite curl -s "http://host.docker.internal:8096/System/Info/Public"
```
The `docker-compose.yml` maps `host.docker.internal` to the host machine. Use this hostname if Emby runs on the same machine.

**Library cache empty after rebuild:**
```bash
docker-compose exec redis redis-cli KEYS "library:*" | wc -l
```
Should return > 0. If 0, check Emby connection in Settings.

**Simkl daily limit hit:**
The Settings page shows current usage. Background jobs pause automatically. Interactive pages may show errors — wait until the reset (midnight US Eastern) or reduce the watchlist sync interval.

**Database migration failed on startup:**
```bash
docker-compose logs emby-simkl-suite | grep -i "alembic\|migration"
```
Migrations are idempotent and auto-run on container start. If stuck, check the logs for the specific error.

**Container logs:**
```bash
docker-compose logs -f emby-simkl-suite
```

## Pages

| Page | URL | Description |
|------|-----|-------------|
| Dashboard | `/` | Main hub — status, features, activity log |
| Lists | `/universes` | Shared universes and custom collections |
| Predictions | `/predictions` | ML rating predictions for unwatched items |
| Taste Profile | `/bias` | Rating patterns, genre analysis, hidden gems |
| Rewatch | `/rewatch` | Rewatch recommendations |
| Stats | `/stats` | Viewing statistics and trends |
| Watch History | `/watch-history` | Full watch history with filters |
| Library Health | `/library-health` | Library gap analysis |
| Settings | `/settings` | All configuration |
| Watch Party | `/watch-party` | Synchronised group viewing |
| User Guide | `/guide` | In-app documentation |
