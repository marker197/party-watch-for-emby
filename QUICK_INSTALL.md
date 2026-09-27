# Quick Install Guide

**Time to deploy**: ~10 minutes

## Prerequisites

- Docker + Docker Compose
- Emby server running (you'll need the URL and an API key)
- Simkl.tv account (free) — for scrobbling and watch history

## 1. Get Credentials

### Emby API Key
1. Open Emby web UI
2. Settings → Advanced → API Key → Generate new key
3. Note the key and your server URL (e.g. `http://192.168.1.100:8096`)

### Simkl V2 Client ID
1. Go to [simkl.com/settings/developer](https://simkl.com/settings/developer/)
2. Create a new app (name it anything, e.g. "Emby Suite")
3. Copy the **Client ID** — you don't need a Client Secret for V2 auth

## 2. Start Docker

```bash
cd emby-simkl-suite
docker-compose up -d
```

No `.env` file needed. The `docker-compose.yml` has sensible defaults for PostgreSQL and Redis. All integration credentials are collected through the web-based setup wizard.

Wait 30 seconds for services to be healthy:
```bash
docker-compose ps
# All three services should show "healthy" or "running"
```

## 3. Setup Wizard

Open `http://YOUR-IP:8000` in a browser. On first run you'll see the setup wizard:

1. **Emby connection** — enter your server URL and API key, click Test Connection. Pick your primary user from the dropdown.
2. **Provider** — choose Simkl (recommended), MDBList, Both, or None. Enter your Simkl V2 Client ID if using Simkl.
3. **Review** — confirm and click Save & Launch.

The suite saves everything and redirects to the dashboard.

## 4. Link Simkl Account

On the dashboard, scroll to **Link Simkl Account**:

1. Enter your Emby User ID and username
2. Click **Get Code**
3. Go to the verification URL shown and enter the user code on Simkl's site
4. The suite polls automatically — once authorised, you'll see "Linked as [username]" ✅

Tokens use Simkl V2 OAuth2 (device-code flow with `media:read media:write` scope). Access tokens last 7 days and auto-refresh using a 180-day rolling refresh token.

## 5. Set Up Emby Webhook

1. Emby web UI → **Settings → Notifications → Webhooks**
2. Add webhook:
   - **URL**: `http://YOUR-SUITE-IP:8000/webhook/emby`
   - **Events**: Playback Start, Pause, Unpause, Stop, Mark Played, New Media Added, Media Removed
3. Test by playing something briefly — the activity log on the dashboard should show the event

## 6. Build Library Cache

Click **Rebuild Cache** on the dashboard. Takes 2–5 minutes depending on library size. This indexes your Emby library into Redis so all features can look up items without calling the Emby API every time.

The cache rebuilds nightly at 1:30 AM UTC automatically.

## 7. You're Running

Everything else is optional. The dashboard shows feature cards you can expand — Smart Queue, ML Predictor, Universe Discovery, etc. Each has a button to trigger a manual run.

### Optional next steps

- **Radarr / Sonarr** — configure in Settings for "Send to Radarr/Sonarr" buttons and watchlist sync
- **SABnzbd** — configure in Settings for real-time download progress
- **TMDB API key** — enter in Settings for streaming logos and universe auto-discovery
- **Sonarr webhook** — `http://YOUR-SUITE-IP:8000/webhook/sonarr/` for episode import badges on Airing Soon
- **Notifications** — Discord, Gotify, or webhook alerts in Settings

## Scheduled Jobs

All times UTC, configurable from Settings → Schedules:

| Job | Default | Description |
|-----|---------|-------------|
| Library cache rebuild | Daily 1:30 AM | Re-indexes Emby library |
| Smart Queue refresh | Daily 2:00 AM | Rebuilds watch queue |
| Watchlist sync | Every 6 hours | Radarr/Sonarr ↔ Simkl/MDBList sync |
| Library Health scan | Weekly Wed 4:30 AM | Finds library gaps |
| Universe scan | Weekly Sun 3:00 AM | Matches shared universes |
| ML retrain | Weekly Mon 4:00 AM | Retrains rating predictor |

## Simkl API Limits

The suite tracks daily API calls and pauses background jobs when approaching the limit:

| Tier | Daily Limit |
|------|------------|
| Free | 500 |
| VIP | 1,000 |
| VIP+ | 10,000 |

The Settings page shows a live usage meter. Resets at midnight US Eastern (5 AM UK). Search results and metadata are cached in Redis to minimise repeat calls.

## Troubleshooting

### Can't connect to Emby
```bash
docker-compose exec emby-simkl-suite curl -s "http://host.docker.internal:8096/System/Info/Public"
```
The compose file maps `host.docker.internal` to the host. Use this hostname if Emby runs on the same machine.

### Cache empty after rebuild
```bash
docker-compose exec redis redis-cli KEYS "library:*" | wc -l
```
Should be > 0. If 0, check Emby connection in Settings.

### Simkl 403 errors after linking
You need to re-link if your token was issued before the V2 scope fix. Go to Dashboard → Link Simkl Account and link again. The new token includes `media:read media:write` scope.

### Simkl daily limit hit
Background jobs pause automatically. The Settings page shows usage and time until reset. Reduce the watchlist sync interval in Settings → Schedules if you're hitting the limit regularly.

### Container logs
```bash
docker-compose logs -f emby-simkl-suite
```

### DB migration failed
Migrations run automatically on container start and are idempotent. Check logs:
```bash
docker-compose logs emby-simkl-suite | grep -i "alembic"
```

### Nuclear option (destroys all data)
```bash
docker-compose down
docker volume rm emby-simkl-suite_postgres-data
docker-compose up -d
```
Back up first — use Settings → Database Backup → Create Backup.
