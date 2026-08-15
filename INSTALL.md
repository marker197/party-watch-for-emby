# How to Install Emby-Simkl Suite

This guide assumes you have never done anything like this before. Every step is done through a web browser — no typing commands into a black screen.

---

## What You Need Before You Start

- A Synology NAS with **Container Manager** installed (or any machine with Docker)
- An **Emby** media server already running on your network
- A **Simkl** account at simkl.com (free)
- Optionally, an **MDBList** account at mdblist.com (free)

---

## Step 1 — Get Your Emby API Key

1. Open your Emby server in a web browser (the same page you use to browse your movies)
2. Click the **gear icon** (Settings) in the top-right corner
3. In the left sidebar, scroll down and click **API Keys** (under the Advanced section)
4. Click **New API Key**
5. Give it a name like `Simkl Suite`
6. Click **OK**
7. Copy the long string of letters and numbers — that's your API key
8. Also note your Emby server's address from the browser bar (something like `http://192.168.1.100:8096`)

---

## Step 2 — Get Your Simkl Client ID

1. Go to **simkl.com/apps/new** and sign in
2. Fill in the form:
   - **Name**: `Emby-Simkl-Suite`
   - **Redirect URI**: `http://localhost:8000/auth/callback`
3. Click **Save**
4. Copy the **Client ID** somewhere safe — you'll need it during setup

If you also want MDBList integration, go to **mdblist.com** → your profile → API and copy your API key.

---

## Step 3 — Deploy on Synology Container Manager

No `.env` file is needed. The suite configures itself through a setup wizard on first launch.

1. Open **Container Manager** on your Synology
2. Go to the **Project** section in the left sidebar
3. Click **Create**
4. Give it a name like `emby-simkl-suite`
5. For the source, choose **Upload docker-compose.yml** and select the `docker-compose.yml` file from the archive you downloaded
6. Click **Next**, review the summary, and click **Done**

Container Manager will download the required images and start everything up. This takes a few minutes the first time. When all three containers show green "Running" status, the suite is ready.

---

## Step 4 — Run the Setup Wizard

1. Open your browser and go to `http://your-synology-ip:8000`
2. The setup wizard will appear automatically on first run

**Step 1 of 3 — Emby Connection**
- Enter your Emby server URL (e.g., `http://192.168.1.100:8096`) — make sure to include `http://`
- Enter your Emby API key
- Click **Test connection** — you should see the server name and version
- A user dropdown will appear — select the Emby account whose library and watch history the suite should use (usually the admin account)
- Click **Next**

**Step 2 of 3 — Integration Provider**
- Choose your tracking provider: Simkl, MDBList, Both, or None
- Enter your Simkl Client ID (and/or MDBList API key depending on your choice)
- Click **Next**

**Step 3 of 3 — Confirm and Save**
- Review the summary
- Click **Save & launch**

You'll be taken to the dashboard.

---

## Step 5 — First-Time Setup (Do These In Order)

After the wizard completes, open **Settings** from the sidebar and work through these steps in order. Each one builds on the previous.

### 5.1 — Link Your Simkl Account

1. In Settings, find the **Simkl Configuration** section
2. Click **Link Simkl Account**
3. You'll be given a code and a link to Simkl's website
4. Open the link, enter the code, and approve the connection
5. Return to Settings — you should see a green "Connected" badge

### 5.2 — Rebuild Library Cache

1. In Settings, find the **Library Cache** section (or use the dashboard card)
2. Click **Rebuild Cache**
3. Wait for it to complete — the bigger your library, the longer this takes
4. This indexes your entire Emby library so the suite knows what you have

### 5.3 — Import Watch History

1. In Settings, find the **Watch History** section
2. Click **Sync Watch History**
3. This pulls your complete watch history from Simkl, MDBList, and Emby and merges it into the local database
4. Wait for it to complete — first import may take a minute

If you're migrating from a previous Trakt-based setup, you can also import a Trakt data export:
1. Scroll to the **Trakt Data Import** section
2. Upload your Trakt export zip file (download it from trakt.tv → Settings → Export)
3. The suite will parse the zip and let you review before importing

### 5.4 — Backfill Genres

1. Still in the Watch History section, click **Backfill Genres**
2. This enriches your watch history with genre metadata from Emby, which powers the stats page, taste profile, and ML predictions

### 5.5 — Run Scrobble Audit

1. Go to the **Scrobble Audit** page from the sidebar (under Tools)
2. Click **Run Audit**
3. This compares what Emby says you've watched against what Simkl/MDBList knows about
4. Any missed scrobbles will appear with a **Backfill** button to sync them
5. Click **Backfill All** to push everything across

### 5.6 — Train the ML Model

1. Go to the **Predictions** page from the sidebar
2. Click **Train Model**
3. This trains a machine learning model on your rating history to predict how you'd rate unwatched items
4. You need at least 15 ratings on Simkl for this to work

### 5.7 — Run Bias Analysis

1. Go to the **Taste Profile** page from the sidebar
2. Click **Run Analysis**
3. This analyses your rating patterns by genre, era, and more
4. After running, you'll see your genre insights, era breakdown, hidden gems, and taste challenges

---

## Step 6 — Optional Setup

These are all in Settings and can be done in any order.

### Radarr / Sonarr Integration
Connect your Radarr and Sonarr instances so you can send missing movies and shows directly from the Smart Queue and Library Health pages. Enter the URL and API key for each server, click **Test**, then **Save**.

### SABnzbd Integration
Connect your SABnzbd download client to see download queue status on the dashboard. Enter the URL and API key (find it in SABnzbd → Config → General), click **Test**, then **Save**.

### Watchlist Sync
Enable two-way sync between your Simkl/MDBList watchlist and Radarr/Sonarr. When you add something to your watchlist, it automatically gets sent to Radarr or Sonarr for download.

### TMDB API
Add a TMDB API key for enhanced metadata (posters, detailed info on the item detail page). Get a free key at themoviedb.org.

### Notifications
Set up Discord webhooks, Gotify, or custom webhook endpoints to get alerts when scrobbles happen, downloads complete, or the ML model finishes training. You can configure up to 5 notification services and choose which events trigger each one.

### Emby Webhook
To enable real-time scrobbling (instead of relying on scheduled syncs):
1. In Emby, go to Settings → Webhooks (or Notifications → Webhooks on older versions)
2. Add a webhook:
   - **URL**: `http://your-synology-ip:8000/webhook/emby`
   - **Events**: Playback Stop, Item Marked Played
3. Now when you finish watching something, the suite is notified immediately

---

## Step 7 — You're Done

Everything is now running. The suite will automatically:

- Rebuild the library cache every night at 1:30 AM
- Refresh the Smart Watch Queue every night at 2:00 AM
- Sync your watchlist daily at 2:30 AM
- Scan for shared universe playlists every Sunday at 3:00 AM
- Retrain the ML rating predictor every Monday at 4:00 AM
- Run bias analysis every Monday at 5:00 AM

All times are UTC. You can change them in Settings → Scheduler Configuration.

Go to `http://your-synology-ip:8000` to see your dashboard.

---

## If Something Goes Wrong

**The page won't load at all**
Check Container Manager — are all three containers showing green / Running? If any are stopped, click on the container and click Start.

**"Cannot connect to Emby" on the dashboard**
Double-check the Emby URL — it must include `http://` (e.g., `http://192.168.1.100:8096`, not `192.168.1.100:8096`). Make sure the API key has no extra spaces. Restart the project in Container Manager.

**Simkl shows "Not linked"**
Go to Settings → Simkl Configuration and click Link Simkl Account to go through the device code flow again.

**Watch history is empty**
Make sure you've done step 5.3 (Sync Watch History). The suite doesn't automatically import your full history — you trigger it once, then ongoing scrobbles are tracked automatically.

**Stats page shows no genre data**
Run the genre backfill (step 5.4) and then run the bias analysis (step 5.7). The genre data comes from enriching your watch history with Emby metadata, and the analysis runs on top of that.

**Predictions page shows nothing**
Click Train Model on the predictions page. You need at least 15 ratings on Simkl for the ML model to train.

**SABnzbd says "API key incorrect"**
Make sure you copied the API key from SABnzbd → Config → General (not the URL). The API key is a long string of letters and numbers, not an IP address.

**Need to start completely fresh**
In Container Manager, stop the project, delete it, then delete the Docker volumes via SSH:
```
sudo docker volume ls | grep postgres
sudo docker volume rm <volume_name>
```
Then create the project again from Step 3. Note: Synology's Container Manager does not delete Docker volumes when you delete a project — you must remove them manually via SSH if you want a truly clean start.

**Enable debug logging**
Go to Settings → Debug Logging and flip the toggle. This shows verbose logs including all API calls and internal state, which is helpful for troubleshooting. Turn it off when you're done — debug mode produces a lot of log output.
