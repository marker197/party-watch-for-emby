# How to Install Emby-Trakt Suite

This guide assumes you have never done anything like this before. Every step is done through a web browser — no typing commands into a black screen.

---

## Step 1 — Get Your Trakt Credentials

You need two pieces of information from Trakt before you start.

1. Open your web browser and go to **trakt.tv/oauth/applications**
2. Sign in to your Trakt account
3. Click **New Application**
4. Fill in the form:
   - **Name**: type `Emby-Trakt-Suite`
   - **Redirect URI**: type `http://localhost:8000/auth/callback`
     (If your server has a different address, replace `localhost` with that address)
   - Leave everything else as-is
5. Click **Save App**
6. You'll see two values: **Client ID** and **Client Secret**
7. Copy both of these somewhere safe — a text file, a note on your phone, anywhere you can get them back from in a minute

---

## Step 2 — Get Your Emby API Key

1. Open your Emby server in a web browser (the same page you use to browse your movies)
2. Click the **gear icon** (Settings) in the top-right corner
3. In the left sidebar, scroll down and click **API Keys** (it's under the Advanced section)
4. Click **New API Key**
5. Give it a name like `Trakt Suite`
6. Click **OK**
7. Copy the long string of letters and numbers that appears — that's your API key
8. Also make a note of your Emby server's address (the URL in your browser bar, something like `http://192.168.1.100:8096`)

---

## Step 3 — Create the Configuration File

You need to create a small text file called `.env` that tells the suite how to connect to everything.

1. Open any text editor on your computer (Notepad on Windows, TextEdit on Mac)
2. Copy and paste everything below into it:

```
TRAKT_CLIENT_ID=paste_your_trakt_client_id_here
TRAKT_CLIENT_SECRET=paste_your_trakt_client_secret_here

EMBY_URL=http://192.168.1.100:8096
EMBY_API_KEY=paste_your_emby_api_key_here

DB_USER=embytrakt
DB_PASSWORD=pick_any_password_you_like
DB_NAME=embytrakt

ENABLE_SMART_QUEUE=true
ENABLE_ML_PREDICTOR=true
ENABLE_UNIVERSE_DISCOVERY=true
ENABLE_WATCH_PARTY=true

SMART_QUEUE_CRON=0 2 * * *
UNIVERSE_SCAN_CRON=0 3 * * 0
ML_RETRAIN_CRON=0 4 * * 1

SUITE_PORT=8000
WS_PORT=8001

LOG_LEVEL=INFO
```

3. Replace the placeholder values:
   - Replace `paste_your_trakt_client_id_here` with the Client ID you copied from Trakt
   - Replace `paste_your_trakt_client_secret_here` with the Client Secret from Trakt
   - Replace `http://192.168.1.100:8096` with your actual Emby address
   - Replace `paste_your_emby_api_key_here` with the API key you created in Emby
   - Replace `pick_any_password_you_like` with any password (this is just for the internal database — you won't need to type it again)
4. Save the file with the name `.env` (make sure it's not saved as `.env.txt` — the name must be exactly `.env`)

---

## Step 4 — Set Up on Synology Container Manager

1. Open **Container Manager** on your Synology (find it in the main menu)
2. Go to the **Project** section in the left sidebar
3. Click **Create**
4. Give it a name like `emby-trakt-suite`
5. For the source, choose **Upload docker-compose.yml** and select the `docker-compose.yml` file from the archive you downloaded
6. When it asks about environment variables, upload or paste the `.env` file you created in Step 3
7. Click **Next**, review the summary, and click **Done**

Container Manager will now download the required pieces and start everything up. This may take a few minutes the first time.

When all three containers show a green "Running" status, the suite is up.

---

## Step 5 — Link Your Trakt Account

1. Open your web browser
2. Go to `http://your-server-address:8000/link`
   (Replace `your-server-address` with your Synology's IP address — the same kind of address you use for Emby, just with `:8000` at the end instead of `:8096`)
3. Click **Get Device Code**
4. A short code and a web link will appear
5. Open that web link in a new tab — it goes to Trakt's website
6. Type in the code shown on your screen
7. Trakt will ask you to allow the connection — click **Yes** / **Allow**
8. Go back to the suite tab and click **Poll** — it checks every few seconds until Trakt confirms
9. Once you see a green "Linked" message, you're connected

---

## Step 6 — Build the Library Cache

The suite needs to scan your Emby library once so it knows what you have. This happens from the dashboard.

1. Go to `http://your-server-address:8000` in your browser
2. Look for the **Settings** link (gear icon) in the top-right corner
3. In Settings, find the **Library Cache** section
4. Click **Rebuild Cache**
5. Wait a few minutes — the bigger your library, the longer this takes
6. When the status changes to show a number of cached items, it's done

---

## Step 7 — You're Done

Everything is now running. The suite will automatically:

- Rebuild your library cache every night at 1:30 AM
- Refresh your Smart Watch Queue every night at 2:00 AM
- Scan for shared universes every Sunday at 3:00 AM
- Retrain the rating predictor every Monday at 4:00 AM

(All times are UTC. You can change them in the `.env` file if you'd like different times.)

Go to `http://your-server-address:8000` to see your dashboard.

---

## If Something Goes Wrong

**The page won't load at all**
- Check Container Manager — are all three containers showing green / "Running"?
- If any are red or stopped, click on the stopped container, then click **Start**

**"Cannot connect to Emby" on the dashboard**
- Double-check the Emby address in your `.env` file — it should match exactly what you type into your browser to reach Emby
- Make sure the API key is correct (no extra spaces)
- Restart the project in Container Manager (stop, then start)

**"Cannot connect to Trakt" on the dashboard**
- Go through Step 5 again to re-link your account
- Check that the Client ID and Client Secret in your `.env` file match what Trakt shows

**The Smart Queue is empty**
- Make sure you've built the library cache first (Step 6)
- Make sure you have items on your Trakt watchlist
- Click the "Refresh Queue" button on the dashboard

**Need to start fresh**
- In Container Manager, stop the project
- Delete the project
- Go through Step 4 again from scratch
- Note: this will erase all your prediction history and party data
