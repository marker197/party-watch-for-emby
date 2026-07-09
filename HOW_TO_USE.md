# How to Use Emby-Trakt Suite

Open your browser and go to `http://your-server-address:8000` to reach the dashboard. Everything below is done from there.

---

## The Dashboard

The dashboard is your home screen. It shows cards for each feature. Click any card to expand it and see more detail.

---

## Smart Watch Queue

This is your personalised "what to watch next" list. The suite looks at your Trakt watchlist, what's trending, and your rating history, then ranks everything and builds a playlist inside Emby.

**How to use it:**

1. On the dashboard, find the **Smart Queue** card
2. Click **Refresh Queue** if you want to update it right now (it also refreshes automatically every night)
3. The queue shows a ranked list of movies and shows
4. Each item shows why it was recommended (from your watchlist, trending, etc.)
5. Open Emby and look for a playlist called **Smart Up Next** — it's the same list, ready to play

**Missing something from the queue?**

If a movie on your queue isn't in your Emby library yet, you'll see a red **"Not in Library"** badge. If you have Radarr set up, click the purple **Radarr** button next to it to send it to Radarr for downloading. Same for TV shows if you have Sonarr set up — click the purple **Sonarr** button.

---

## Airing Soon

Shows you what's coming up in the next 14 days.

**What it shows:**

- **TV shows** already in your library that have new episodes airing soon
  - Episodes tagged as a **Premiere** (first episode of a new season) get a special badge
  - Episodes tagged as a **Finale** (last episode of the season) get a special badge
- **Movies** from your Trakt watchlist that are about to be released
  - These show whether they're already in your library or not

**How to use it:**

1. On the dashboard, find the **Airing Soon** card
2. Click it to expand and see the full list
3. Shows are sorted by air date — closest first

---

## ML Rating Predictor

The suite learns from every rating you've given on Trakt, then predicts how much you'd enjoy the unwatched movies and shows in your Emby library.

**How to use it:**

1. On the dashboard, find the **Predictions** card
2. Click **View Predictions** to see the full list
3. Each item shows:
   - A predicted rating out of 10
   - A confidence level (how sure the model is)
   - An explanation of what drove the prediction (for example: "sci-fi genre, Christopher Nolan, 2010s era")
4. The more you rate on Trakt, the smarter the predictions get — it retrains itself every Monday

**First time?**

You need at least a handful of ratings on Trakt for predictions to work. If you've never rated anything, go to trakt.tv and rate some movies and shows you've already watched, then come back.

---

## Shared Universe Discovery

Finds franchises and cinematic universes in your library and puts them in the correct watch order.

**How to use it:**

1. On the dashboard, find the **Universes** card
2. Click **View Universes** to see what was found
3. Each universe shows:
   - The full watch order
   - Which items you've already watched (with a tick)
   - Which items are in your library vs missing
   - Your completion percentage (for example: "34 of 51 watched")
   - **Next recommended** — the next thing you should watch in the sequence
4. Universes rescan every Sunday night, or you can trigger a rescan from the settings page

---

## Watch Party

Watch a movie or show with friends in sync — play, pause, and reactions are shared across all screens.

**Starting a party (you're the host):**

1. On the dashboard, find the **Watch Party** card
2. Click **Create Party**
3. Pick what you want to watch (search your Emby library)
4. You'll get a party code — a short code like `ABCD`
5. Share two things with your friends:
   - The **party code** (e.g. `ABCD`)
   - The **watch party link** — this is your server's address with `/party` at the end, for example `http://your-address:8000/party`
   
   If your friends are outside your home network, they can't use your local address (the `192.168...` one). They need your external/public IP address or, better yet, set up **Tailscale** (or a similar tool like ZeroTier or WireGuard) so they can reach your server securely from anywhere. With Tailscale, each device gets its own address that works from any network — share that address instead

**Joining a party (you're a guest):**

1. On the dashboard, find the **Watch Party** card
2. Click **Join Party**
3. Type in the code the host gave you
4. Choose the Emby username you're signed in with on your device
5. Make sure the Emby app is open and in the foreground on your device (phone, Android TV, Shield, Apple TV, etc.) — it won't receive playback commands if it's in the background or closed
6. You'll be connected to the party

**During the party:**

- Playback starts on all screens at the same time
- If anyone pauses, it pauses for everyone
- Click the emoji buttons to send reactions (everyone sees them in real-time)
- Reactions are saved and counted

**When the party ends:**

- The host ends the party from the dashboard
- A summary of all reactions gets posted as a comment on Trakt (for example: "Watched with 3 people. Reactions: 😂 x4, 😱 x2")

---

## Settings

Click the **gear icon** in the top-right corner of the dashboard to reach settings.

**What you can do in settings:**

- **Rebuild Library Cache** — re-scans your Emby library. Do this if you've added a lot of new content and don't want to wait for the nightly scan
- **Radarr Servers** — connect one or two Radarr instances so you can send missing movies to be downloaded
- **Sonarr Servers** — connect one or two Sonarr instances so you can send missing TV shows to be downloaded
- **Connection Status** — see at a glance whether your Emby, Trakt, Radarr, and Sonarr connections are all working (green = good)

---

## Setting Up Emby Webhooks (Optional)

This step lets the suite know whenever you watch something in Emby, so it can automatically update your queue and track which recommendations you actually watched.

1. Open your Emby server in a browser
2. Go to **Settings** (gear icon)
3. Find **Webhooks** (it may be under Notifications, depending on your Emby version)
4. Click **Add Webhook**
5. For the URL, type: `http://your-server-address:8000/webhook/emby`
   (Use the same address as your suite dashboard, with `/webhook/emby` at the end)
6. For events, tick **Playback Stop** and **Item Marked Played**
7. Save

Now whenever you finish watching something in Emby, the suite knows about it right away.

---

## Tips

- **Rate things on Trakt.** The more you rate, the better the predictions and queue rankings get.
- **Check the dashboard once a week.** The queue and predictions refresh overnight, so there's always something new.
- **Don't worry about the schedules.** Everything important runs automatically overnight. You only need to click "Refresh" buttons if you want instant results.
- **Universes take time.** The first universe scan can take a while if you have a large library. Let it finish overnight.
