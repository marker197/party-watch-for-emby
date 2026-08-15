# Emby-Simkl / MDblist Suite

A self-hosted app that connects your Emby media server to your Trakt.tv account. It helps you decide what to watch next, predicts what you'll enjoy, organises movie universes, and lets you watch with friends.

---

## What Does It Do?

### Smart Watch Queue

Looks at your Simkl/MDBList watchlists, trending shows, and what's popular, then builds a "what to watch next" playlist inside Emby. It updates itself every night so there's always something ready.

If a show or movie on your queue isn't in your Emby library yet, you can send it straight to Radarr (for movies) or Sonarr (for TV shows) to be downloaded — one button press from the dashboard.

### Airing Soon

Shows you what's coming up in the next two weeks for shows already in your library. Flags season premieres and finales so you don't miss anything. Also shows movies from your Trakt watchlist that are about to be released.

### ML Rating Predictor

Learns from everything you've rated on Trakt, then predicts how much you'd enjoy the unwatched movies and shows sitting in your Emby library. Each prediction comes with an explanation of why it thinks you'll like (or dislike) something.

### Shared Universe Discovery

Finds movie and TV universes in your library (Marvel, Star Wars, etc.) and puts them in the right watch order. Tracks your progress through each universe and tells you what to watch next.

### Watch Party

Start a watch party, share a code with friends, and everyone watches together in sync. Pause on one screen, it pauses on all screens. Send emoji reactions during playback. When the party ends, a summary of everyone's reactions gets posted to Trakt.

---

## What You Need Before Installing

1. **A Synology NAS** (or any machine that can run Docker)
2. **Emby** already installed and working
3. **A Trakt.tv account** (free at trakt.tv)
4. **Container Manager** (the Docker app on Synology — it comes pre-installed on most models)


https://addons.mozilla.org/en-GB/firefox/addon/emby-remote-play/

Fireforx extension that allows you to play an item driect from trakt, imdb or tmdb item page. Do npt pin the extension, has a floating button with a device selection. 
---

## Quick Links

- **INSTALL.md** — Step-by-step setup guide (no technical knowledge needed)
- **HOW_TO_USE.md** — How to use every feature once it's running
- **The dashboard** — Once running, open your browser and go to your server's address on port 8000 (for example: `http://192.168.1.100:8000`)
