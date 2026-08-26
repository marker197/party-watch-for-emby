function esc(s) { if (!s && s !== 0) return ''; return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;'); }
const API = '';  // same origin
let currentUserId = null;
let allUsers = [];
let _radarrServers = [];
let _sonarrServers = [];
// WATCH_PARTY_ENABLED is set inline in the HTML template (Jinja2 variable)
function toggleSuiteTheme() {
  const isLight = document.body.classList.toggle('light-mode');
  localStorage.setItem('suite-theme', isLight ? 'light' : 'dark');
  const btn = document.getElementById('themeToggle');
  if (btn) btn.textContent = isLight ? '🌙 Dark' : '☀️ Light';
}
// Set initial button label
document.addEventListener('DOMContentLoaded', () => {
  const btn = document.getElementById('themeToggle');
  if (btn) btn.textContent = document.body.classList.contains('light-mode') ? '🌙 Dark' : '☀️ Light';
});

async function _loadArrServers() {
  try {
    const [rr, sr] = await Promise.all([
      fetch(API + '/api/radarr/servers').then(r => r.json()),
      fetch(API + '/api/sonarr/servers').then(r => r.json()),
    ]);
    _radarrServers = rr.servers || [];
    _sonarrServers = sr.servers || [];
  } catch(e) { /* silent */ }
}

// ═══════════════════════════════════════════════════════════════════════════
// User Management
// ═══════════════════════════════════════════════════════════════════════════

async function loadUsers() {
  try {
    const r = await fetch(API + '/auth/users');
    allUsers = await r.json();
    // Dashboard only shows Simkl-linked users
    const linkedUsers = allUsers.filter(u => u.linked);
    document.getElementById('userCount').textContent = linkedUsers.length;
    
    // Populate selector with linked users only
    const select = document.getElementById('userSelect');
    select.innerHTML = linkedUsers.map(u => 
      `<option value="${u.id}">${esc(u.emby_username || u.emby_user_id)}${u.linked ? ' ✓' : ''}</option>`
    ).join('');
    
    // Auto-select first linked user
    if (linkedUsers.length > 0) {
      select.value = linkedUsers[0].id;
      switchUser();
    }
    
    // Update user list below selector — linked users only
    const list = document.getElementById('userList');
    list.innerHTML = linkedUsers.map(u => {
      let tokenHtml = '';
      if (u.token_status) {
        const icon = u.token_status === 'ok' ? '🟢' : u.token_status === 'expiring_soon' ? '🟡' : u.token_status === 'expiring_today' ? '🟠' : '🔴';
        let label = '';
        if (u.token_status === 'expired') label = 'Token expired';
        else if (u.token_status === 'expiring_today') {
          if (u.token_hours_left > 0) label = `${u.token_hours_left}h ${u.token_minutes_left || 0}m left`;
          else label = `${u.token_minutes_left || 0}m left`;
        }
        else if (u.token_days_left != null) label = `${u.token_days_left}d left`;
        tokenHtml = ` <span style="font-size:0.72rem;opacity:0.7;">${icon} ${label}</span>`;
      }
      return `<div class="user-row">
        <span class="name">${esc(u.emby_username || u.emby_user_id)}</span>
        <span class="linked">✓ ${esc(u.simkl_username)}${tokenHtml}</span>
      </div>`;
    }).join('');
  } catch(e) {
    console.error('Failed to load users:', e);
  }
}

function switchUser() {
  const select = document.getElementById('userSelect');
  currentUserId = parseInt(select.value);
  
  if (!currentUserId) return;
  
  const user = allUsers.find(u => u.id === currentUserId);
  if (user) {
    const indicator = document.getElementById('userIndicator');
    const text = document.getElementById('indicatorText');
    text.textContent = `Using: ${user.emby_username || user.emby_user_id}`;
    indicator.style.display = 'flex';
    glog(`Switched to user: ${user.emby_username || user.emby_user_id}`, 'ok');
    
    // Refresh data for new user (no navigation — view buttons handle that)
    loadParties();
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Health & Status
// ═══════════════════════════════════════════════════════════════════════════

async function rebuildCache() {
  glog('Rebuilding library cache…', 'ok');
  try {
    const r = await fetch(API + '/cache/rebuild', {method: 'POST'});
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const d = await r.json();
    glog(`Cache rebuilt: ${d.movies ?? 0} movies, ${d.series ?? 0} series, ${d.cached_entries ?? 0} keys`, 'ok');
    showToast('library_cache_rebuild', 'ok', null);
    await dashboardPoll();
  } catch(e) {
    glog('Cache rebuild failed: ' + e.message, 'err');
    showToast('library_cache_rebuild', 'error', null, e.message);
  }
}

let activeCategory = '';  // '' means all

function filterActivity(btn, cat) {
  activeCategory = cat;
  document.querySelectorAll('.activity-filter').forEach(b => {
    b.classList.remove('active');
    b.style.background = 'var(--surface-hover)';
  });
  btn.classList.add('active');
  btn.style.background = 'var(--accent)';
  dashboardPoll();
}

function cronToHuman(cron) {
  const p = cron.split(/\s+/);
  if (p.length < 5) return cron;
  const min = parseInt(p[0]) || 0;
  const hour = parseInt(p[1]) || 0;
  const dow = p[4];
  const days = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  const hh = String(hour).padStart(2,'0');
  const mm = String(min).padStart(2,'0');
  if (dow === '*') return `Daily ${hh}:${mm} UTC`;
  const dayName = days[parseInt(dow)] || dow;
  return `${dayName} ${hh}:${mm} UTC`;
}

let _schedLoaded = false;

function toggleScheduler() {
  const grid = document.getElementById('schedulerGrid');
  const btn = document.getElementById('btnToggleSched');
  if (grid.style.display === 'none') {
    grid.style.display = '';
    btn.textContent = 'Hide ▴';
    if (!_schedLoaded) loadSchedulerStatus();
  } else {
    grid.style.display = 'none';
    btn.textContent = 'Show ▾';
  }
}

async function loadSchedulerStatus() {
  try {
    const r = await fetch(API + '/api/scheduler/status');
    const jobs = await r.json();
    const grid = document.getElementById('schedulerGrid');
    if (!grid) return;

    const entries = Object.entries(jobs);
    if (!entries.length) {
      grid.innerHTML = '<div class="cache-stat"><span class="label">No jobs registered yet</span><span class="value" style="font-size:0.88rem;">—</span></div>';
      return;
    }

    const labels = {
      library_cache_rebuild: '💾 Cache Rebuild',
      smart_queue: '🎯 Smart Queue',
      ml_retrain: '🤖 ML Retrain',
      universe_scan: '🌌 Universe Scan',
      bias_analysis: '🎓 Bias Analysis',
      ssl_cert_check: '🔒 SSL Check',
      heartbeat: '💓 Heartbeat',
      watchlist_sync: '🔄 Watchlist Sync',
    };

    grid.style.gridTemplateColumns = 'repeat(4, 1fr)';
    grid.style.gap = '6px';
    _schedLoaded = true;

    grid.innerHTML = entries.map(([id, j]) => {
      const label = labels[id] || id.replace(/_/g, ' ');
      const dot = j.status === 'ok' ? '🟢' : j.status === 'error' ? '🔴' : '⚪';
      const lastRun = j.last_run
        ? new Date(j.last_run + 'Z').toLocaleString([], {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'})
        : 'Never';
      const dur = j.duration_s != null ? `${j.duration_s}s` : '';
      const sched = j.cron ? cronToHuman(j.cron) : '';
      const errHtml = j.error ? `<span style="color:var(--red);font-size:0.64rem;display:block;margin-top:1px;word-break:break-all;">${j.error.substring(0,40)}</span>` : '';
      return `<div class="cache-stat" style="text-align:left;padding:6px 10px;">
        <span class="label" style="text-align:left;font-size:0.65rem;">${label}</span>
        <span class="value" style="font-size:0.78rem;display:flex;align-items:center;gap:4px;">${dot} ${lastRun}</span>
        <span style="color:var(--text-dim);font-size:0.64rem;">${sched} ${dur ? '· ' + dur : ''}</span>
        ${errHtml}
      </div>`;
    }).join('');
  } catch(e) {
    console.warn('Failed to load scheduler status:', e);
  }
}

async function loadSSLStatus() {
  try {
    const r = await fetch(API + '/api/ssl/status');
    const d = await r.json();
    const section = document.getElementById('sslSection');
    const card = document.getElementById('sslCard');
    if (!section || !card) return;
    if (!d.enabled) { section.style.display = 'none'; return; }
    section.style.display = 'block';

    if (d.error && !d.days_left && d.days_left !== 0) {
      card.innerHTML = `<div class="cache-stat" style="text-align:left;padding:10px 14px;">
        <span class="label" style="text-align:left;">🔴 ${esc(d.domain || 'Unknown')}</span>
        <span class="value" style="font-size:0.88rem;">Error</span>
        <span style="color:var(--red);font-size:0.7rem;word-break:break-all;">${esc((d.error||'').substring(0,80))}</span>
      </div>`;
      return;
    }

    const dot = d.status === 'ok' ? '🟢' : d.status === 'expiring_soon' ? '🟡' : d.status === 'critical' ? '🔴' : d.status === 'expired' ? '⛔' : '⚪';
    const expiry = d.not_after ? new Date(d.not_after + 'Z').toLocaleDateString([], {year:'numeric', month:'short', day:'numeric'}) : '?';
    const checked = d.checked_at ? new Date(d.checked_at + 'Z').toLocaleString([], {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'}) : '';

    card.innerHTML = `<div class="cache-stat" style="text-align:left;padding:10px 14px;min-width:180px;">
      <span class="label" style="text-align:left;">${esc(d.domain || '')}</span>
      <span class="value" style="font-size:0.88rem;display:flex;align-items:center;gap:6px;">${dot} ${d.days_left} days left</span>
      <span style="color:var(--text-dim);font-size:0.7rem;">Expires ${expiry}</span>
    </div>
    <div class="cache-stat" style="text-align:left;padding:10px 14px;">
      <span class="label" style="text-align:left;">Issuer</span>
      <span class="value" style="font-size:0.88rem;">${d.issuer || '?'}</span>
      <span style="color:var(--text-dim);font-size:0.7rem;">Checked ${checked}</span>
    </div>`;
  } catch(e) {
    console.warn('Failed to load SSL status:', e);
  }
}



let _libCacheVersion = -1;
async function loadLibraries() {
  try {
    const r = await fetch(API + '/api/libraries/stats');
    const libs = await r.json();

    const container = document.getElementById('cacheStats');
    if (!container) return;

    // Remove any previously added library cards
    container.querySelectorAll('.lib-card').forEach(el => el.remove());

    const fragment = document.createDocumentFragment();
    for (const lib of libs) {
      const icon = lib.collection_type === 'movies' ? '🎬' : '📺';
      const label = lib.collection_type === 'movies' ? 'Movies' : 'Series';
      const div = document.createElement('div');
      div.className = 'cache-stat lib-card';
      div.innerHTML = `
        <span class="label">${icon} ${esc(lib.name)}</span>
        <span class="value">${lib.item_count}</span>
        <span style="color:var(--text-dim);font-size:0.68rem;">${label}</span>
      `;
      fragment.appendChild(div);
    }
    container.appendChild(fragment);

  } catch(e) {
    console.warn('Failed to load libraries:', e);
  }
}

async function loadParties() {
  try {
    const r = await fetch(API + '/parties');
    const parties = await r.json();
    document.getElementById('partyCount').textContent = parties.length;

    // Build tooltip content
    const tooltip = document.getElementById('partyTooltip');
    const chip = document.getElementById('partyChip');
    if (!tooltip || !chip) return;

    if (parties.length === 0) {
      tooltip.innerHTML = '<span style="color:var(--text-dim);">No active parties</span>';
    } else {
      tooltip.innerHTML = parties.map(p => {
        const users = (p.participants && p.participants.length)
          ? p.participants.join(', ')
          : 'no participants';
        return `<div style="margin-bottom:4px;"><strong>${esc(p.title)}</strong><br><span style="color:var(--text-dim);">${esc(p.code)} · ${esc(users)}</span></div>`;
      }).join('');
    }

    chip.onmouseenter = () => { tooltip.style.display = 'block'; };
    chip.onmouseleave = () => { tooltip.style.display = 'none'; };
  } catch(e) {}
}

// ═══════════════════════════════════════════════════════════════════════════
// Logging
// ═══════════════════════════════════════════════════════════════════════════

function logTo(id, msg, cls='') {
  const el = document.getElementById(id);
  if (!el) return;
  el.style.display = 'block';
  const entry = document.createElement('div');
  entry.className = 'entry ' + cls;
  entry.textContent = new Date().toLocaleTimeString() + '  ' + msg;
  el.prepend(entry);
  
  // Limit to 50 entries
  while (el.children.length > 50) {
    el.removeChild(el.lastChild);
  }
}

function glog(msg, cls='') {
  const el = document.getElementById('globalLog');
  if (!el) return;
  const entry = document.createElement('div');
  entry.className = 'entry client-event ' + cls;
  entry.textContent = new Date().toLocaleTimeString() + '  ' + msg;
  el.prepend(entry);
  while (el.querySelectorAll('.client-event').length > 20) {
    const all = el.querySelectorAll('.client-event');
    all[all.length - 1].remove();
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Auth
// ═══════════════════════════════════════════════════════════════════════════

let _authPollInterval = null;
let _authPollTimeout = null;

async function startLink() {
  const uid = document.getElementById('embyUserId').value.trim();
  const uname = document.getElementById('embyUsername').value.trim();
  if (!uid) {
    logTo('authOutput', 'Enter your Emby User ID first', 'err');
    document.getElementById('embyUserId').style.border = '1px solid var(--red)';
    return;
  }
  document.getElementById('embyUserId').style.border = '';

  // Cancel any previous poll loop
  if (_authPollInterval) { clearInterval(_authPollInterval); _authPollInterval = null; }
  if (_authPollTimeout) { clearTimeout(_authPollTimeout); _authPollTimeout = null; }

  logTo('authOutput', 'Requesting device code…');
  try {
    const r = await fetch(API + '/auth/simkl/device-code', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({emby_user_id: uid, emby_username: uname})
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      const msg = typeof err.detail === 'string' ? err.detail
        : Array.isArray(err.detail) ? err.detail.map(e => e.msg || e).join('; ')
        : `HTTP ${r.status}`;
      logTo('authOutput', `Failed to get device code: ${msg}`, 'err');
      return;
    }
    const d = await r.json();
    logTo('authOutput', `Go to ${d.verification_url} and enter: ${d.user_code}`, 'ok');

    const expiresIn = (d.expires_in || 600) * 1000;
    const pollInterval = (d.interval || 5) * 1000;

    // Auto-stop after expiry
    _authPollTimeout = setTimeout(() => {
      if (_authPollInterval) { clearInterval(_authPollInterval); _authPollInterval = null; }
      logTo('authOutput', 'Device code expired — click Link again to get a new code.', 'err');
    }, expiresIn);

    // Poll loop
    _authPollInterval = setInterval(async () => {
      try {
        const pr = await fetch(API + '/auth/simkl/poll', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({emby_user_id: uid, device_code: d.device_code})
        });
        if (!pr.ok) {
          // 429 or server error — stop polling
          clearInterval(_authPollInterval); _authPollInterval = null;
          clearTimeout(_authPollTimeout); _authPollTimeout = null;
          logTo('authOutput', `Poll error (HTTP ${pr.status}) — click Link to retry.`, 'err');
          return;
        }
        const pd = await pr.json();
        if (pd.status === 'linked') {
          clearInterval(_authPollInterval); _authPollInterval = null;
          clearTimeout(_authPollTimeout); _authPollTimeout = null;
          logTo('authOutput', `Linked as ${pd.simkl_username}!`, 'ok');
          glog(`User linked: ${pd.simkl_username}`, 'ok');
          loadUsers();
        }
      } catch(e) {
        // Network error — stop polling
        clearInterval(_authPollInterval); _authPollInterval = null;
        clearTimeout(_authPollTimeout); _authPollTimeout = null;
        logTo('authOutput', 'Poll failed: ' + e.message, 'err');
      }
    }, pollInterval);
  } catch(e) {
    logTo('authOutput', 'Failed: ' + e.message, 'err');
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Feature #1: Smart Queue
// ═══════════════════════════════════════════════════════════════════════════

async function triggerQueue() {
  if (!currentUserId) return alert('Select a user first');
  glog('Smart Queue refresh triggered…');
  try {
    const r = await fetch(API + '/queue/refresh', {method:'POST'});
    if (!r.ok) throw new Error('HTTP ' + r.status);
    logTo('queueOutput', '⏳ Refreshing queue — this usually takes under a minute…', 'ok');
    // Poll every 5s until queue data changes or 90s timeout
    _pollForQueueDone();
  } catch(e) {
    logTo('queueOutput', 'Refresh failed: ' + e.message, 'err');
    showToast('smart_queue', 'error', null, e.message);
  }
}

async function _pollForQueueDone() {
  const maxAttempts = 18; // 18 × 5s = 90s
  for (let i = 0; i < maxAttempts; i++) {
    await new Promise(ok => setTimeout(ok, 5000));
    try {
      const r = await fetch(API + `/queue/${currentUserId}?limit=1`);
      if (!r.ok) continue;
      const items = await r.json();
      if (Array.isArray(items) && items.length > 0) {
        logTo('queueOutput', '✓ Queue refreshed — ' + items.length + '+ items ready!', 'ok');
        showToast('smart_queue', 'ok', null);
        // Auto-refresh if panel is open
        if (document.getElementById('queuePanel').style.display !== 'none') {
          viewQueue();
        }
        // Clear the log area after a moment so the card returns to normal height
        setTimeout(() => {
          const out = document.getElementById('queueOutput');
          out.innerHTML = '';
          out.style.display = 'none';
        }, 10000);
        return;
      }
    } catch(e) { /* keep trying */ }
  }
  logTo('queueOutput', '⚠ Refresh is taking longer than expected — try opening the queue in a moment.', 'err');
}

let _dlPollTimer = null;
let _realtimeTimer = null;
let _dlMode = 'idle'; // idle | normal | realtime
let _patchInFlight = false; // guard against overlapping 500ms fetches

function toggleQueue() {
  const panel = document.getElementById('queuePanel');
  const btn = document.getElementById('btnToggleQueue');
  if (panel.style.display === 'none') {
    panel.style.display = 'block';
    btn.textContent = 'Hide Queue ▴';
    viewQueue();
    _startNormalPoll();
  } else {
    panel.style.display = 'none';
    btn.textContent = 'View Queue ▾';
    _stopAllPolling();
  }
}

function _startNormalPoll() {
  _stopAllPolling();
  _dlMode = 'normal';
  _dlPollTimer = setInterval(() => {
    if (document.getElementById('queuePanel').style.display === 'none') {
      _stopAllPolling();
      return;
    }
    viewQueue();
  }, 5000);
}

function _startRealtimePoll() {
  if (_dlMode === 'realtime') return;
  // Stop normal poll, start 500ms SAB-only poll
  if (_dlPollTimer) { clearInterval(_dlPollTimer); _dlPollTimer = null; }
  _dlMode = 'realtime';
  _patchInFlight = false;
  _realtimeTimer = setInterval(_patchProgress, 500);
}

function _stopAllPolling() {
  if (_dlPollTimer) { clearInterval(_dlPollTimer); _dlPollTimer = null; }
  if (_realtimeTimer) { clearInterval(_realtimeTimer); _realtimeTimer = null; }
  _dlMode = 'idle';
  _patchInFlight = false;
}

async function _patchProgress() {
  if (_patchInFlight) return; // skip if previous fetch still running
  _patchInFlight = true;
  try {
    const r = await fetch(API + '/api/download-progress');
    if (!r.ok) return;
    const d = await r.json();

    if (d.count === 0) {
      // Downloads finished — do one full refresh, resume normal polling
      if (_realtimeTimer) { clearInterval(_realtimeTimer); _realtimeTimer = null; }
      _dlMode = 'idle';
      await viewQueue(true); // skipTransition — don't re-enter realtime
      refreshDownloadsCard();
      if (document.getElementById('queuePanel').style.display !== 'none') {
        _startNormalPoll();
      }
      return;
    }

    const slots = d.slots || {};
    // Patch queue panel + downloads card progress bars by data-nzo
    document.querySelectorAll('[data-nzo]').forEach(el => {
      const nzo = el.getAttribute('data-nzo');
      const sab = slots[nzo];
      if (!sab) return;
      const fill = el.querySelector('.dl-progress-fill');
      const text = el.querySelector('.dl-progress-text');
      // dl-meta is a sibling, not a child — walk up to parent then find it
      const meta = el.parentElement ? el.parentElement.querySelector('.dl-meta') : null;
      if (fill) fill.style.width = sab.progress + '%';
      if (text) text.textContent = sab.progress.toFixed(0) + '%';
      if (meta) {
        const parts = [];
        if (sab.eta) parts.push(sab.eta);
        if (sab.speed) parts.push(sab.speed + 'B/s');
        if (sab.status && sab.status !== 'Downloading') parts.push(sab.status);
        meta.textContent = parts.join(' · ');
      }
    });
  } catch { /* silent */ }
  finally { _patchInFlight = false; }
}

async function viewQueue(skipTransition) {
  if (!currentUserId) return alert('Select a user first');
  try {
    const [qr, dlr, arrr] = await Promise.all([
      fetch(API + `/queue/${currentUserId}?limit=30`),
      fetch(API + '/api/download-queue').catch(() => null),
      fetch(API + '/api/arr-library').catch(() => null),
    ]);
    const items = await qr.json();
    const el = document.getElementById('queueTable');
    if (!Array.isArray(items) || items.length === 0) {
      el.innerHTML = '<div style="color:var(--text-dim);padding:8px;">Queue is empty — click Refresh Queue, then check back in a minute.</div>';
      return;
    }

    // Build download lookup: tmdb_id → download, tvdb_id → [downloads]
    const dlByTmdb = {};
    const dlByTvdb = {};
    if (dlr && dlr.ok) {
      const dlData = await dlr.json();
      for (const dl of (dlData.downloads || [])) {
        if (dl.tmdb_id) dlByTmdb[dl.tmdb_id] = dl;
        if (dl.tvdb_id) {
          if (!dlByTvdb[dl.tvdb_id]) dlByTvdb[dl.tvdb_id] = [];
          dlByTvdb[dl.tvdb_id].push(dl);
        }
      }
    }

    // Build arr-library lookup: tmdb_id → server name, tvdb_id → server name
    const arrRadarr = {};
    const arrSonarr = {};
    if (arrr && arrr.ok) {
      const arrData = await arrr.json();
      for (const tid of (arrData.radarr_tmdb || [])) arrRadarr[tid] = (arrData.radarr_names || {})[tid] || 'Radarr';
      for (const tid of (arrData.sonarr_tvdb || [])) arrSonarr[tid] = (arrData.sonarr_names || {})[tid] || 'Sonarr';
    }

    el.innerHTML = '<table class="queue-table"><thead><tr>' +
      '<th style="width:30px;">#</th><th>Title</th><th style="width:80px;">Source</th><th style="width:55px;">Score</th><th style="width:70px;"></th><th style="width:24px;"></th>' +
      '</tr></thead><tbody>' +
      items.map((i, n) => {
        const inLib = i.in_library !== false;
        let rating = '';
        const mr = i.mdblist_ratings || {};
        if (mr.imdb || mr.tmdb || mr.tomatoes || mr.popcorn) {
          const parts = [];
          if (mr.imdb != null) parts.push('⭐' + mr.imdb);
          if (mr.tmdb != null) parts.push('🎬' + mr.tmdb);
          if (mr.tomatoes != null) parts.push('🍅' + mr.tomatoes + '%');
          if (mr.popcorn != null) parts.push('🍿' + mr.popcorn + '%');
          rating = ' <span style="font-size:0.72rem;letter-spacing:0.5px;">' + parts.join(' ') + '</span>';
        } else if (i.community_rating != null) {
          rating = ' <span style="color:var(--orange);font-size:0.78rem;">★' + Number(i.community_rating).toFixed(1) + '</span>';
        }
        const yearStr = i.year ? ` <span style="color:var(--text-dim);font-size:0.72rem;">(${i.year})</span>` : '';

        // Check if this item is actively downloading
        let dl = null;
        let dlEps = null;
        if (i.type === 'movie' && i.tmdb_id && dlByTmdb[i.tmdb_id]) {
          dl = dlByTmdb[i.tmdb_id];
        } else if (i.type === 'show' && i.tvdb_id && dlByTvdb[i.tvdb_id]) {
          dlEps = dlByTvdb[i.tvdb_id];
        }

        // "New" badge — shown inline in title after rating, only if added to library within 7 days
        let newBadge = '';
        if (inLib && !i.played && !dl && !dlEps && i.date_created) {
          const addedMs = new Date(i.date_created).getTime();
          const sevenDaysAgo = Date.now() - (7 * 24 * 60 * 60 * 1000);
          if (addedMs > sevenDaysAgo) newBadge = ' <span class="queue-badge unwatched">new</span>';
        }

        // Download progress — shown inline under title
        let dlInline = '';
        if (dl) {
          const pct = dl.progress || 0;
          const barColor = dl.tracked_status === 'warning' ? 'var(--orange)' : 'var(--blue)';
          const etaStr = _formatEta(dl.eta);
          const sizeStr = dl.size_mb ? `${dl.size_mb.toFixed(0)} MB` : '';
          const nzoAttr = dl.download_id ? ` data-nzo="${dl.download_id}"` : '';
          dlInline = `<div class="dl-progress-wrap"${nzoAttr} style="margin-top:3px;">
            <div class="dl-progress-bar"><div class="dl-progress-fill" style="width:${pct}%;background:${barColor};"></div></div>
            <span class="dl-progress-text">${pct.toFixed(0)}%</span>
          </div>
          <div class="dl-meta" style="font-size:0.6rem;color:var(--text-dim);margin-top:1px;">${etaStr}${sizeStr ? ' · ' + sizeStr : ''}</div>`;
        } else if (dlEps && dlEps.length > 0) {
          const totalPct = dlEps.reduce((s, d) => s + (d.progress || 0), 0) / dlEps.length;
          const anyWarning = dlEps.some(d => d.tracked_status === 'warning');
          const barColor = anyWarning ? 'var(--orange)' : 'var(--blue)';
          const epLabels = dlEps.map(d => d.episode_label).filter(Boolean).join(', ');
          const nzoAttr = dlEps[0].download_id ? ` data-nzo="${dlEps[0].download_id}"` : '';
          dlInline = `<div class="dl-progress-wrap"${nzoAttr} style="margin-top:3px;">
            <div class="dl-progress-bar"><div class="dl-progress-fill" style="width:${totalPct}%;background:${barColor};"></div></div>
            <span class="dl-progress-text">${totalPct.toFixed(0)}%</span>
          </div>
          <div class="dl-meta" style="font-size:0.6rem;color:var(--text-dim);margin-top:1px;">${dlEps.length} ep${dlEps.length > 1 ? 's' : ''}${epLabels ? ': ' + esc(epLabels) : ''}</div>`;
        }

        const titleClass = inLib ? 'queue-title' : (dl || dlEps ? 'queue-title' : 'queue-title missing');
        const safeTitle = esc(i.title || '').replace(/'/g, '&#39;');
        const imdbLink = (!inLib && !dl && !dlEps && i.imdb_id)
          ? ` <a href="https://www.imdb.com/title/${i.imdb_id}/" target="_blank" rel="noopener" class="queue-imdb-link">IMDb</a>`
          : '';

        let actionBtn = '';
        if (dl || dlEps) {
          // Item is downloading — show the server badge instead of action button
          const srvName = dl ? dl.server : dlEps[0].server;
          actionBtn = `<span class="dl-badge">${esc(srvName)}</span>`;
        } else if (!inLib && i.tmdb_id && i.type === 'movie' && arrRadarr[i.tmdb_id]) {
          // Already in Radarr — show label instead of send button
          actionBtn = `<span class="arr-in-badge">In ${esc(arrRadarr[i.tmdb_id])}</span>`;
        } else if (!inLib && i.type === 'show' && i.tvdb_id && arrSonarr[i.tvdb_id]) {
          // Already in Sonarr — show label instead of send button
          actionBtn = `<span class="arr-in-badge">In ${esc(arrSonarr[i.tvdb_id])}</span>`;
        } else if (!inLib && i.tmdb_id && i.type === 'movie') {
          if (_radarrServers.length > 1) {
            const opts = _radarrServers.map((s, si) =>
              `<option value="${si}">${esc(s.quality_profile_name || s.name)}</option>`
            ).join('');
            actionBtn = `<span class="arr-dropdown-wrap"><select class="arr-dropdown radarr" id="radarr-sel-${n}" title="Choose Radarr server">${opts}</select>` +
              `<button class="queue-radarr-btn" id="radarr-btn-${n}-0" onclick="sendToRadarr(${n}, ${i.tmdb_id}, '${i.imdb_id || ''}', '${safeTitle}', ${i.year || 0}, parseInt(document.getElementById('radarr-sel-${n}').value))">📥</button></span>`;
          } else {
            actionBtn = `<button class="queue-radarr-btn" id="radarr-btn-${n}-0" onclick="sendToRadarr(${n}, ${i.tmdb_id}, '${i.imdb_id || ''}', '${safeTitle}', ${i.year || 0}, 0)">📥 Radarr</button>`;
          }
        } else if (!inLib && i.type === 'show') {
          if (_sonarrServers.length > 1) {
            const opts = _sonarrServers.map((s, si) =>
              `<option value="${si}">${esc(s.quality_profile_name || s.name)}</option>`
            ).join('');
            actionBtn = `<span class="arr-dropdown-wrap"><select class="arr-dropdown sonarr" id="sonarr-sel-${n}" title="Choose Sonarr server">${opts}</select>` +
              `<button class="queue-sonarr-btn" id="sonarr-btn-${n}-0" onclick="sendToSonarr(${n}, ${i.tvdb_id || 0}, '${i.imdb_id || ''}', '${safeTitle}', ${i.year || 0}, parseInt(document.getElementById('sonarr-sel-${n}').value))">📥</button></span>`;
          } else {
            actionBtn = `<button class="queue-sonarr-btn" id="sonarr-btn-${n}-0" onclick="sendToSonarr(${n}, ${i.tvdb_id || 0}, '${i.imdb_id || ''}', '${safeTitle}', ${i.year || 0}, 0)">📥 Sonarr</button>`;
          }
        } else if (inLib && i.emby_item_id) {
          actionBtn = `<span style="display:inline-flex;gap:4px;"><button class="btn btn-sm" style="background:none;border:1px solid var(--accent);color:var(--accent);padding:2px 7px;border-radius:4px;font-size:0.72rem;white-space:nowrap;cursor:pointer;" onclick="createPartyFromQueue('${i.emby_item_id}', '${safeTitle}')">🎉 Party</button><button class="btn btn-sm" style="background:none;border:1px solid var(--green);color:var(--green);padding:2px 7px;border-radius:4px;font-size:0.72rem;white-space:nowrap;cursor:pointer;" onclick="pickDeviceAndPlay('${i.emby_item_id}', 0)">▶ Play</button></span>`;
        }

        // Block button
        const blockBtn = i.simkl_id
          ? `<button class="queue-block-btn" title="Never show again" onclick="blockQueueItem('${i.simkl_id}', '${safeTitle}', '${i.type || ''}')">✕</button>`
          : '';

        return `<tr id="queue-row-${n}">
          <td style="color:var(--text-dim);font-size:0.78rem;">${n + 1}</td>
          <td><span class="${titleClass}">${esc(i.title)}</span>${yearStr}${rating}${newBadge}${imdbLink}${dlInline}</td>
          <td class="queue-source"><span class="source-tag source-${i.source}">${i.source}</span></td>
          <td class="queue-score">${i.score}</td>
          <td>${actionBtn}</td>
          <td>${blockBtn}</td>
        </tr>`;
      }).join('') +
      '</tbody></table>';

    // Transition to realtime mode if any downloads are active
    // (skip when called from _patchProgress to prevent re-entry loop)
    if (!skipTransition) {
      const hasDownloads = document.querySelectorAll('[data-nzo]').length > 0;
      if (hasDownloads && _dlMode !== 'realtime') {
        _startRealtimePoll();
      } else if (!hasDownloads && _dlMode === 'realtime') {
        if (_realtimeTimer) { clearInterval(_realtimeTimer); _realtimeTimer = null; }
        if (document.getElementById('queuePanel').style.display !== 'none') {
          _startNormalPoll();
        }
      }
    }
  } catch(e) {
    document.getElementById('queueTable').innerHTML = `<div style="color:var(--red);padding:8px;">Failed: ${esc(e.message)}</div>`;
  }
}

async function blockQueueItem(simklId, title, itemType) {
  if (!currentUserId) return;
  if (!confirm(`Permanently hide "${title}" from your Smart Queue?\n\nIt will never appear again. You can undo this from Settings.`)) return;
  try {
    const r = await fetch(API + '/api/queue/block', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ user_id: currentUserId, simkl_id: simklId, title: title, item_type: itemType }),
    });
    const d = await r.json();
    if (d.status === 'ok') {
      showNotification(`"${title}" blocked — won't appear again`);
      viewQueue();
    } else {
      showNotification('Error: ' + (d.detail || 'unknown'));
    }
  } catch(e) {
    showNotification('Error: ' + e.message);
  }
}

function _formatEta(isoStr) {
  if (!isoStr) return '';
  try {
    const eta = new Date(isoStr);
    const now = new Date();
    const diffMs = eta - now;
    if (diffMs <= 0) return 'any moment';
    const mins = Math.round(diffMs / 60000);
    if (mins < 60) return `~${mins}m`;
    const hrs = Math.floor(mins / 60);
    const rm = mins % 60;
    if (hrs < 24) return `~${hrs}h${rm > 0 ? rm + 'm' : ''}`;
    const days = Math.floor(hrs / 24);
    return `~${days}d`;
  } catch(e) { return ''; }
}

function toggleAiring() {
  const panel = document.getElementById('airingPanel');
  const btn = document.getElementById('btnToggleAiring');
  if (panel.style.display === 'none') {
    panel.style.display = 'block';
    btn.textContent = 'Hide Airing Soon ▴';
    viewAiring();
  } else {
    panel.style.display = 'none';
    btn.textContent = 'View Airing Soon ▾';
  }
}

async function viewAiring() {
  if (!currentUserId) return alert('Select a user first');
  const el = document.getElementById('airingTable');
  el.innerHTML = '<div style="color:var(--text-dim);padding:8px;">Loading…</div>';
  try {
    const [r, arrr, importedR] = await Promise.all([
      fetch(API + `/api/airing-soon/${currentUserId}?days=30`),
      fetch(API + '/api/arr-library').catch(() => null),
      fetch(API + '/api/sonarr/imported').catch(() => null),
    ]);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const items = data.items || [];

    // Build arr-library lookup
    const arrRadarr = {};
    const arrSonarr = {};
    if (arrr && arrr.ok) {
      const arrData = await arrr.json();
      for (const tid of (arrData.radarr_tmdb || [])) arrRadarr[tid] = (arrData.radarr_names || {})[tid] || 'Radarr';
      for (const tid of (arrData.sonarr_tvdb || [])) arrSonarr[tid] = (arrData.sonarr_names || {})[tid] || 'Sonarr';
    }

    // Build sonarr imported lookup: "tvdbId:SxxExx" → data
    const sonarrImported = {};
    if (importedR && importedR.ok) {
      const impData = await importedR.json();
      for (const [k, v] of Object.entries(impData)) sonarrImported[k] = v;
    }

    const homeReleases = data.upcoming_home_releases || [];
    const fmtDate = (d) => { if (!d) return '—'; try { const dt = new Date(d + 'T00:00:00'); return dt.toLocaleDateString('en-GB', {day:'numeric',month:'short',year:'numeric'}); } catch(e) { return d; } };

    let html = '';

    if (items.length === 0 && homeReleases.length === 0) {
      el.innerHTML = '<div style="color:var(--text-dim);padding:8px;">Nothing airing in the next 30 days.</div>';
      return;
    }

    // ── Main airing table ──
    // Filter out movies that are in library with digital/physical release dates
    // — those belong in the Upcoming Releases section only
    const mainItems = items.filter(i => {
      if (i.media_type === 'movie' && i.in_library && (i.digital_release || i.physical_release)) return false;
      return true;
    });
    if (mainItems.length > 0) {
    html += '<table class="queue-table" id="airingMainTable"><thead><tr>' +
      '<th>Title</th><th style="width:70px;">S/E</th><th style="width:70px;">Airs</th><th style="width:90px;">Status</th><th style="width:70px;"></th>' +
      '</tr></thead><tbody>' +
      mainItems.map((i, idx) => {
        const epNum = (i.season != null && i.episode != null) ? `S${String(i.season).padStart(2,'0')}E${String(i.episode).padStart(2,'0')}` : '—';
        let se = i.media_type === 'movie'
          ? '<span style="color:var(--text-dim);">movie</span>'
          : epNum;
        let rowClass = '';
        if (i.is_premiere && i.media_type !== 'movie') {
          rowClass = 'airing-premiere-row';
        }
        const days = i.days_until_air;
        const inStr = days == null ? '—' : (days === 0 ? 'today' : days === 1 ? 'tomorrow' : `${days}d`);

        // Format air date as DD Mon for premiere display
        const fmtAirDD = (d) => { if (!d) return ''; try { const dt = new Date(d.length === 10 ? d + 'T00:00:00' : d); return dt.toLocaleDateString('en-GB', {day:'numeric',month:'short'}); } catch(e) { return ''; } };

        // Build title cell with inline premiere label + air date
        let titleCell = (i.title || '').replace(/</g,'&lt;');
        if (i.is_premiere && i.media_type !== 'movie') {
          titleCell += ` — <span style="color:var(--orange);font-weight:700;">Premiere</span>`;
          const airDD = fmtAirDD(i.air_date);
          if (airDD) titleCell += ` <span style="color:var(--text-dim);font-size:0.72rem;">${airDD}</span>`;
          if (i.episode_title) {
            titleCell += ` : <span style="color:var(--text-dim);font-size:0.78rem;">${i.episode_title.replace(/</g,'&lt;')}</span>`;
          }
        } else if (i.is_finale) {
          titleCell += ` — <span style="color:var(--red);font-weight:700;">🏁 Finale</span>`;
          if (i.episode_title) {
            titleCell += ` : <span style="color:var(--text-dim);font-size:0.78rem;">${i.episode_title.replace(/</g,'&lt;')}</span>`;
          }
        } else if (i.episode_title) {
          titleCell += ` <span style="color:var(--text-dim);font-size:0.78rem;">— ${i.episode_title.replace(/</g,'&lt;')}</span>`;
        }
        let binge = '';
        if (i.binge_plan && i.is_finale) {
          const bp = i.binge_plan;
          if (bp.status === 'caught_up') {
            binge = '<div style="font-size:0.72rem;color:var(--green);margin-top:2px;">✓ All caught up</div>';
          } else {
            const diffColor = bp.difficulty === 'easy' ? 'var(--green)' : bp.difficulty === 'moderate' ? 'var(--orange)' : 'var(--red)';
            binge = `<div style="font-size:0.72rem;color:${diffColor};margin-top:2px;">📺 ${bp.message}</div>`;
          }
        }
        // Movie release dates — only show under main heading if NOT in library
        // and only for upcoming airing (not release dates which go in the releases section)
        let releaseDates = '';
        if (i.media_type === 'movie' && !i.in_library && i.theatrical_release && !i.digital_release && !i.physical_release) {
          releaseDates = `<div style="font-size:0.72rem;color:var(--text-dim);margin-top:2px;">🎬 Cinema: ${fmtDate(i.theatrical_release)}</div>`;
        }
        // Streaming service logos
        let streamBadges = '';
        if (i.streaming_services && i.streaming_services.length > 0) {
          const badges = i.streaming_services.map(s => {
            if (s.logo_url) {
              return `<img src="${s.logo_url}" alt="${esc(s.name)}" title="${esc(s.name)}" style="height:20px;width:20px;border-radius:4px;vertical-align:middle;background:#222;padding:1px;">`;
            }
            return `<span style="font-size:0.68rem;color:#ccc;background:#333;padding:2px 6px;border-radius:4px;vertical-align:middle;white-space:nowrap;">${esc(s.name)}</span>`;
          }).join(' ');
          streamBadges = `<span style="margin-left:8px;display:inline-flex;gap:4px;align-items:center;vertical-align:middle;">${badges}</span>`;
        }
        // Status column: Imported / In Radarr / In Sonarr badge
        // Check sonarr import status first (highest priority for shows)
        let statusBadge = '';
        const importKey = i.tvdb_id && i.season != null && i.episode != null
          ? `${i.tvdb_id}:S${i.season}E${i.episode}` : '';
        if (importKey && sonarrImported[importKey]) {
          statusBadge = '<span class="queue-badge watched" style="background:rgba(61,154,86,0.15);color:var(--green);border-color:var(--green);">Imported</span>';
        } else if (i.media_type === 'movie') {
          if (i.in_library) {
            statusBadge = '<span class="queue-badge unwatched">in library</span>';
          } else if (i.tmdb_id && arrRadarr[i.tmdb_id]) {
            statusBadge = `<span class="arr-in-badge">In ${esc(arrRadarr[i.tmdb_id])}</span>`;
          }
        } else {
          if (i.tvdb_id && arrSonarr[i.tvdb_id]) {
            statusBadge = `<span class="arr-in-badge">In ${esc(arrSonarr[i.tvdb_id])}</span>`;
          } else if (i.in_library) {
            statusBadge = '<span class="queue-badge unwatched">in library</span>';
          }
        }
        // Action column: send to arr buttons (only for items not already in arr)
        let actionBtn = '';
        if (i.media_type === 'movie' && !i.in_library && !(i.tmdb_id && arrRadarr[i.tmdb_id])) {
          if (i.tmdb_id) {
            const safeTitle = esc(i.title || '').replace(/'/g, '&#39;');
            actionBtn = `<button class="queue-radarr-btn" id="radarr-btn-${2000 + idx}-0" onclick="sendToRadarr(${2000 + idx}, ${i.tmdb_id || 0}, '${i.imdb_id || ''}', '${safeTitle}', ${i.year || 0}, 0)">📥 Radarr</button>`;
          }
        } else if (i.media_type === 'show' && !i.in_library && !(i.tvdb_id && arrSonarr[i.tvdb_id])) {
          if (i.is_premiere) {
            const safeTitle = esc(i.title || '').replace(/'/g, '&#39;');
            actionBtn = `<button class="queue-sonarr-btn" id="sonarr-btn-${1000 + idx}-0" onclick="sendToSonarr(${1000 + idx}, ${i.tvdb_id || 0}, '${i.imdb_id || ''}', '${safeTitle}', ${i.year || 0}, 0)">📥 Sonarr</button>`;
          }
        }
        return `<tr class="${rowClass}" data-media-type="${i.media_type}">
          <td>${titleCell}${streamBadges}${binge}${releaseDates}</td>
          <td class="queue-source">${se}</td>
          <td>${inStr}</td>
          <td>${statusBadge}</td>
          <td>${actionBtn}</td>
        </tr>`;
      }).join('') +
      '</tbody></table>';
    }

    // ── Upcoming Digital / Physical Releases (missing in Radarr) ──
    if (homeReleases.length > 0) {
      html += `<div style="margin-top:16px;padding-top:12px;border-top:1px solid var(--surface-hover);">
        <h4 style="font-size:0.85rem;margin:0 0 8px;">💿 Upcoming Digital / Physical Releases</h4>
        <table class="queue-table"><thead><tr>
          <th>Title</th><th style="width:120px;">Digital</th><th style="width:120px;">Physical</th><th style="width:70px;">In</th>
        </tr></thead><tbody>` +
        homeReleases.map(m => {
          const title = (m.title || '').replace(/</g, '&lt;');
          const digital = m.digital_release ? fmtDate(m.digital_release) : '—';
          const physical = m.physical_release ? fmtDate(m.physical_release) : '—';
          const daysStr = m.days_until == null ? '' : (m.days_until === 0 ? 'today' : m.days_until === 1 ? 'tomorrow' : `${m.days_until}d`);
          const digitalCell = m.digital_release
            ? `${digital} <span style="opacity:0.5;font-size:0.72rem;">${m.digital_release && m.days_until != null && m.digital_release <= (m.physical_release || '9') ? daysStr : ''}</span>`
            : '—';
          const physicalCell = m.physical_release
            ? `${physical} <span style="opacity:0.5;font-size:0.72rem;">${m.physical_release && m.days_until != null && (m.physical_release <= (m.digital_release || '9')) ? daysStr : ''}</span>`
            : '—';
          // Streaming logos
          let streamLogos = '';
          if (m.streaming_services && m.streaming_services.length > 0) {
            const logos = m.streaming_services.map(s => {
              if (s.logo_url) {
                return `<img src="${s.logo_url}" alt="${esc(s.name)}" title="${esc(s.name)}" style="height:18px;width:18px;border-radius:3px;vertical-align:middle;background:#222;padding:1px;">`;
              }
              return `<span style="font-size:0.65rem;color:#ccc;background:#333;padding:1px 5px;border-radius:3px;">${esc(s.name)}</span>`;
            }).join(' ');
            streamLogos = ` <span style="display:inline-flex;gap:3px;align-items:center;margin-left:4px;">${logos}</span>`;
          }
          return `<tr>
            <td>${title}${streamLogos}</td>
            <td style="font-size:0.8rem;">${digitalCell}</td>
            <td style="font-size:0.8rem;">${physicalCell}</td>
            <td style="font-size:0.75rem;color:var(--text-dim);">${daysStr}</td>
          </tr>`;
        }).join('') +
        '</tbody></table></div>';
    }

    el.innerHTML = html;
  } catch(e) {
    el.innerHTML = `<div style="color:var(--red);padding:8px;">Failed: ${esc(e.message)}</div>`;
  }
}

function filterAiring(type) {
  document.querySelectorAll('.airing-filter-btn').forEach(b => b.classList.remove('active'));
  document.getElementById(type === 'show' ? 'airingFilterTV' : type === 'movie' ? 'airingFilterMovie' : 'airingFilterAll').classList.add('active');
  const table = document.getElementById('airingMainTable');
  if (!table) return;
  table.querySelectorAll('tbody tr').forEach(row => {
    if (type === 'all') { row.style.display = ''; return; }
    row.style.display = row.getAttribute('data-media-type') === type ? '' : 'none';
  });
}

async function sendToRadarr(rowIdx, tmdbId, imdbId, title, year, serverIdx) {
  const btn = document.getElementById('radarr-btn-' + rowIdx + '-' + serverIdx);
  if (!btn) return;
  btn.disabled = true;
  btn.textContent = '⏳';
  try {
    const r = await fetch(API + '/api/radarr/add', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        server_index: serverIdx || 0,
        movies: [{ tmdb_id: tmdbId, imdb_id: imdbId || undefined, title: title, year: year || undefined }]
      })
    });
    const d = await r.json();
    if (r.ok && d.added > 0) {
      btn.textContent = '✓ Sent';
      btn.style.background = 'var(--green)';
      showNotification(`${title} sent to ${d.server || 'Radarr'}`);
      // Refresh queue after a short delay so download progress shows
      setTimeout(() => viewQueue(), 5000);
    } else {
      const reason = (d.results && d.results[0] && d.results[0].reason) || d.detail || 'Failed';
      btn.textContent = '✗';
      btn.style.background = 'var(--red)';
      showNotification(`Radarr: ${reason}`);
    }
  } catch(e) {
    btn.textContent = '✗';
    btn.style.background = 'var(--red)';
    showNotification(`Radarr error: ${e.message}`);
  }
}

async function sendToSonarr(rowIdx, tvdbId, imdbId, title, year, serverIdx) {
  const btn = document.getElementById('sonarr-btn-' + rowIdx + '-' + serverIdx);
  if (!btn) return;
  btn.disabled = true;
  btn.textContent = '⏳';
  try {
    const r = await fetch(API + '/api/sonarr/add', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        server_index: serverIdx || 0,
        shows: [{ tvdb_id: tvdbId || undefined, imdb_id: imdbId || undefined, title: title, year: year || undefined }]
      })
    });
    const d = await r.json();
    if (r.ok && d.added > 0) {
      btn.textContent = '✓ Sent';
      btn.style.background = 'var(--green)';
      showNotification(`${title} sent to ${d.server || 'Sonarr'}`);
      // Refresh queue after a short delay so download progress shows
      setTimeout(() => viewQueue(), 5000);
    } else {
      const reason = (d.results && d.results[0] && d.results[0].reason) || d.detail || 'Failed';
      btn.textContent = '✗';
      btn.style.background = 'var(--red)';
      showNotification(`Sonarr: ${reason}`);
    }
  } catch(e) {
    btn.textContent = '✗';
    btn.style.background = 'var(--red)';
    showNotification(`Sonarr error: ${e.message}`);
  }
}

async function apiError(r) {
  try { const d = await r.json(); return d.detail || JSON.stringify(d); }
  catch(_) { return 'HTTP ' + r.status; }
}

async function createPartyFromQueue(embyItemId, title) {
  if (!currentUserId) return alert('Select a user first');
  try {
    const r = await fetch(API + '/party/create', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({host_user_id: currentUserId, emby_item_id: embyItemId})
    });
    if (!r.ok) throw new Error(await apiError(r));
    const d = await r.json();
    glog(`Watch party created from queue: ${d.code} — "${d.title}"`, 'ok');
    if (confirm(`Party created! Code: ${d.code}\n\nOpen Watch Party page?`)) {
      window.location.href = `/watch-party?code=${d.code}`;
    }
  } catch(e) {
    glog('Error creating party: ' + e.message, 'err');
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Feature #2: ML Rating Predictor
// ═══════════════════════════════════════════════════════════════════════════

async function trainModel() {
  if (!currentUserId) return alert('Select a user first');
  glog('ML training triggered…');
  logTo('mlOutput', '⏳ Training started — this may take a few minutes…');
  try {
    const r = await fetch(API + `/ml/train/${currentUserId}`, {method:'POST'});
    const d = await r.json();
    const isOk = d.status === 'trained';
    logTo('mlOutput', isOk ? '✓ Model trained successfully!' : `✗ ${d.status}`, isOk ? 'ok' : 'err');
    glog(`ML training: ${d.status}`, isOk ? 'ok' : 'err');
    showToast('ml_retrain', isOk ? 'ok' : 'error', null, !isOk ? d.status : null);
    if (isOk) {
      setTimeout(() => {
        const out = document.getElementById('mlOutput');
        out.innerHTML = '';
        out.style.display = 'none';
      }, 10000);
    }
  } catch(e) {
    logTo('mlOutput', 'Training failed: ' + e.message, 'err');
    showToast('ml_retrain', 'error', null, e.message);
  }
}

function viewPredictions() {
  window.location.href = '/predictions';
}

// ═══════════════════════════════════════════════════════════════════════════
// Feature #3: Universe Discovery
// ═══════════════════════════════════════════════════════════════════════════

async function scanUniverses() {
  glog('Universe scan triggered…');
  try {
    const r = await fetch(API + '/universes/scan', {method:'POST'});
    if (!r.ok) throw new Error('HTTP ' + r.status);
    logTo('universeOutput', 'Scan started…', 'ok');
    showToast('universe_scan', 'ok', null);
  } catch(e) {
    logTo('universeOutput', 'Scan failed: ' + e.message, 'err');
    showToast('universe_scan', 'error', null, e.message);
  }
}

async function viewUniverses() {
  window.location.href = '/lists';
}

// ═══════════════════════════════════════════════════════════════════════════
// Watchlist Sync
// ═══════════════════════════════════════════════════════════════════════════

async function runWatchlistSync() {
  const btn = document.getElementById('btnWlSync');
  btn.disabled = true;
  btn.textContent = 'Syncing…';
  btn.style.color = 'var(--text-dim)';
  try {
    const r = await fetch(API + '/api/watchlist-sync/run', {method: 'POST'});
    if (!r.ok) throw new Error('HTTP ' + r.status);
    btn.textContent = '✓ Synced';
    btn.style.color = 'var(--green)';
    glog('Watchlist sync completed', 'ok');
  } catch(e) {
    btn.textContent = '✗ Failed';
    btn.style.color = 'var(--red)';
    glog('Watchlist sync failed: ' + e.message, 'err');
  } finally {
    setTimeout(() => {
      btn.disabled = false;
      btn.textContent = 'Watchlist Sync';
      btn.style.color = 'var(--accent)';
    }, 3000);
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Scrobble Audit
// ═══════════════════════════════════════════════════════════════════════════

function toggleAudit() {
  const panel = document.getElementById('auditPanel');
  const btn = document.getElementById('btnToggleAudit');
  if (panel.style.display === 'none') {
    panel.style.display = 'block';
    btn.textContent = 'Hide Audit ▴';
    runAudit();
  } else {
    panel.style.display = 'none';
    btn.textContent = 'Run Audit ▾';
  }
}

let _auditData = null;

async function runAudit() {
  const el = document.getElementById('auditTable');
  const summary = document.getElementById('auditSummary');
  el.innerHTML = '<div style="color:var(--text-dim);padding:8px;">Comparing Emby played items against Simkl history… this may take a moment.</div>';
  summary.innerHTML = '';
  document.getElementById('btnBackfillAll').style.display = 'none';
  document.getElementById('btnClearDismissals').style.display = 'none';
  try {
    const r = await fetch(API + `/api/scrobble-audit/${currentUserId}?force=true`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    _auditData = data;
    const movies = data.movies || [];
    const shows = data.shows || [];
    const s = data.summary || {};

    _updateAuditSummary();

    if (movies.length === 0 && shows.length === 0) {
      el.innerHTML = '<div style="color:var(--green);padding:8px;">✓ Everything in sync — no missed scrobbles found.</div>';
      return;
    }

    document.getElementById('btnBackfillAll').style.display = 'inline-block';

    // Movies table (unchanged layout)
    let html = '';
    if (movies.length > 0) {
      html += '<div style="font-weight:600;margin-bottom:6px;">Movies</div>';
      html += '<table class="queue-table"><thead><tr>' +
        '<th>Title</th><th style="width:70px;">Year</th><th style="width:110px;">Date Played</th><th style="width:140px;">Action</th>' +
        '</tr></thead><tbody>';
      movies.forEach((item, idx) => {
        let datePlayed = '—';
        if (item.last_played) {
          try {
            const d = new Date(item.last_played).toLocaleDateString();
            datePlayed = item.date_source === 'added' ? d + ' *' : d;
          } catch(e) {}
        }
        const eid = item.emby_id || '';
        html += `<tr id="audit-row-m-${eid}" data-emby-id="${eid}">
          <td>${(item.title || '').replace(/</g,'&lt;')}</td>
          <td class="queue-source">${item.year || '—'}</td>
          <td class="queue-source">${datePlayed}</td>
          <td style="display:flex;gap:4px;"><button class="btn btn-sm btn-green" data-audit-btn="m-${eid}" onclick="backfillMovie('${eid}')">Backfill</button><button class="btn btn-sm" style="background:var(--surface-hover);font-size:0.72rem;" onclick="dismissAuditItem('${eid}','m')" title="Hide from audit">✕</button></td>
        </tr>`;
      });
      html += '</tbody></table>';
    }

    // Shows table — one row per series with episode count, expandable
    if (shows.length > 0) {
      const totalEps = shows.reduce((sum, s) => sum + (s.episode_count || 0), 0);
      html += `<div style="font-weight:600;margin:16px 0 6px;">TV Shows <span style="font-weight:400;color:var(--text-dim);font-size:0.82rem;">(${totalEps} episodes across ${shows.length} shows)</span></div>`;
      html += '<table class="queue-table"><thead><tr>' +
        '<th>Series</th><th style="width:80px;">Episodes</th><th style="width:110px;">Last Played</th><th style="width:140px;">Action</th>' +
        '</tr></thead><tbody>';
      shows.forEach((show, idx) => {
        let datePlayed = '—';
        if (show.last_played) {
          try { datePlayed = new Date(show.last_played).toLocaleDateString(); } catch(e) {}
        }
        const epCount = show.episode_count || (show.episodes || []).length || 1;
        const sid = show.emby_id || '';
        html += `<tr id="audit-row-s-${sid}" data-emby-id="${sid}" style="cursor:pointer;" onclick="toggleAuditEpisodes('${sid}')">
          <td><span style="color:var(--text-dim);margin-right:4px;">▸</span> ${(show.title || '').replace(/</g,'&lt;')} <span style="color:var(--text-dim);font-size:0.78rem;">(${show.year || ''})</span></td>
          <td class="queue-source" style="color:var(--orange);font-weight:600;">${epCount}</td>
          <td class="queue-source">${datePlayed}</td>
          <td style="display:flex;gap:4px;"><button class="btn btn-sm btn-green" data-audit-btn="s-${sid}" onclick="event.stopPropagation(); backfillShow('${sid}')">Backfill</button><button class="btn btn-sm" style="background:var(--surface-hover);font-size:0.72rem;" onclick="event.stopPropagation(); dismissAuditItem('${sid}','s')" title="Hide from audit">✕</button></td>
        </tr>`;
        // Hidden episode detail rows
        if (show.episodes && show.episodes.length > 0) {
          html += `<tr id="audit-eps-${sid}" style="display:none;"><td colspan="4" style="padding:0;">` +
            '<div style="padding:6px 12px 8px 28px; background:var(--surface-alt, rgba(0,0,0,0.15));">' +
            show.episodes.map(ep => {
              let epDate = '—';
              if (ep.last_played) {
                try {
                  const d = new Date(ep.last_played).toLocaleDateString();
                  epDate = ep.date_source === 'added' ? d + ' *' : d;
                } catch(e) {}
              }
              return `<div style="display:flex;justify-content:space-between;padding:2px 0;font-size:0.82rem;">` +
                `<span style="color:var(--text-dim);">S${String(ep.season).padStart(2,'0')}E${String(ep.episode).padStart(2,'0')}</span>` +
                `<span style="flex:1;margin:0 10px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${(ep.title || '').replace(/</g,'&lt;')}</span>` +
                `<span style="color:var(--text-dim);font-size:0.78rem;">${epDate}</span></div>`;
            }).join('') +
            '</div></td></tr>';
        }
      });
      html += '</tbody></table>';
    }

    el.innerHTML = html;
  } catch(e) {
    el.innerHTML = `<div style="color:var(--red);padding:8px;">Failed: ${esc(e.message)}</div>`;
  }
}

function toggleAuditEpisodes(eid) {
  const row = document.getElementById('audit-eps-' + eid);
  if (!row) return;
  const parentRow = document.getElementById('audit-row-s-' + eid);
  if (row.style.display === 'none') {
    row.style.display = '';
    if (parentRow) parentRow.querySelector('span').textContent = '▾';
  } else {
    row.style.display = 'none';
    if (parentRow) parentRow.querySelector('span').textContent = '▸';
  }
}

function _updateAuditSummary() {
  if (!_auditData) return;
  const movies = _auditData.movies || [];
  const shows = _auditData.shows || [];
  const totalEps = shows.reduce((sum, s) => sum + (s.episode_count || (s.episodes || []).length || 0), 0);
  const s = _auditData.summary || {};
  const dismissed = s.dismissed_count || 0;
  const summary = document.getElementById('auditSummary');
  if (!summary) return;
  let html = `<div style="font-size:0.82rem; color:var(--text-dim);">` +
    `Emby played: ${s.emby_movies_played || 0} movies, ${s.emby_shows_played || 0} shows · ` +
    `Simkl watched: ${s.simkl_movies_watched || 0} movies, ${s.simkl_shows_watched || 0} shows · ` +
    `<span style="color:${(movies.length + totalEps) > 0 ? 'var(--orange)' : 'var(--green)'}; font-weight:600;">` +
    `${movies.length} movies, ${totalEps} episodes missed</span>`;
  if (dismissed > 0) {
    html += ` · <span style="color:var(--text-dim);">${dismissed} dismissed</span>`;
  }
  html += `</div>`;
  summary.innerHTML = html;
  const btnAll = document.getElementById('btnBackfillAll');
  if (btnAll) btnAll.style.display = (movies.length + shows.length) > 0 ? '' : 'none';
  const btnClear = document.getElementById('btnClearDismissals');
  if (btnClear) btnClear.style.display = dismissed > 0 ? '' : 'none';
}

function _removeAuditRow(id) {
  const el = document.getElementById(id);
  if (el) el.remove();
}

async function backfillMovie(embyId) {
  if (!_auditData) return;
  const movies = _auditData.movies || [];
  const idx = movies.findIndex(m => m.emby_id === embyId);
  if (idx < 0) return;
  const item = movies[idx];
  const btn = document.querySelector(`[data-audit-btn="m-${embyId}"]`);
  if (btn) { btn.disabled = true; btn.textContent = '⏳'; }
  try {
    const r = await fetch(API + `/api/scrobble-audit/${currentUserId}/backfill`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({items: [item]}),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    movies.splice(idx, 1);
    _removeAuditRow('audit-row-m-' + embyId);
    _updateAuditSummary();
  } catch(e) {
    if (btn) { btn.textContent = '✗'; btn.style.background = 'var(--red)'; btn.disabled = false; }
  }
}

async function backfillShow(embyId) {
  if (!_auditData) return;
  const shows = _auditData.shows || [];
  const idx = shows.findIndex(s => s.emby_id === embyId);
  if (idx < 0) return;
  const show = shows[idx];
  const btn = document.querySelector(`[data-audit-btn="s-${embyId}"]`);
  if (btn) { btn.disabled = true; const ec = show.episode_count || (show.episodes || []).length || 0; btn.textContent = `⏳ ${ec} eps`; }
  try {
    const r = await fetch(API + `/api/scrobble-audit/${currentUserId}/backfill`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({items: [show]}),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    shows.splice(idx, 1);
    _removeAuditRow('audit-row-s-' + embyId);
    _removeAuditRow('audit-eps-' + embyId);
    _updateAuditSummary();
  } catch(e) {
    if (btn) { btn.textContent = '✗'; btn.style.background = 'var(--red)'; btn.disabled = false; }
  }
}

async function backfillAll() {
  const btn = document.getElementById('btnBackfillAll');
  btn.disabled = true;
  btn.textContent = '⏳ Backfilling…';
  try {
    const r = await fetch(API + `/api/scrobble-audit/${currentUserId}/backfill-all`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    // Build per-provider result summary
    const parts = [];
    const s = data.simkl;
    if (s) {
      const sParts = [];
      if (s.movies) sParts.push(`${s.movies} movies`);
      if (s.shows) sParts.push(`${s.shows} shows`);
      if (s.episodes) sParts.push(`${s.episodes} episodes`);
      if (sParts.length) parts.push('Simkl: ' + sParts.join(', '));
    }
    const m = data.mdblist;
    if (m && m.active !== false && !m.error) {
      const mParts = [];
      if (m.movies) mParts.push(`${m.movies} movies`);
      if (m.shows) mParts.push(`${m.shows} shows`);
      if (m.episodes) mParts.push(`${m.episodes} episodes`);
      if (mParts.length) parts.push('MDBList: ' + mParts.join(', '));
    }
    const summary = parts.length ? parts.join(' · ') : `${data.added || 0} added`;
    btn.textContent = `✓ ${summary}`;
    btn.style.background = 'var(--green)';
    btn.style.fontSize = '0.72rem';
    // Refresh the audit after a moment
    setTimeout(() => runAudit(), 2000);
  } catch(e) {
    btn.textContent = '✗ Failed';
    btn.style.background = 'var(--red)';
    btn.disabled = false;
  }
}

async function dismissAuditItem(embyId, kind) {
  try {
    const r = await fetch(API + `/api/scrobble-audit/${currentUserId}/dismiss`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({emby_id: embyId}),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    if (kind === 'm') {
      const mi = (_auditData.movies || []).findIndex(m => m.emby_id === embyId);
      if (mi >= 0) _auditData.movies.splice(mi, 1);
      _removeAuditRow('audit-row-m-' + embyId);
    } else {
      const si = (_auditData.shows || []).findIndex(s => s.emby_id === embyId);
      if (si >= 0) _auditData.shows.splice(si, 1);
      _removeAuditRow('audit-row-s-' + embyId);
      _removeAuditRow('audit-eps-' + embyId);
    }
    _updateAuditSummary();
  } catch(e) {
    // silent fail
  }
}

async function clearAllDismissals() {
  const btn = document.getElementById('btnClearDismissals');
  if (!confirm('Clear all dismissed items? They will reappear in the next audit run.')) return;
  if (btn) { btn.disabled = true; btn.textContent = '⏳ Clearing…'; }
  try {
    const r = await fetch(API + `/api/scrobble-audit/${currentUserId}/clear-dismissals`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    if (btn) { btn.textContent = `✓ ${data.cleared || 0} cleared`; btn.style.background = 'var(--green)'; }
    // Re-run audit to show previously dismissed items
    setTimeout(() => runAudit(), 1500);
  } catch(e) {
    if (btn) { btn.textContent = '✗ Failed'; btn.style.background = 'var(--red)'; btn.disabled = false; }
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Taste Drift
// ═══════════════════════════════════════════════════════════════════════════

function toggleDrift() {
  const panel = document.getElementById('driftPanel');
  const btn = document.getElementById('btnToggleDrift');
  if (panel.style.display === 'none') {
    panel.style.display = 'block';
    btn.textContent = 'Hide Drift ▴';
    viewDrift();
  } else {
    panel.style.display = 'none';
    btn.textContent = 'View Drift ▾';
  }
}

async function viewDrift() {
  const el = document.getElementById('driftTable');
  const summary = document.getElementById('driftSummary');
  el.innerHTML = '<div style="color:var(--text-dim);padding:8px;">Loading drift data…</div>';
  summary.innerHTML = '';
  try {
    const r = await fetch(API + `/ml/drift/${currentUserId}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();

    summary.innerHTML = `<div style="font-size:0.82rem; color:var(--text-dim);">${data.summary || 'No data yet.'}<br>` +
      `<span style="font-size:0.75rem;">${data.snapshots?.length || 0} snapshot${(data.snapshots?.length || 0) !== 1 ? 's' : ''} recorded</span></div>`;

    const changes = data.changes || [];
    if (changes.length === 0) {
      el.innerHTML = '<div style="color:var(--text-dim);padding:8px;">No significant drift detected yet. Train the model at least twice to start tracking.</div>';
      return;
    }

    el.innerHTML = changes.map(c => {
        const arrow = c.direction === 'up' ? '▲' : '▼';
        const color = c.direction === 'up' ? 'var(--green)' : 'var(--red)';
        const desc = c.description || c.name;
        const badge = c.category;
        return `<div style="display:flex; align-items:flex-start; gap:10px; padding:8px 4px; border-bottom:1px solid var(--border);">
          <span style="color:${color}; font-weight:700; font-size:1rem; flex-shrink:0; margin-top:1px;">${arrow}</span>
          <div style="flex:1; min-width:0;">
            <div style="font-size:0.85rem; line-height:1.4;">${esc(desc)}</div>
            <span class="queue-source" style="font-size:0.7rem; margin-top:3px; display:inline-block;">${esc(badge)}</span>
          </div>
        </div>`;
      }).join('');
  } catch(e) {
    el.innerHTML = `<div style="color:var(--red);padding:8px;">Failed: ${esc(e.message)}</div>`;
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Toast Notifications
// ═══════════════════════════════════════════════════════════════════════════

const _JOB_LABELS = {
  smart_queue: 'Smart Queue',
  ml_retrain: 'ML Retrain',
  universe_scan: 'Universe Scan',
  library_cache_rebuild: 'Library Cache',
  bias_analysis: 'Bias Analysis',
  ssl_cert_check: 'SSL Check',
  heartbeat: 'Heartbeat',
};

const _MAX_TOASTS = 3;

function showToast(job, status, durationS, error) {
  const container = document.getElementById('toastContainer');
  const label = _JOB_LABELS[job] || job;
  const isOk = status === 'ok';

  // Enforce max visible toasts — dismiss oldest
  while (container.children.length >= _MAX_TOASTS) {
    dismissToast(container.firstElementChild);
  }

  const el = document.createElement('div');
  el.className = `toast ${isOk ? 'toast-ok' : 'toast-error'}`;

  let detail = isOk
    ? (durationS != null ? `Completed in ${durationS}s` : 'Completed')
    : `Failed${error ? ': ' + esc(error.slice(0, 80)) : ''}`;

  el.innerHTML = `<div class="toast-job">${esc(label)}</div>` +
    `<div class="toast-detail">${detail}</div>` +
    `<button class="toast-close" onclick="dismissToast(this.parentElement)">&times;</button>`;

  container.appendChild(el);

  // Auto-dismiss after 10 seconds
  setTimeout(() => dismissToast(el), 10000);
}

function dismissToast(el) {
  if (!el || !el.parentElement) return;
  el.classList.add('toast-out');
  setTimeout(() => el.remove(), 250);
}

function showNotification(message) {
  const container = document.getElementById('toastContainer');
  while (container.children.length >= _MAX_TOASTS) {
    dismissToast(container.firstElementChild);
  }
  const el = document.createElement('div');
  el.className = 'toast toast-ok';
  el.innerHTML = `<div class="toast-detail">${esc(message)}</div>` +
    `<button class="toast-close" onclick="dismissToast(this.parentElement)">&times;</button>`;
  container.appendChild(el);
  setTimeout(() => dismissToast(el), 10000);
}


// ═══════════════════════════════════════════════════════════════════════════
// Consolidated dashboard poll (replaces separate activity + health + job-completions)
// ═══════════════════════════════════════════════════════════════════════════

async function dashboardPoll() {
  try {
    const url = activeCategory
      ? API + `/api/dashboard-poll?category=${activeCategory}`
      : API + '/api/dashboard-poll';
    const r = await fetch(url);
    if (!r.ok) return;
    const d = await r.json();

    // --- Health ---
    const h = d.health;
    document.getElementById('apiStatus').textContent = h.status === 'ok' ? '✓ Connected' : '✗ Error';
    if (h.library_cache) {
      const lc = h.library_cache;
      document.getElementById('cacheItems').textContent = (lc.items || 0) + ' items';
      const ck = document.getElementById('statCachedKeys');
      const hr = document.getElementById('statHitRate');
      const lr = document.getElementById('statLastRebuild');
      if (ck) ck.textContent = lc.cached_keys || 0;
      if (hr) hr.textContent = (lc.hit_rate || 0).toFixed(0) + '%';
      if (lr) lr.textContent = lc.last_rebuild
        ? new Date(lc.last_rebuild).toLocaleString([], {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'})
        : 'Never';
    }
    // Refresh library counts only when a real webhook bumped the version
    const _v = (h.library_cache || {}).version || 0;
    if (_v !== _libCacheVersion) {
      _libCacheVersion = _v;
      loadLibraries();
    }

    // --- Activity ---
    const entries = (d.activity || []).filter(e => !e.msg.startsWith('Webhook received') && !e.msg.includes('_UNPACK_') && !e.msg.toLowerCase().includes('unpack'));
    const el = document.getElementById('globalLog');
    if (el) {
      const clientEntries = el.querySelectorAll('.entry.client-event');
      const clientHtml = Array.from(clientEntries).map(e => e.outerHTML).join('');
      if (!entries.length && !clientHtml) {
        el.innerHTML = '<div class="entry" style="color:var(--text-dim);">No activity yet</div>';
      } else {
        const apiHtml = entries.map(e => {
          let icon = '•';
          const m = e.msg.replace(/ — not in smart queue$/, '').replace(/ — no provider IDs to match$/, '');
          if (m.startsWith('Started Watching:')) icon = '▶️';
          else if (m.startsWith('Stopped Watching:')) icon = m.includes('Synced') ? '✅' : '⏹️';
          else if (m.includes(': Paused')) icon = '⏸️';
          else if (m.includes(': Continued')) icon = '▶️';
          else if (m.startsWith('▶ Simkl watching') || m.startsWith('▶ Simkl resumed')) icon = '▶️';
          else if (m.startsWith('⏸')) icon = '⏸️';
          else if (m.startsWith('⏹') || m.includes('stopped')) icon = '⏹️';
          else if (m.startsWith('✓ Synced')) icon = '✅';
          else if (m.startsWith('✓')) icon = '✅';
          else if (m.startsWith('✗') || m.startsWith('⚠')) icon = '❌';
          else if (m.includes('Watched:')) icon = '⏹️';
          else if (m.includes('webhook')) icon = '📡';
          else if (m.includes('party')) icon = '🎉';
          else if (m.includes('Queue') || m.includes('queue')) icon = '🎯';
          else if (m.includes('universe') || m.includes('Universe')) icon = '🌌';
          else if (m.includes('ML') || m.includes('Train') || m.includes('predict')) icon = '🤖';
          else if (m.includes('cache') || m.includes('Cache')) icon = '💾';
          else if (e.cat === 'simkl') icon = '🔄';
          else if (e.cat === 'webhook') icon = '📡';
          let cls = '';
          // Category-based colouring (from backend category field)
          if (e.cat === 'play-start') cls = 'play-start';
          else if (e.cat === 'play-stop') cls = m.includes('Sync error') ? 'err' : 'play-stop';
          else if (e.cat === 'library-movie') cls = 'lib-movie';
          else if (e.cat === 'library-episode') cls = 'lib-episode';
          else if (m.includes('Synced to MDBList')) cls = 'mdblist';
          else if (m.startsWith('✓ Simkl scrobbled') || m.includes('Synced to Simkl')) cls = 'simkl-ok';
          else if (m.startsWith('✓')) cls = 'ok';
          else if (m.startsWith('✗') || m.startsWith('⚠')) cls = 'err';
          return `<div class="entry ${cls}"><span style="opacity:0.45;font-size:0.78rem;">${e.ts.split(' ')[1]}</span>  ${icon} ${esc(m)}</div>`;
        }).join('');
        el.innerHTML = clientHtml + apiHtml;
      }
    }

    // --- Job completions (toasts) ---
    for (const ev of (d.job_completions || [])) {
      if (ev.job === 'heartbeat') continue;
      showToast(ev.job, ev.status, ev.duration_s, ev.error);
    }
  } catch(e) {
    document.getElementById('apiStatus').textContent = '✗ Offline';
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Continue Watching (with Simkl playback sync + Emby play)
// ═══════════════════════════════════════════════════════════════════════════

let _continueLoaded = false;

async function toggleContinue() {
  const panel = document.getElementById('continuePanel');
  const btn = document.getElementById('btnToggleContinue');
  if (panel.style.display === 'none') {
    panel.style.display = 'block';
    btn.textContent = 'Hide Paused ▴';
    if (!_continueLoaded) await loadContinueWatching();
  } else {
    panel.style.display = 'none';
    btn.textContent = 'View Paused ▾';
  }
}

async function loadContinueWatching() {
  if (!currentUserId) return;
  const container = document.getElementById('continueTable');
  container.innerHTML = '<div style="color:var(--text-dim);padding:8px;">Loading…</div>';

  try {
    const r = await fetch(API + `/api/continue-watching/${currentUserId}`);
    if (!r.ok) {
      const txt = await r.text();
      container.innerHTML = `<div style="color:var(--red);padding:8px;">HTTP ${r.status}: ${esc(txt.substring(0,200))}</div>`;
      return;
    }
    const d = await r.json();
    const items = d.items || [];
    _continueLoaded = true;

    if (!items.length) {
      container.innerHTML = '<div style="color:var(--text-dim);padding:8px;">Nothing resumable right now.</div>';
      return;
    }

    container.innerHTML = `
      <div style="color:var(--text-dim);font-size:0.78rem;margin-bottom:8px;">${items.length} resumable item${items.length !== 1 ? 's' : ''}</div>
      ${items.map(item => {
        const pct = item.progress_pct || 0;
        const daysText = item.days_ago != null
          ? (item.days_ago === 0 ? 'Today' : item.days_ago === 1 ? 'Yesterday' : `${item.days_ago} days ago`)
          : '';
        const barColor = item.days_ago > 180 ? 'var(--red)' : item.days_ago > 30 ? 'var(--orange)' : 'var(--green)';
        const imdbLink = item.imdb_id
          ? ` <a href="https://www.imdb.com/title/${item.imdb_id}/" target="_blank" style="color:#f5c518;font-size:0.7rem;font-weight:700;text-decoration:none;border:1px solid rgba(245,197,24,0.3);padding:1px 5px;border-radius:3px;">IMDb</a>`
          : '';

        let subtitle = '';
        if (item.type === 'movie') {
          subtitle = '🎬 Movie · ' + pct + '% watched · ' + daysText;
        } else {
          const epCount = item.episode_count || 0;
          const epList = (item.episodes || []).map(e =>
            'S' + String(e.season).padStart(2,'0') + 'E' + String(e.episode).padStart(2,'0') + ' (' + e.progress_pct + '%)'
          ).join(', ');
          subtitle = '📺 ' + epCount + ' episode' + (epCount !== 1 ? 's' : '') + ' in progress · ' + daysText;
          if (epList) subtitle += '<br><span style="font-size:0.7rem;color:var(--text-dim);">' + epList + '</span>';
        }

        // For shows with multiple episodes, show episode picker; for single episode or movies, play directly
        let playBtn = '';
        if (item.type === 'show' && (item.episodes || []).length > 1) {
          // Multiple episodes — show picker button
          const epDataAttr = encodeURIComponent(JSON.stringify((item.episodes || []).map(e => ({
            id: e.emby_id, s: e.season, e: e.episode, pct: e.progress_pct, ticks: e.position_ticks || 0, title: e.title
          }))));
          playBtn = '<button onclick="pickEpisodeAndPlay(this)" data-eps="' + epDataAttr + '" style="background:none;border:1px solid var(--green);color:var(--green);padding:2px 7px;border-radius:4px;cursor:pointer;font-size:0.72rem;white-space:nowrap;" title="Choose episode">▶ Play</button>';
        } else {
          const playId = item.type === 'show' ? (item.resume_emby_id || item.emby_id) : item.emby_id;
          const playTicks = item.type === 'show' ? (item.resume_ticks || 0) : (item.position_ticks || 0);
          playBtn = playId
            ? '<button onclick="pickDeviceAndPlay(\'' + playId + '\',' + playTicks + ')" style="background:none;border:1px solid var(--blue);color:var(--blue);padding:2px 7px;border-radius:4px;cursor:pointer;font-size:0.72rem;white-space:nowrap;" title="Play on Emby">▶ Play</button>'
            : '';
        }

        // Watch Party quick-start button — uses the series emby_id for shows
        const partyItemId = item.emby_id;
        const partyTitle = esc(item.title) + (item.year ? ' (' + item.year + ')' : '');
        const partyTicks = item.type === 'show' ? (item.resume_ticks || 0) : (item.position_ticks || 0);
        const partyPct = item.type === 'show'
          ? ((item.episodes || []).find(e => e.emby_id === item.resume_emby_id) || {}).progress_pct || pct
          : pct;
        const partyDataAttr = encodeURIComponent(JSON.stringify({id: partyItemId, title: partyTitle, ticks: partyTicks, pct: partyPct}));
        const partyBtn = WATCH_PARTY_ENABLED
          ? '<button onclick="quickStartPartyFromBtn(this)" data-party="' + partyDataAttr + '" style="background:none;border:1px solid var(--blue);color:var(--blue);padding:2px 7px;border-radius:4px;cursor:pointer;font-size:0.72rem;white-space:nowrap;" title="Start Watch Party">🎉 Party</button>'
          : '';

        return '<div style="display:flex;align-items:center;gap:12px;padding:8px 10px;border-bottom:1px solid var(--border);">'
          + '<div style="flex:1;min-width:0;">'
          + '<div style="font-weight:600;font-size:0.88rem;">' + esc(item.title) + (item.year ? ' <span style="color:var(--text-dim);font-weight:400;">(' + item.year + ')</span>' : '') + imdbLink + '</div>'
          + '<div style="font-size:0.75rem;color:var(--text-dim);margin-top:2px;">' + subtitle + '</div>'
          + '</div>'
          + '<div style="display:flex;align-items:center;gap:8px;">'
          + '<div style="width:60px;text-align:right;">'
          + '<div style="background:var(--border);border-radius:4px;height:6px;overflow:hidden;"><div style="background:' + barColor + ';height:100%;width:' + Math.min(pct, 100) + '%;border-radius:4px;"></div></div>'
          + '<div style="font-size:0.7rem;color:var(--text-dim);margin-top:2px;">' + pct + '%</div>'
          + '</div>'
          + partyBtn
          + playBtn
          + '</div>'
          + '</div>';
      }).join('')}
    `;
  } catch(e) {
    container.innerHTML = `<div style="color:var(--red);padding:8px;">Failed: ${esc(e.message)}</div>`;
  }
}

// ── Device picker + play ──

let _devicePickerTarget = null;
let _devicePickerTicks = 0;

async function pickDeviceAndPlay(embyId, ticks) {
  _devicePickerTarget = embyId;
  _devicePickerTicks = ticks || 0;
  try {
    const r = await fetch(API + `/api/remote-play/sessions/${currentUserId}`);
    const d = await r.json();
    const sessions = d.sessions || [];

    if (!sessions.length) {
      showToast('Play', 'error', null, 'No active Emby devices found');
      return;
    }

    if (sessions.length === 1) {
      await playOnDevice(embyId, sessions[0].session_id, sessions[0].device_name, _devicePickerTicks);
      return;
    }

    // Multiple devices — show picker
    let pickerHtml = '<div id="devicePicker" style="position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.7);z-index:9999;display:flex;align-items:center;justify-content:center;" onclick="if(event.target===this)this.remove()">';
    pickerHtml += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:20px;min-width:260px;max-width:400px;">';
    pickerHtml += '<div style="font-weight:600;margin-bottom:12px;font-size:0.92rem;">Choose device</div>';
    sessions.forEach(s => {
      const label = s.device_name + (s.client ? ' (' + s.client + ')' : '') + (s.now_playing ? ' — playing: ' + s.now_playing : '');
      pickerHtml += '<button onclick="document.getElementById(\'devicePicker\').remove();playOnDevice(\'' + embyId + '\',\'' + s.session_id + '\',\'' + s.device_name.replace(/'/g,"\\'") + '\',' + _devicePickerTicks + ')" style="display:block;width:100%;text-align:left;background:var(--surface-hover);border:1px solid var(--border);color:var(--text);padding:10px 14px;border-radius:6px;cursor:pointer;margin-bottom:6px;font-size:0.85rem;">' + label + '</button>';
    });
    pickerHtml += '<button onclick="document.getElementById(\'devicePicker\').remove()" style="display:block;width:100%;text-align:center;background:none;border:1px solid var(--border);color:var(--text-dim);padding:8px;border-radius:6px;cursor:pointer;font-size:0.82rem;margin-top:4px;">Cancel</button>';
    pickerHtml += '</div></div>';
    document.body.insertAdjacentHTML('beforeend', pickerHtml);
  } catch(e) {
    showToast('Play', 'error', null, 'Failed to fetch devices: ' + e.message);
  }
}

async function playOnDevice(embyId, sessionId, deviceName, ticks) {
  try {
    const r = await fetch(API + '/api/play-on-session', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({user_id: currentUserId, emby_item_id: embyId, session_id: sessionId, start_position_ticks: ticks || 0}),
    });
    const d = await r.json();
    if (d.status === 'playing') {
      showToast('▶ ' + deviceName, 'ok');
    } else {
      showToast('Play', 'error', null, d.message || 'Play failed');
    }
  } catch(e) {
    showToast('Play', 'error', null, e.message);
  }
}

function pickEpisodeAndPlay(btnEl) {
  const eps = JSON.parse(decodeURIComponent(btnEl.getAttribute('data-eps')));
  // Show episode picker modal
  let pickerHtml = '<div id="episodePicker" style="position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.7);z-index:9999;display:flex;align-items:center;justify-content:center;" onclick="if(event.target===this)this.remove()">';
  pickerHtml += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:20px;min-width:280px;max-width:420px;">';
  pickerHtml += '<div style="font-weight:600;margin-bottom:12px;font-size:0.92rem;">Choose episode to resume</div>';
  eps.forEach(ep => {
    const label = 'S' + String(ep.s).padStart(2,'0') + 'E' + String(ep.e).padStart(2,'0')
      + (ep.title ? ' — ' + ep.title : '') + ' (' + ep.pct + '%)';
    pickerHtml += '<button onclick="document.getElementById(\'episodePicker\').remove();pickDeviceAndPlay(\'' + ep.id + '\',' + (ep.ticks||0) + ')" style="display:block;width:100%;text-align:left;background:var(--surface-hover);border:1px solid var(--border);color:var(--text);padding:10px 14px;border-radius:6px;cursor:pointer;margin-bottom:6px;font-size:0.85rem;">'
      + '<span style="color:var(--green);margin-right:6px;">▶</span>' + label + '</button>';
  });
  pickerHtml += '<button onclick="document.getElementById(\'episodePicker\').remove()" style="display:block;width:100%;text-align:center;background:none;border:1px solid var(--border);color:var(--text-dim);padding:8px;border-radius:6px;cursor:pointer;font-size:0.82rem;margin-top:4px;">Cancel</button>';
  pickerHtml += '</div></div>';
  document.body.insertAdjacentHTML('beforeend', pickerHtml);
}

// ── Simkl playback sync ──

async function syncSimklPlayback() {
  const btn = document.getElementById('btnSimklSync');
  btn.disabled = true;
  btn.textContent = '🔄 Syncing…';

  try {
    const r = await fetch(API + `/api/playback-sync/${currentUserId}`);
    const d = await r.json();
    const items = d.items || [];
    const unsynced = items.filter(i => !i.synced);
    const stale = items.filter(i => i.days_stale != null && i.days_stale > 30);

    let msg = items.length + ' item(s) on Simkl';
    if (unsynced.length) msg += ', ' + unsynced.length + ' out of sync with Emby';
    if (stale.length) msg += ', ' + stale.length + ' stale (>30d)';

    // Show sync details below the button
    const container = document.getElementById('continueTable');
    let syncHtml = '<div style="background:var(--surface-hover);border:1px solid var(--border);border-radius:6px;padding:10px;margin-bottom:12px;">';
    syncHtml += '<div style="font-size:0.82rem;font-weight:600;color:var(--accent);margin-bottom:6px;">🔄 Simkl Playback Status</div>';
    syncHtml += '<div style="font-size:0.78rem;color:var(--text-dim);margin-bottom:8px;">' + msg + '</div>';

    if (items.length) {
      items.forEach(item => {
        const syncColor = item.synced ? 'var(--green)' : 'var(--orange)';
        const syncIcon = item.synced ? '✓' : '⚠';
        const staleTag = item.days_stale != null && item.days_stale > 30
          ? ' <span style="color:var(--red);font-size:0.68rem;">(stale ' + item.days_stale + 'd)</span>' : '';
        const epLabel = item.episode ? ' <span style="color:var(--text-dim);font-size:0.75rem;">' + item.episode + '</span>' : '';
        const simklP = 'Simkl: ' + item.simkl_progress + '%';
        const embyP = item.emby_progress != null ? ' · Emby: ' + item.emby_progress + '%' : ' · not in Emby';
        const removeBtn = '<button onclick="removeSimklPb(' + item.simkl_playback_id + ')" style="background:none;border:1px solid var(--red);color:var(--red);padding:1px 5px;border-radius:3px;cursor:pointer;font-size:0.68rem;margin-left:6px;" title="Remove from Simkl">✕</button>';

        syncHtml += '<div style="display:flex;align-items:center;justify-content:space-between;padding:4px 8px;border-bottom:1px solid var(--border);font-size:0.82rem;">'
          + '<span style="color:' + syncColor + ';margin-right:5px;">' + syncIcon + '</span>'
          + '<span style="flex:1;">' + esc(item.title) + epLabel + staleTag + '</span>'
          + '<span style="font-size:0.72rem;color:var(--text-dim);white-space:nowrap;">' + simklP + embyP + removeBtn + '</span>'
          + '</div>';
      });
    }

    syncHtml += '</div>';

    // Prepend sync results above existing continue watching list
    const existing = container.innerHTML;
    // Remove any previous sync block
    const cleaned = existing.replace(/<div style="background:var\(--surface-hover\).*?<\/div>\s*<\/div>/s, '');
    container.innerHTML = syncHtml + cleaned;

    showToast('Simkl Sync', unsynced.length ? 'error' : 'ok', null, unsynced.length ? msg : null);
  } catch(e) {
    showToast('Simkl Sync', 'error', null, e.message);
  } finally {
    btn.disabled = false;
    btn.textContent = '🔄 Simkl Sync';
  }
}

async function removeSimklPb(playbackId) {
  try {
    const r = await fetch(API + `/api/playback-sync/${currentUserId}/${playbackId}`, {method:'DELETE'});
    if (r.ok) {
      showToast('Simkl Sync', 'ok');
      await syncSimklPlayback();
    }
  } catch(e) { /* ignore */ }
}


// ── Watch Party Quick-Start ──

function quickStartPartyFromBtn(btnEl) {
  const data = JSON.parse(decodeURIComponent(btnEl.getAttribute('data-party')));
  quickStartParty(data.id, data.title, data.ticks || 0, data.pct || 0);
}

async function quickStartParty(embyItemId, itemTitle, resumeTicks, resumePct) {
  if (!currentUserId) { showToast('Party', 'error', null, 'Select a user first'); return; }

  // Fetch all active server sessions grouped by user
  let users;
  try {
    const r = await fetch(API + '/api/watch-party/server-sessions');
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const d = await r.json();
    users = d.users || [];
  } catch(e) {
    showToast('Party', 'error', null, 'Failed to fetch sessions: ' + e.message);
    return;
  }

  if (!users.length) {
    showToast('Party', 'error', null, 'No active Emby devices found on the server');
    return;
  }

  // Build the multi-user device picker modal
  let html = '<div id="partyPicker" style="position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.7);z-index:9999;display:flex;align-items:center;justify-content:center;" onclick="if(event.target===this)this.remove()">';
  html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:24px;min-width:320px;max-width:480px;max-height:80vh;overflow-y:auto;">';
  html += '<div style="font-weight:700;font-size:1rem;margin-bottom:4px;">🎉 Quick Watch Party</div>';
  html += '<div style="font-size:0.82rem;color:var(--text-dim);margin-bottom:16px;">' + itemTitle + '</div>';

  // Resume vs start-from-beginning toggle
  const hasResume = resumeTicks > 0;
  html += '<div style="background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:10px 12px;margin-bottom:14px;">';
  html += '<div style="font-size:0.78rem;color:var(--text-dim);margin-bottom:8px;">Start position</div>';
  html += '<label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-size:0.83rem;margin-bottom:6px;" for="ppResume">';
  html += '<input type="radio" name="ppStartPos" id="ppResume" value="resume" ' + (hasResume ? 'checked' : '') + ' style="accent-color:var(--accent);" />';
  html += 'Resume from ' + Math.round(resumePct) + '%';
  if (!hasResume) html += ' <span style="color:var(--text-dim);font-size:0.72rem;">(no position saved)</span>';
  html += '</label>';
  html += '<label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-size:0.83rem;" for="ppBeginning">';
  html += '<input type="radio" name="ppStartPos" id="ppBeginning" value="beginning" ' + (!hasResume ? 'checked' : '') + ' style="accent-color:var(--accent);" />';
  html += 'Start from beginning';
  html += '</label>';
  html += '</div>';

  html += '<div style="font-size:0.78rem;color:var(--text-dim);margin-bottom:12px;">Select devices to include in the party.</div>';

  users.forEach((u, ui) => {
    const isCurrentUser = u.db_user_id === currentUserId;
    html += '<div style="margin-bottom:14px;">';
    html += '<div style="font-weight:600;font-size:0.88rem;margin-bottom:6px;display:flex;align-items:center;gap:6px;">';
    html += '<span style="color:var(--accent);">👤</span> ' + u.username;
    if (isCurrentUser) html += ' <span style="font-size:0.68rem;color:var(--green);font-weight:400;">(you)</span>';
    html += '</div>';

    u.devices.forEach((d, di) => {
      const checkId = 'pp_' + ui + '_' + di;
      const checked = isCurrentUser ? 'checked' : '';
      const label = d.device_name + (d.client ? ' (' + d.client + ')' : '') + (d.now_playing ? ' — ' + d.now_playing : '');
      html += '<label style="display:flex;align-items:center;gap:8px;padding:6px 10px;background:var(--bg);border:1px solid var(--border);border-radius:6px;margin-bottom:4px;cursor:pointer;font-size:0.83rem;" for="' + checkId + '">';
      html += '<input type="checkbox" id="' + checkId + '" ' + checked + ' data-session-id="' + d.session_id + '" data-db-user-id="' + (u.db_user_id || '') + '" style="accent-color:var(--accent);" />';
      html += label;
      html += '</label>';
    });
    html += '</div>';
  });

  html += '<div style="display:flex;gap:8px;margin-top:16px;">';
  html += '<button onclick="launchQuickParty(\'' + embyItemId + '\',' + resumeTicks + ')" style="flex:1;padding:10px;background:var(--accent);color:#fff;border:none;border-radius:6px;cursor:pointer;font-weight:600;font-size:0.88rem;">Create &amp; Play</button>';
  html += '<button onclick="document.getElementById(\'partyPicker\').remove()" style="padding:10px 16px;background:none;border:1px solid var(--border);color:var(--text-dim);border-radius:6px;cursor:pointer;font-size:0.85rem;">Cancel</button>';
  html += '</div>';
  html += '</div></div>';

  document.body.insertAdjacentHTML('beforeend', html);
}

async function launchQuickParty(embyItemId, resumeTicks) {
  const picker = document.getElementById('partyPicker');
  const checks = picker.querySelectorAll('input[type="checkbox"]:checked');
  if (!checks.length) {
    showToast('Party', 'error', null, 'Select at least one device');
    return;
  }

  // Determine start position from radio selection
  const resumeRadio = picker.querySelector('input[name="ppStartPos"]:checked');
  const startTicks = (resumeRadio && resumeRadio.value === 'resume') ? resumeTicks : 0;

  // Collect selected session IDs and the set of unique DB user IDs
  const sessionIds = [];
  const userIds = new Set();
  checks.forEach(cb => {
    sessionIds.push(cb.dataset.sessionId);
    if (cb.dataset.dbUserId) userIds.add(parseInt(cb.dataset.dbUserId));
  });

  // Disable button while working
  const btn = picker.querySelector('button');
  btn.disabled = true;
  btn.textContent = 'Creating…';

  try {
    // 1. Create the party as the current user
    const createR = await fetch(API + '/party/create', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({host_user_id: currentUserId, emby_item_id: embyItemId}),
    });
    if (!createR.ok) { const d = await createR.json(); throw new Error(d.detail || 'Create failed'); }
    const party = await createR.json();
    const code = party.code;

    // 2. Join other users to the party
    for (const uid of userIds) {
      if (uid === currentUserId) continue;
      try {
        await fetch(API + '/party/join', {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({code, user_id: uid}),
        });
      } catch(e) { /* best-effort join */ }
    }

    // 3. Start playback on selected sessions
    const startR = await fetch(API + `/party/${code}/start-selected`, {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({session_ids: sessionIds, emby_item_id: embyItemId, start_position_ticks: startTicks}),
    });
    if (!startR.ok) { const d = await startR.json(); throw new Error(d.detail || 'Start failed'); }

    picker.remove();
    showToast('🎉 Party ' + code, 'ok', null, 'Playback started on ' + sessionIds.length + ' device' + (sessionIds.length !== 1 ? 's' : ''));
  } catch(e) {
    showToast('Party', 'error', null, e.message);
    btn.disabled = false;
    btn.textContent = 'Create & Play';
  }
}


// ═══════════════════════════════════════════════════════════════════════════
// New & Coming Soon (merged availability + recently arrived)
// ═══════════════════════════════════════════════════════════════════════════

let _availLoaded = false;

async function toggleAvailability() {
  const panel = document.getElementById('availPanel');
  const btn = document.getElementById('btnToggleAvail');
  if (panel.style.display === 'none') {
    panel.style.display = 'block';
    btn.textContent = 'Hide Status ▴';
    if (!_availLoaded) await loadAvailability();
  } else {
    panel.style.display = 'none';
    btn.textContent = 'View Status ▾';
  }
}

async function loadAvailability() {
  const container = document.getElementById('availTable');
  container.innerHTML = '<div style="color:var(--text-dim);padding:8px;">Loading…</div>';

  try {
    // Fetch both in parallel
    const [availRes, arrivedRes] = await Promise.all([
      fetch(API + '/api/availability'),
      fetch(API + '/api/recently-arrived'),
    ]);
    const availData = await availRes.json();
    const arrivedData = await arrivedRes.json();
    _availLoaded = true;

    let html = '';

    // ── Recently arrived section ──
    const arrivedMovies = arrivedData.arrived_movies || [];
    const arrivedShows = arrivedData.arrived_shows || [];
    const arrivedTotal = arrivedMovies.length + arrivedShows.length;
    const badge = document.getElementById('arrivedBadge');

    if (arrivedTotal > 0) {
      badge.textContent = arrivedTotal;
      badge.style.display = 'inline';

      html += '<div style="background:rgba(46,204,113,0.08);border:1px solid rgba(46,204,113,0.25);border-radius:6px;padding:10px;margin-bottom:12px;">';
      html += '<div style="font-size:0.82rem;font-weight:600;color:var(--green);margin-bottom:6px;">🆕 Recently Arrived (' + arrivedTotal + ')</div>';

      arrivedMovies.forEach(m => {
        html += '<div style="display:flex;align-items:center;justify-content:space-between;padding:4px 8px;border-bottom:1px solid var(--border);font-size:0.85rem;">'
          + '<span><span style="color:var(--green);margin-right:6px;">✓</span>🎬 ' + esc(m.title) + (m.year ? ' (' + m.year + ')' : '') + '</span>'
          + '<button onclick="dismissArrivedItem(\'movie\',' + (m.tmdb_id||0) + ',this)" style="background:none;border:1px solid var(--text-dim);color:var(--text-dim);padding:1px 5px;border-radius:3px;cursor:pointer;font-size:0.68rem;" title="Dismiss">✕</button>'
          + '</div>';
      });

      arrivedShows.forEach(s => {
        const newEpLabel = s.new_episodes ? ' <span style="color:var(--green);font-size:0.72rem;font-weight:600;">+' + s.new_episodes + ' new</span>' : '';
        html += '<div style="display:flex;align-items:center;justify-content:space-between;padding:4px 8px;border-bottom:1px solid var(--border);font-size:0.85rem;">'
          + '<span><span style="color:var(--green);margin-right:6px;">✓</span>📺 ' + esc(s.title) + (s.year ? ' (' + s.year + ')' : '') + ' <span style="color:var(--text-dim);font-size:0.72rem;">' + s.episodes_on_disk + '/' + s.episodes_total + ' eps</span>' + newEpLabel + '</span>'
          + '<button onclick="dismissArrivedItem(\'show\',' + (s.tvdb_id||s.id||0) + ',this)" style="background:none;border:1px solid var(--text-dim);color:var(--text-dim);padding:1px 5px;border-radius:3px;cursor:pointer;font-size:0.68rem;" title="Dismiss">✕</button>'
          + '</div>';
      });

      html += '</div>';
    } else {
      badge.style.display = 'none';
    }

    // ── Pending items section (existing availability) ──
    const pendingMovies = (availData.movies || {}).pending || [];
    const pendingShows = (availData.shows || {}).pending || [];
    const isPartial = availData.partial || false;
    const failedServers = availData.failed_servers || [];

    if (isPartial) {
      html += '<div style="background:rgba(210,153,34,0.12);border:1px solid rgba(210,153,34,0.3);border-radius:6px;padding:8px 12px;margin-bottom:10px;font-size:0.8rem;color:var(--orange);">'
        + '⚠️ ' + esc(failedServers.join(', ')) + ' unreachable — results are partial.'
        + ' <button onclick="_availLoaded=false;loadAvailability()" style="background:none;border:1px solid var(--orange);color:var(--orange);padding:2px 8px;border-radius:4px;cursor:pointer;font-size:0.75rem;margin-left:8px;">Retry</button>'
        + '</div>';
    }

    if (!pendingMovies.length && !pendingShows.length && !arrivedTotal) {
      const mTotal = (availData.movies || {}).total_monitored || 0;
      const sTotal = (availData.shows || {}).total_monitored || 0;
      container.innerHTML = html + '<div style="color:var(--green);padding:8px;">All clear! ' + mTotal + ' movies and ' + sTotal + ' shows fully available.</div>';
      return;
    }

    const statusBadge = (status) => {
      const colors = { monitored: 'var(--orange)', downloading: 'var(--blue)', partial: 'var(--accent-light)' };
      const c = colors[status] || 'var(--text-dim)';
      return '<span style="background:rgba(0,0,0,0.2);color:' + c + ';padding:2px 8px;border-radius:4px;font-size:0.72rem;font-weight:600;border:1px solid ' + c + '33;">' + status + '</span>';
    };

    if (pendingMovies.length) {
      html += '<div style="font-size:0.82rem;font-weight:600;margin:8px 0 6px;color:var(--text-dim);">🎬 Movies (' + pendingMovies.length + ' pending)</div>';
      pendingMovies.forEach(m => {
        html += '<div style="display:flex;align-items:center;justify-content:space-between;padding:6px 10px;border-bottom:1px solid var(--border);font-size:0.85rem;">'
          + '<span>' + esc(m.title) + (m.year ? ' (' + m.year + ')' : '') + '</span>'
          + '<span style="display:flex;align-items:center;gap:6px;">' + statusBadge(m.status)
          + '<span style="color:var(--text-dim);font-size:0.7rem;">' + esc(m.server) + '</span></span></div>';
      });
    }

    if (pendingShows.length) {
      html += '<div style="font-size:0.82rem;font-weight:600;margin:12px 0 6px;color:var(--text-dim);">📺 Shows (' + pendingShows.length + ' pending)</div>';
      pendingShows.forEach(s => {
        html += '<div style="display:flex;align-items:center;justify-content:space-between;padding:6px 10px;border-bottom:1px solid var(--border);font-size:0.85rem;">'
          + '<span>' + esc(s.title) + (s.year ? ' (' + s.year + ')' : '')
          + ' <span style="color:var(--text-dim);font-size:0.72rem;margin-left:6px;">' + s.episodes_on_disk + '/' + s.episodes_total + ' eps</span></span>'
          + '<span style="display:flex;align-items:center;gap:6px;">' + statusBadge(s.status)
          + '<span style="color:var(--text-dim);font-size:0.7rem;">' + esc(s.server) + '</span></span></div>';
      });
    }

    container.innerHTML = html;
  } catch(e) {
    container.innerHTML = '<div style="color:var(--red);padding:8px;">Failed: ' + esc(e.message) + '</div>';
  }
}

async function dismissArrivedItem(type, id, btnEl) {
  try {
    const r = await fetch(API + '/api/recently-arrived/dismiss-item', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({type, id}),
    });
    if (r.ok) {
      // Remove the row visually
      const row = btnEl.closest('div[style*="border-bottom"]');
      if (row) row.remove();
      // Update badge
      const badge = document.getElementById('arrivedBadge');
      const current = parseInt(badge.textContent || '0');
      if (current > 1) { badge.textContent = current - 1; }
      else { badge.style.display = 'none'; }
    }
  } catch(e) { /* ignore */ }
}


// ═══════════════════════════════════════════════════════════════════════════



// ═══════════════════════════════════════════════════════════════════════════
// Active Downloads Card
// ═══════════════════════════════════════════════════════════════════════════

async function refreshDownloadsCard() {
  try {
    const r = await fetch(API + '/api/download-queue');
    if (!r.ok) return;
    const d = await r.json();
    const items = d.downloads || [];
    const badge = document.getElementById('dlCountBadge');
    const preview = document.getElementById('dlCardPreview');

    if (items.length === 0) {
      badge.style.display = 'none';
      preview.innerHTML = '<div style="color:var(--text-dim);font-size:0.8rem;">No active downloads.</div>';
      const _grid0 = document.getElementById('featureCardsGrid');
      if (_grid0 && _grid0.style.maxHeight !== '0px') {
        requestAnimationFrame(() => { _grid0.style.maxHeight = _grid0.scrollHeight + 'px'; });
      }
      return;
    }

    badge.style.display = '';
    badge.textContent = items.length;
    badge.className = 'badge badge-on';

    // Group by title (Sonarr can have multiple episodes per show)
    const grouped = {};
    for (const dl of items) {
      const key = dl.type === 'show' ? `show:${dl.tvdb_id || dl.title}` : `movie:${dl.tmdb_id || dl.title}`;
      if (!grouped[key]) {
        grouped[key] = { title: dl.title, type: dl.type, server: dl.server, tracked_status: dl.tracked_status, items: [] };
      }
      grouped[key].items.push(dl);
    }

    const entries = Object.values(grouped);
    preview.innerHTML = entries.slice(0, 5).map(g => {
      const isWarn = g.items.some(d => d.tracked_status === 'warning');
      const barColor = isWarn ? 'var(--orange)' : 'var(--blue)';

      if (g.type === 'movie') {
        const dl = g.items[0];
        const pct = dl.progress || 0;
        const sabStatus = dl.sab_status || '';
        const etaStr = dl.sab_eta || _formatEta(dl.eta);
        const sizeStr = dl.size_mb ? `${dl.size_mb.toFixed(0)} MB` : '';
        const speedStr = dl.sab_speed ? `${dl.sab_speed}B/s` : '';
        const statusLabel = sabStatus && sabStatus !== 'Downloading' ? `<span style="font-size:0.6rem;color:var(--accent-light);margin-left:4px;">${esc(sabStatus)}</span>` : '';
        const nzoAttr = dl.download_id ? ` data-nzo="${dl.download_id}"` : '';
        return `<div style="padding:4px 0;border-bottom:1px solid var(--border);">
          <div style="display:flex;justify-content:space-between;align-items:center;">
            <span style="font-size:0.78rem;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:160px;">${esc(dl.title)}</span>
            <span style="font-size:0.68rem;color:var(--text-dim);">${esc(g.server)}</span>
          </div>
          <div class="dl-progress-wrap" style="margin-top:2px;"${nzoAttr}>
            <div class="dl-progress-bar"><div class="dl-progress-fill" style="width:${pct}%;background:${barColor};"></div></div>
            <span class="dl-progress-text">${pct.toFixed(0)}%${statusLabel}</span>
          </div>
          <div class="dl-meta" style="font-size:0.6rem;color:var(--text-dim);">${etaStr}${sizeStr ? ' · ' + sizeStr : ''}${speedStr ? ' · ' + speedStr : ''}</div>
        </div>`;
      } else {
        // Show — aggregate episodes
        const avgPct = g.items.reduce((s, d) => s + (d.progress || 0), 0) / g.items.length;
        const epLabels = g.items.map(d => d.episode_label).filter(Boolean).slice(0, 4).join(', ');
        const moreCount = g.items.length > 4 ? ` +${g.items.length - 4}` : '';
        // Use first item's SABnzbd status as representative for the group
        const sabStatus = g.items[0].sab_status || '';
        const speedStr = g.items[0].sab_speed ? `${g.items[0].sab_speed}B/s` : '';
        const statusLabel = sabStatus && sabStatus !== 'Downloading' ? ` · ${esc(sabStatus)}` : '';
        const nzoAttr = g.items[0].download_id ? ` data-nzo="${g.items[0].download_id}"` : '';
        return `<div style="padding:4px 0;border-bottom:1px solid var(--border);">
          <div style="display:flex;justify-content:space-between;align-items:center;">
            <span style="font-size:0.78rem;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:160px;">${esc(g.title)}</span>
            <span style="font-size:0.68rem;color:var(--text-dim);">${esc(g.server)}</span>
          </div>
          <div class="dl-progress-wrap" style="margin-top:2px;"${nzoAttr}>
            <div class="dl-progress-bar"><div class="dl-progress-fill" style="width:${avgPct}%;background:${barColor};"></div></div>
            <span class="dl-progress-text">${avgPct.toFixed(0)}%</span>
          </div>
          <div class="dl-meta" style="font-size:0.6rem;color:var(--text-dim);">${g.items.length} ep${g.items.length > 1 ? 's' : ''}${epLabels ? ': ' + esc(epLabels) : ''}${moreCount}${statusLabel}${speedStr ? ' · ' + speedStr : ''}</div>
        </div>`;
      }
    }).join('') + (entries.length > 5 ? `<div style="font-size:0.7rem;color:var(--text-dim);padding:4px 0;">+${entries.length - 5} more</div>` : '');

    // Resize grid AFTER innerHTML updated so scrollHeight reflects new content
    const _grid = document.getElementById('featureCardsGrid');
    if (_grid && _grid.style.maxHeight !== '0px') {
      requestAnimationFrame(() => { _grid.style.maxHeight = _grid.scrollHeight + 'px'; });
    }
  } catch(e) {
    // silent — card just shows stale data
  }
}


// ═══════════════════════════════════════════════════════════════════════════
// Initialization
// ═══════════════════════════════════════════════════════════════════════════

// Adaptive token expiry refresh: 1h normally, 5m within 30m, 1m within 5m
let _tokenRefreshTimer = null;
function scheduleTokenRefresh() {
  if (_tokenRefreshTimer) clearTimeout(_tokenRefreshTimer);
  // Find the shortest token expiry across all linked users
  let minMinsLeft = Infinity;
  for (const u of allUsers) {
    if (!u.linked || !u.token_status || u.token_status === 'expired') continue;
    const total = (u.token_days_left || 0) * 1440 + (u.token_hours_left || 0) * 60 + (u.token_minutes_left || 0);
    if (total < minMinsLeft) minMinsLeft = total;
  }
  let intervalMs;
  if (minMinsLeft <= 5) intervalMs = 60_000;       // every 1 min
  else if (minMinsLeft <= 30) intervalMs = 300_000; // every 5 min
  else intervalMs = 3600_000;                       // every 1 hour
  _tokenRefreshTimer = setTimeout(async () => {
    await loadUsers();
    scheduleTokenRefresh();
  }, intervalMs);
}

function init() {
  dashboardPoll();
  loadUsers().then(scheduleTokenRefresh);
  loadParties();
  loadSSLStatus();
  _loadArrServers();
  refreshDownloadsCard();
  setInterval(dashboardPoll, 30000); // consolidated: health + activity + job completions
  setInterval(loadParties, 60000);
  setInterval(() => { if (_schedLoaded) loadSchedulerStatus(); }, 60000);
  setInterval(loadSSLStatus, 300000);
  // Downloads card: poll every 5s only when features panel is expanded AND not in realtime mode
  setInterval(() => {
    const grid = document.getElementById('featureCardsGrid');
    if (grid && grid.style.maxHeight !== '0px' && _dlMode !== 'realtime') refreshDownloadsCard();
  }, 5000);

  // Real-time activity log via Socket.IO
  try {
    const _dashSocket = io({ path: '/ws/socket.io', transports: ['websocket'] });
    _dashSocket.on('activity_entry', function(e) {
      if (!e || !e.msg) return;
      if (e.msg.startsWith('Webhook received')) return;
      if (e.msg.includes('_UNPACK_') || e.msg.toLowerCase().includes('unpack')) return;
      if (activeCategory && e.cat !== activeCategory) return;
      const el = document.getElementById('globalLog');
      if (!el) return;
      if (el.textContent === 'No activity yet') el.innerHTML = '';
      let icon = '•';
      const m = e.msg.replace(/ — not in smart queue$/, '').replace(/ — no provider IDs to match$/, '');
      if (m.startsWith('Started Watching:')) icon = '▶️';
      else if (m.startsWith('Stopped Watching:')) icon = m.includes('Synced') ? '✅' : '⏹️';
      else if (m.includes(': Paused')) icon = '⏸️';
      else if (m.includes(': Continued')) icon = '▶️';
      else if (m.startsWith('▶ Simkl watching') || m.startsWith('▶ Simkl resumed')) icon = '▶️';
      else if (m.startsWith('⏸')) icon = '⏸️';
      else if (m.startsWith('⏹') || m.includes('stopped')) icon = '⏹️';
      else if (m.startsWith('✓ Synced')) icon = '✅';
      else if (m.startsWith('✓')) icon = '✅';
      else if (m.startsWith('✗') || m.startsWith('⚠')) icon = '❌';
      else if (m.includes('Watched:')) icon = '⏹️';
      else if (m.includes('📥 Library')) icon = '📥';
      else if (m.includes('webhook')) icon = '📡';
      else if (m.includes('party')) icon = '🎉';
      else if (m.includes('Queue') || m.includes('queue')) icon = '🎯';
      else if (m.includes('universe') || m.includes('Universe')) icon = '🌌';
      else if (m.includes('ML') || m.includes('Train') || m.includes('predict')) icon = '🤖';
      else if (m.includes('cache') || m.includes('Cache')) icon = '💾';
      else if (e.cat === 'simkl') icon = '🔄';
      else if (e.cat === 'webhook') icon = '📡';
      const ts = (e.ts || '').split(' ')[1] || '';
      let cls = '';
      if (e.cat === 'play-start') cls = 'play-start';
      else if (e.cat === 'play-stop') cls = m.includes('Sync error') ? 'err' : 'play-stop';
      else if (e.cat === 'library-movie') cls = 'lib-movie';
      else if (e.cat === 'library-episode') cls = 'lib-episode';
      else if (m.includes('Synced to MDBList')) cls = 'mdblist';
      else if (m.startsWith('✓ Simkl scrobbled') || m.includes('Synced to Simkl')) cls = 'simkl-ok';
      else if (m.startsWith('✓')) cls = 'ok';
      else if (m.startsWith('✗') || m.startsWith('⚠')) cls = 'err';
      const div = document.createElement('div');
      div.className = 'entry ' + cls;
      div.innerHTML = `<span style="opacity:0.45;font-size:0.78rem;">${ts}</span>  ${icon} ${esc(m)}`;
      el.insertBefore(div, el.querySelector('.entry:not(.client-event)'));
    });
  } catch(_e) {}
}

// Start when page loads
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
