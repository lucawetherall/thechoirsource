/**
 * @thechoirsource Dashboard — vanilla JS, no build step, no dependencies.
 *
 * Architecture:
 * - Fetches queue JSON from same-origin paths (queue/*.json)
 * - Cache-busted with ?t=Date.now() to avoid GitHub Pages CDN caching
 * - POSTs approve/reject decisions to Cloudflare Worker
 * - Settings (workerUrl, dashboardSecret) stored in localStorage
 */

'use strict';

// ============================================================
// State
// ============================================================

const state = {
  pending: [],
  approved: [],
  archive: [],
  settings: {
    workerUrl: '',
    secret: '',
  },
};

// ============================================================
// Settings
// ============================================================

function loadSettings() {
  state.settings.workerUrl = localStorage.getItem('workerUrl') || '';
  state.settings.secret = localStorage.getItem('dashboardSecret') || '';
}

function saveSettings() {
  const url = document.getElementById('worker-url').value.trim();
  const secret = document.getElementById('dashboard-secret').value.trim();
  localStorage.setItem('workerUrl', url);
  localStorage.setItem('dashboardSecret', secret);
  state.settings.workerUrl = url;
  state.settings.secret = secret;
  hideModal();
  showToast('Settings saved.', 'success');
}

function showSettingsModal() {
  document.getElementById('worker-url').value = state.settings.workerUrl;
  document.getElementById('dashboard-secret').value = state.settings.secret;
  document.getElementById('setup-modal').classList.remove('hidden');
}

function hideModal() {
  document.getElementById('setup-modal').classList.add('hidden');
}

function shouldShowSetup() {
  return !state.settings.workerUrl || !state.settings.secret;
}

// ============================================================
// Data fetching
// ============================================================

async function fetchJson(path) {
  const url = `${path}?t=${Date.now()}`;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`HTTP ${resp.status} fetching ${url}`);
  return resp.json();
}

async function loadAllQueues() {
  const results = await Promise.allSettled([
    fetchJson('queue/pending.json'),
    fetchJson('queue/approved.json'),
    fetchJson('queue/archive.json'),
  ]);

  state.pending = results[0].status === 'fulfilled' ? results[0].value : [];
  state.approved = results[1].status === 'fulfilled' ? results[1].value : [];
  state.archive = results[2].status === 'fulfilled' ? results[2].value : [];

  if (results.every(r => r.status === 'rejected')) {
    showError('pending-list', 'No data loaded. The pipeline may not have run yet, or the dashboard has not been deployed.');
    showError('approved-list', '');
    showError('archive-list', '');
    return;
  }

  updateCounts();
  renderPending();
  renderApproved();
  renderArchive();
}

// ============================================================
// Rendering helpers
// ============================================================

function updateCounts() {
  document.getElementById('count-pending').textContent = state.pending.length;
  document.getElementById('count-approved').textContent = state.approved.length;
  document.getElementById('count-archive').textContent = state.archive.length;
}

function showError(containerId, msg) {
  const el = document.getElementById(containerId);
  if (!el) return;
  if (!msg) { el.innerHTML = ''; return; }
  el.innerHTML = `
    <div class="empty-state">
      <h3>Could not load data</h3>
      <p>${escapeHtml(msg)}</p>
    </div>
  `;
}

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function formatDate(isoStr) {
  if (!isoStr) return '—';
  try {
    return new Date(isoStr).toLocaleString('en-GB', {
      dateStyle: 'medium',
      timeStyle: 'short',
      timeZone: 'Europe/London',
    });
  } catch {
    return isoStr;
  }
}

function buildVideoEl(r2Url) {
  if (!r2Url) return `<div class="clip-placeholder">Video preview unavailable — the clip may still be processing.</div>`;
  return `
    <video
      class="clip-video"
      controls
      preload="metadata"
      onerror="this.parentNode.innerHTML = '<div class=\\'clip-placeholder\\'>Video preview unavailable — the clip may still be processing.</div>'"
    >
      <source src="${escapeHtml(r2Url)}" type="video/mp4" />
      Video preview not available.
    </video>
  `;
}

// ============================================================
// Pending tab
// ============================================================

function renderPending() {
  const container = document.getElementById('pending-list');
  if (state.pending.length === 0) {
    container.innerHTML = `
      <div class="empty-state">
        <h3>No videos pending review</h3>
        <p>Run the weekly pipeline to discover and process new choir videos.</p>
      </div>
    `;
    return;
  }

  container.innerHTML = state.pending.map(item => buildPendingCard(item)).join('');
  attachPendingListeners();
}

function buildPendingCard(item) {
  const clips = item.clips || [];
  const numClips = clips.length;

  const clipsHtml = clips.map(clip => `
    <label class="clip-option ${clip.rank === 1 ? 'selected' : ''}" data-rank="${clip.rank}">
      <input
        type="radio"
        name="clip-${escapeHtml(item.youtube_id)}"
        value="${clip.rank}"
        ${clip.rank === 1 ? 'checked' : ''}
      />
      <div class="clip-label">
        <span class="rank">Option ${clip.rank}</span>
        <span>Score: ${clip.contrast_score ? clip.contrast_score.toFixed(2) : '—'}</span>
        <span>${clip.duration_seconds ? clip.duration_seconds.toFixed(0) + 's' : ''}</span>
      </div>
      ${buildVideoEl(clip.r2_url)}
    </label>
  `).join('');

  const caption = item.caption || '';
  const hashtags = item.hashtags || '';
  const fullText = caption + (hashtags ? '\n\n' + hashtags : '');
  const charCount = fullText.length;
  const instagramLimit = 2200;

  return `
    <div class="card" data-youtube-id="${escapeHtml(item.youtube_id)}" id="card-${escapeHtml(item.youtube_id)}">
      <div class="card-header">
        <div class="video-title">${escapeHtml(item.title || item.youtube_id)}</div>
        <div class="video-meta">
          <span class="meta-tag">${escapeHtml(item.channel_name || '')}</span>
          ${item.view_count ? `<span class="meta-tag">${Number(item.view_count).toLocaleString()} views</span>` : ''}
          ${item.published_at ? `<span class="meta-tag">${formatDate(item.published_at)}</span>` : ''}
          <span class="meta-tag">${escapeHtml(item.source || '')}</span>
        </div>

        ${(item.piece_title || item.composer || item.ensemble_name) ? `
        <div class="parsed-meta">
          ${item.piece_title ? `<div class="piece-title">${escapeHtml(item.piece_title)}</div>` : ''}
          ${item.composer ? `<div class="composer">${escapeHtml(item.composer)}</div>` : ''}
          ${item.ensemble_name ? `<div class="ensemble">${escapeHtml(item.ensemble_name)}</div>` : ''}
        </div>
        ` : ''}
      </div>

      ${numClips > 0 ? `
      <div class="clips-section">
        <h4>${numClips} Clip ${numClips === 1 ? 'Option' : 'Options'}</h4>
        <div class="clips-grid">
          ${clipsHtml}
        </div>
      </div>
      ` : ''}

      <div class="caption-section">
        <label for="caption-${escapeHtml(item.youtube_id)}">Caption</label>
        <textarea
          id="caption-${escapeHtml(item.youtube_id)}"
          rows="4"
          maxlength="2200"
        >${escapeHtml(caption)}</textarea>
      </div>

      <div class="caption-section">
        <label for="hashtags-${escapeHtml(item.youtube_id)}">Hashtags</label>
        <textarea
          id="hashtags-${escapeHtml(item.youtube_id)}"
          rows="2"
        >${escapeHtml(hashtags)}</textarea>
        <div class="char-count ${charCount > instagramLimit * 0.9 ? 'warning' : ''}" id="chars-${escapeHtml(item.youtube_id)}">
          ${charCount} / ${instagramLimit} characters (Instagram limit)
        </div>
      </div>

      <div class="card-actions">
        <button class="btn btn-approve" data-action="approve" data-id="${escapeHtml(item.youtube_id)}">
          ✓ Approve
        </button>
        <button class="btn btn-reject" data-action="reject" data-id="${escapeHtml(item.youtube_id)}">
          ✕ Reject
        </button>
      </div>
    </div>
  `;
}

function attachPendingListeners() {
  // Clip selection
  document.querySelectorAll('.clip-option').forEach(option => {
    option.addEventListener('click', () => {
      const card = option.closest('.card');
      card.querySelectorAll('.clip-option').forEach(o => o.classList.remove('selected'));
      option.classList.add('selected');
      const radio = option.querySelector('input[type="radio"]');
      if (radio) radio.checked = true;
    });
  });

  // Character count on textarea
  document.querySelectorAll('.card').forEach(card => {
    const ytId = card.dataset.youtubeId;
    const captionEl = document.getElementById(`caption-${ytId}`);
    const hashtagsEl = document.getElementById(`hashtags-${ytId}`);
    const charsEl = document.getElementById(`chars-${ytId}`);

    function updateCount() {
      if (!charsEl) return;
      const total = (captionEl?.value || '').length + (hashtagsEl?.value ? '\n\n'.length + hashtagsEl.value.length : 0);
      const limit = 2200;
      charsEl.textContent = `${total} / ${limit} characters (Instagram limit)`;
      charsEl.classList.toggle('warning', total > limit * 0.9);
    }

    captionEl?.addEventListener('input', updateCount);
    hashtagsEl?.addEventListener('input', updateCount);
  });

  // Approve / Reject buttons
  document.querySelectorAll('.btn-approve, .btn-reject').forEach(btn => {
    btn.addEventListener('click', () => handleAction(btn));
  });
}

async function handleAction(btn) {
  const action = btn.dataset.action;
  const ytId = btn.dataset.id;
  const card = document.getElementById(`card-${ytId}`);

  if (action === 'reject') {
    if (!confirm('Reject this video? This cannot be undone.')) return;
  }

  if (!state.settings.workerUrl || !state.settings.secret) {
    showToast('Please configure the Worker URL and secret in settings first.', 'error');
    showSettingsModal();
    return;
  }

  // Disable buttons, show spinner
  const buttons = card.querySelectorAll('.btn');
  buttons.forEach(b => b.disabled = true);
  btn.innerHTML = `<span class="spinner"></span> ${action === 'approve' ? 'Approving…' : 'Rejecting…'}`;

  const body = { secret: state.settings.secret, youtube_id: ytId, action };

  if (action === 'approve') {
    const selectedRadio = card.querySelector(`input[name="clip-${ytId}"]:checked`);
    body.selected_clip_rank = selectedRadio ? parseInt(selectedRadio.value, 10) : 1;
    body.edited_caption = document.getElementById(`caption-${ytId}`)?.value || '';
    body.edited_hashtags = document.getElementById(`hashtags-${ytId}`)?.value || '';
  }

  try {
    const resp = await fetch(state.settings.workerUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    if (!resp.ok) {
      const errorText = await resp.text();
      throw new Error(`HTTP ${resp.status}: ${errorText}`);
    }

    card.style.transition = 'opacity 0.3s';
    card.style.opacity = '0';
    setTimeout(() => {
      card.remove();
      state.pending = state.pending.filter(i => i.youtube_id !== ytId);
      document.getElementById('count-pending').textContent = state.pending.length;
    }, 300);

    showToast(
      action === 'approve' ? '✓ Approved! The action has been queued.' : '✕ Rejected.',
      action === 'approve' ? 'success' : 'info'
    );
  } catch (err) {
    buttons.forEach(b => b.disabled = false);
    btn.innerHTML = action === 'approve' ? '✓ Approve' : '✕ Reject';
    showToast(`Error: ${err.message}`, 'error');
  }
}

// ============================================================
// Approved tab
// ============================================================

function renderApproved() {
  const container = document.getElementById('approved-list');
  const sorted = [...state.approved].sort((a, b) =>
    (a.scheduled_at || '').localeCompare(b.scheduled_at || '')
  );

  if (sorted.length === 0) {
    container.innerHTML = `
      <div class="empty-state">
        <h3>No approved videos</h3>
        <p>Approve pending videos to add them to the posting queue.</p>
      </div>
    `;
    return;
  }

  container.innerHTML = sorted.map(item => {
    const selectedRank = item.selected_clip_rank || 1;
    const clip = (item.clips || []).find(c => c.rank === selectedRank) || item.clips?.[0];
    const caption = item.caption || '';
    const hashtags = item.hashtags || '';

    return `
      <div class="approved-card">
        <div>
          ${clip ? buildVideoEl(clip.r2_url) : '<div class="clip-placeholder">No clip preview</div>'}
        </div>
        <div class="approved-info">
          <div class="video-title">${escapeHtml(item.title || item.youtube_id)}</div>
          <span class="scheduled-badge">📅 ${formatDate(item.scheduled_at)}</span>
          ${item.ensemble_name ? `<div class="approved-caption" style="font-weight:600">${escapeHtml(item.ensemble_name)}</div>` : ''}
          ${caption ? `<div class="approved-caption">${escapeHtml(caption)}</div>` : ''}
          ${hashtags ? `<div class="approved-caption" style="color: var(--text-muted); font-size: 0.8rem;">${escapeHtml(hashtags)}</div>` : ''}
        </div>
      </div>
    `;
  }).join('');
}

// ============================================================
// Archive tab
// ============================================================

function renderArchive() {
  const container = document.getElementById('archive-list');

  if (state.archive.length === 0) {
    container.innerHTML = `
      <div class="empty-state">
        <h3>Archive is empty</h3>
        <p>Posted and rejected videos will appear here.</p>
      </div>
    `;
    return;
  }

  const sorted = [...state.archive].sort((a, b) => {
    const da = a.posted_at || a.rejected_at || a.added_at || '';
    const db = b.posted_at || b.rejected_at || b.added_at || '';
    return db.localeCompare(da);
  });

  container.innerHTML = `
    <div class="archive-list">
      ${sorted.map(item => {
        const date = item.posted_at || item.rejected_at || item.added_at;
        const statusClass = item.status === 'posted' ? 'status-posted' : 'status-rejected';
        return `
          <div class="archive-row">
            <div class="archive-title">${escapeHtml(item.title || item.youtube_id)}</div>
            <span class="status-badge ${statusClass}">${escapeHtml(item.status || '—')}</span>
            <span class="archive-date">${formatDate(date)}</span>
          </div>
        `;
      }).join('')}
    </div>
  `;
}

// ============================================================
// Toast notifications
// ============================================================

function showToast(message, type = 'info', duration = 4000) {
  const container = document.getElementById('toast-container');
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.textContent = message;
  container.appendChild(toast);
  setTimeout(() => {
    toast.style.transition = 'opacity 0.3s';
    toast.style.opacity = '0';
    setTimeout(() => toast.remove(), 300);
  }, duration);
}

// ============================================================
// Tab switching
// ============================================================

function setupTabs() {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const tabName = btn.dataset.tab;
      document.querySelectorAll('.tab-btn').forEach(b => {
        b.classList.remove('active');
        b.setAttribute('aria-selected', 'false');
      });
      document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
      btn.classList.add('active');
      btn.setAttribute('aria-selected', 'true');
      document.getElementById(`tab-${tabName}`).classList.add('active');
    });
  });
}

// ============================================================
// Init
// ============================================================

document.addEventListener('DOMContentLoaded', () => {
  loadSettings();
  setupTabs();

  // Settings modal
  document.getElementById('settings-btn').addEventListener('click', showSettingsModal);
  document.getElementById('save-settings-btn').addEventListener('click', saveSettings);
  document.getElementById('cancel-settings-btn').addEventListener('click', hideModal);
  document.getElementById('setup-modal').addEventListener('click', e => {
    if (e.target === document.getElementById('setup-modal')) hideModal();
  });

  // Refresh button
  document.getElementById('refresh-btn').addEventListener('click', () => {
    document.getElementById('pending-list').innerHTML = '<div class="loading">Refreshing…</div>';
    document.getElementById('approved-list').innerHTML = '<div class="loading">Refreshing…</div>';
    document.getElementById('archive-list').innerHTML = '<div class="loading">Refreshing…</div>';
    loadAllQueues();
  });

  // Show setup modal if not configured
  if (shouldShowSetup()) {
    showSettingsModal();
  }

  // Load data
  loadAllQueues();
});
