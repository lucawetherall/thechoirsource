# @thechoirsource — Social Media Automation Pipeline

Automated pipeline for discovering, clipping, and posting short choir performance videos to Instagram, Facebook, and TikTok.

## Architecture

```
Flow:
1. Weekly (Monday 6am UTC):
   GitHub Action → pipeline/run.py → discover YouTube videos
   → download → audio analysis (find exciting 15-40s segments)
   → crop to 9:16 portrait → overlay branded captions
   → generate post copy (Claude API) → upload to Cloudflare R2
   → commit updated queue/pending.json

2. Human reviews dashboard (GitHub Pages):
   Previews 2-3 clip options per video → edits caption/hashtags
   → clicks Approve or Reject → POST to Cloudflare Worker

3. Cloudflare Worker validates secret → triggers on-approval.yml
   GitHub Action → python -m pipeline.run --approve ...
   → moves item to approved queue with scheduled time
   → deletes non-selected clips from R2

4. Daily (5pm UTC):
   GitHub Action → python -m pipeline.run --publish
   → posts due clips to Instagram Reels, Facebook Reels, TikTok
   → moves items to archive

Infrastructure:
- Compute:       GitHub Actions (free tier for public repos)
- Video storage: Cloudflare R2 (S3-compatible, cheap egress)
- Dashboard:     GitHub Pages (static, no server needed)
- Auth proxy:    Cloudflare Worker (validates dashboard → Actions requests)
- Orchestration: Python 3.11
```

---

## Quick Start — Mock Mode

Test the full pipeline locally without any API keys:

```bash
# Install dependencies (requires Python 3.11+, FFmpeg)
pip install -r requirements.txt

# Run the full pipeline in mock mode
python -m pipeline.run --mock

# Check results
cat queue/pending.json | python -m json.tool | head -80
```

This generates synthetic test videos, runs audio analysis, crops to portrait, overlays captions, and populates `queue/pending.json` — all without touching any external services.

---

## Running Tests

```bash
# Install test dependencies (included in requirements.txt)
pip install -r requirements.txt

# Run all tests (requires FFmpeg and libsndfile)
pytest tests/ -v

# Run a specific test module
pytest tests/test_audio_analysis.py -v
pytest tests/test_queue_manager.py -v
pytest tests/test_orchestrator.py -v  # end-to-end mock test (~2-3 min)
```

**System requirements for tests:**
- FFmpeg (with libx264 and AAC support)
- libsndfile (`sudo apt-get install libsndfile1` on Ubuntu/Debian)
- Python 3.11+

---

## Manual Setup Checklist

Work through these steps in order before running the pipeline for real.

### A. YouTube Data API

1. Go to [Google Cloud Console](https://console.cloud.google.com) → Create a new project
2. Enable **YouTube Data API v3** (APIs & Services → Enable APIs)
3. Create an **API Key** (Credentials → Create Credentials → API Key)
4. Copy the key to `.env` as `YOUTUBE_API_KEY`
5. **Verify channel IDs** in `config/channels.yml` — entries marked `PLACEHOLDER_CHANNEL_ID_*` need real values
   - Find a channel ID: go to the YouTube channel → View Page Source → search for `"channelId"`
   - Or use the API: `GET https://www.googleapis.com/youtube/v3/channels?forHandle=@HandleName&part=id&key=YOUR_KEY`

### B. Anthropic API

1. Get an API key from [console.anthropic.com](https://console.anthropic.com)
2. Copy to `.env` as `ANTHROPIC_API_KEY`
3. The pipeline uses `claude-sonnet-4-20250514` for metadata parsing and copy generation

### C. Cloudflare R2

1. Log in to [Cloudflare dashboard](https://dash.cloudflare.com) → **R2** → **Create bucket**
2. Name the bucket `thechoirsource-clips`
3. Enable public access:
   - Go to bucket → Settings → **Public access** → Enable R2.dev subdomain
   - Or add a custom domain (recommended for production)
4. Create an R2 API token:
   - R2 → Manage R2 API Tokens → Create API Token
   - Permissions: **Object Read & Write** for this bucket only
5. Copy the credentials to `.env`:
   ```
   R2_ACCESS_KEY_ID=...
   R2_SECRET_ACCESS_KEY=...
   R2_ACCOUNT_ID=...  (find in right sidebar of R2 dashboard)
   R2_BUCKET_NAME=thechoirsource-clips
   R2_PUBLIC_URL=https://pub-xxxx.r2.dev  (from bucket public access settings)
   ```

### D. Meta (Instagram + Facebook)

1. Create an app at [developers.facebook.com](https://developers.facebook.com)
2. Add products: **Instagram Graph API**, **Pages API**
3. Connect your Facebook Page and Instagram Professional account
4. Required permissions:
   - `pages_manage_posts`, `pages_read_engagement`
   - `instagram_basic`, `instagram_content_publish`
5. Generate a **long-lived Page Access Token**:
   - A standard token expires in 60 days — set a calendar reminder
   - For a non-expiring token: create a **System User** in Business Manager
6. Copy to `.env`:
   ```
   META_ACCESS_TOKEN=...
   META_IG_USER_ID=...   (Instagram User ID, not the username)
   META_PAGE_ID=...      (Facebook Page ID)
   ```

> **Note**: Instagram requires that the video URL be publicly accessible on R2 before it can be processed. Ensure R2 public access is enabled.

### E. TikTok

1. Register at [developers.tiktok.com](https://developers.tiktok.com)
2. Create an app and apply for **Content Posting API** access (requires review — allow 1–2 weeks)
3. Once approved, generate an access token with `video.publish` scope
4. Copy to `.env` as `TIKTOK_ACCESS_TOKEN`

### F. Cloudflare Worker

1. Install Wrangler: `npm install -g wrangler`
2. Deploy the worker:
   ```bash
   cd worker/
   wrangler login
   wrangler deploy
   ```
3. Note the Worker URL (e.g., `https://thechoirsource-worker.your-subdomain.workers.dev`)
4. In Cloudflare dashboard → **Workers & Pages** → `thechoirsource-worker` → **Settings** → **Variables**:
   - Add `DASHBOARD_SECRET` (generate a random 32-char string)
   - Add `GH_PAT_TOKEN` (see step G)
   - Add `GH_OWNER` (your GitHub username)
   - Add `GH_REPO` (e.g., `thechoirsource`)
   - Mark all as **Encrypted**

### G. GitHub Repository

1. Create a fine-grained **Personal Access Token** (PAT):
   - Settings → Developer settings → Personal access tokens → Fine-grained tokens
   - Repository access: only `thechoirsource`
   - Permissions: **Actions** (Read & Write), **Contents** (Read & Write)
2. In repo **Settings** → **Secrets and variables** → **Actions**, add ALL env vars from `.env.example` as repository secrets
3. Enable **GitHub Pages**:
   - Settings → Pages → Source: **GitHub Actions**
4. Add **Montserrat SemiBold** font:
   - Download from [Google Fonts](https://fonts.google.com/specimen/Montserrat) (SIL Open Font License)
   - Rename to `Montserrat-SemiBold.ttf` and place in `assets/fonts/`
   - Commit and push

### H. First Run

1. Trigger the **Weekly Pipeline** manually:
   - Actions tab → **Weekly Pipeline** → **Run workflow**
2. Wait 5–10 minutes for it to complete
3. Check `queue/pending.json` in the repo — should contain video items
4. Open your GitHub Pages URL (Settings → Pages for the URL)
5. In the dashboard setup modal, enter your Worker URL and dashboard secret
6. Preview the clip options → edit caption if needed → click **Approve**
7. Wait for the **On Approval** action to trigger (should happen within 30 seconds)
8. Manually trigger **Daily Publish** to test posting:
   - Actions → **Daily Publish** → **Run workflow**
9. Verify posts appear on Instagram, Facebook, and TikTok

---

## Token Refresh Schedule

| Token | Expiry | Action |
|---|---|---|
| Meta Access Token | 60 days | Refresh via Graph API Explorer, or use System User for non-expiring |
| TikTok Access Token | Check developer docs | Set calendar reminder |
| YouTube API Key | Does not expire | Rotate annually as security hygiene |
| GitHub PAT | Set to 1 year | Set a calendar reminder |

---

## Troubleshooting

**Pipeline fails with "Missing required config"**
→ Ensure all GitHub secrets from `.env.example` are added to repo settings

**YouTube quota exhausted**
→ The pipeline costs ~824 units/run against a 10,000/day quota. If you run manually multiple times in a day, you may hit the limit. Wait until midnight UTC for quota reset.

**yt-dlp fails to download**
→ YouTube frequently changes anti-bot measures. Update yt-dlp: `pip install --upgrade yt-dlp`

**Instagram says "media not found" or "unsupported URL"**
→ The R2 URL must be publicly accessible. Check that R2 public access is enabled and the URL is correct.

**Dashboard shows "No data loaded"**
→ The deploy-dashboard.yml Action may not have run yet. Trigger it manually: Actions → Deploy Dashboard → Run workflow.

**Worker returns 401**
→ The dashboard secret does not match. Check both the localStorage value in your browser (open DevTools → Application → Local Storage) and the Worker's environment variable.

**TikTok posting fails**
→ The Content Posting API requires approved app review. If the token or permissions are wrong, the API response will include a clear error code.

**Clip captions use DejaVu font instead of Montserrat**
→ Add `Montserrat-SemiBold.ttf` to `assets/fonts/` as described in step G above.

**Git conflicts in queue files**
→ Multiple Actions running simultaneously can cause conflicts. The `git pull --rebase` step handles most cases. If a workflow fails mid-rebase, re-trigger it manually.

---

## Project Structure

```
thechoirsource/
├── pipeline/              Python pipeline modules
│   ├── run.py             Orchestrator (main entry point)
│   ├── discover.py        YouTube video discovery
│   ├── download.py        yt-dlp wrapper
│   ├── audio_analysis.py  Dynamic contrast scoring → clip candidates
│   ├── crop_portrait.py   FFmpeg landscape → 9:16 portrait
│   ├── caption_overlay.py FFmpeg drawtext branded overlay
│   ├── generate_copy.py   Claude API → post caption + hashtags
│   ├── metadata_parser.py Claude API → extract piece/composer/ensemble
│   ├── upload_r2.py       Cloudflare R2 upload via boto3
│   ├── queue_manager.py   JSON queue operations
│   ├── publish.py         Post to Instagram, Facebook, TikTok
│   ├── config.py          Configuration loader
│   └── mock.py            Mock implementations for testing
├── tests/                 pytest test suite
├── dashboard/             Static GitHub Pages dashboard
├── worker/                Cloudflare Worker source
├── config/                YAML configuration files
├── queue/                 JSON queue files (committed to repo)
└── assets/fonts/          Brand font (add Montserrat-SemiBold.ttf here)
```
