/*
 * Cloudflare Worker: auth proxy for @thechoirsource dashboard.
 * Receives approve/reject requests from the dashboard, validates the shared
 * secret, and triggers the appropriate GitHub Actions workflow.
 *
 * Environment variables (set in Cloudflare dashboard → Workers → Settings → Variables):
 * - DASHBOARD_SECRET: shared secret with the dashboard
 * - GH_PAT_TOKEN: GitHub PAT with actions:write permission
 * - GH_OWNER: GitHub repo owner (username or org)
 * - GH_REPO: GitHub repo name (e.g. "thechoirsource")
 */

export default {
  async fetch(request, env) {
    // CORS: allow any origin (dashboard could be on any GitHub Pages URL).
    // For tighter security, restrict to the actual Pages URL once known.
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    if (request.method !== 'POST') {
      return new Response('Method not allowed', { status: 405, headers: corsHeaders });
    }

    let body;
    try {
      body = await request.json();
    } catch (err) {
      return new Response(JSON.stringify({ error: 'Invalid JSON body' }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // Validate secret
    if (!env.DASHBOARD_SECRET || body.secret !== env.DASHBOARD_SECRET) {
      return new Response(JSON.stringify({ error: 'Invalid secret' }),
        { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    const { youtube_id, selected_clip_rank, edited_caption, edited_hashtags, action } = body;

    if (!youtube_id || !action) {
      return new Response(JSON.stringify({ error: 'Missing required fields: youtube_id and action' }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    if (!['approve', 'reject'].includes(action)) {
      return new Response(JSON.stringify({ error: 'action must be "approve" or "reject"' }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    if (!env.GH_PAT_TOKEN || !env.GH_OWNER || !env.GH_REPO) {
      return new Response(JSON.stringify({ error: 'Worker not configured (missing GitHub env vars)' }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // Trigger GitHub Action via workflow_dispatch
    const workflowResponse = await fetch(
      `https://api.github.com/repos/${env.GH_OWNER}/${env.GH_REPO}/actions/workflows/on-approval.yml/dispatches`,
      {
        method: 'POST',
        headers: {
          'Authorization': `token ${env.GH_PAT_TOKEN}`,
          'Accept': 'application/vnd.github.v3+json',
          'Content-Type': 'application/json',
          'User-Agent': 'thechoirsource-worker/1.0',
        },
        body: JSON.stringify({
          ref: 'main',
          inputs: {
            youtube_id: String(youtube_id),
            selected_clip_rank: String(selected_clip_rank || '1'),
            edited_caption: String(edited_caption || ''),
            edited_hashtags: String(edited_hashtags || ''),
            action: String(action),
          },
        }),
      }
    );

    if (!workflowResponse.ok) {
      const errorText = await workflowResponse.text();
      console.error(`GitHub API error ${workflowResponse.status}: ${errorText}`);
      return new Response(
        JSON.stringify({
          error: 'GitHub API error',
          status: workflowResponse.status,
          details: errorText.slice(0, 500),
        }),
        { status: 502, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // GitHub returns 204 No Content on success
    return new Response(
      JSON.stringify({ status: 'triggered', action, youtube_id }),
      { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  },
};
