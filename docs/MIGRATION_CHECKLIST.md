# CartoSky Migration Checklist

This document captures the production cutover plan for migrating from The Weather Models to CartoSky.

## Target Host Layout

- `https://cartosky.com` -> primary frontend
- `https://www.cartosky.com` -> redirect or alias to primary frontend
- `https://api.cartosky.com` -> API, OAuth callback, tile endpoints
- `https://cdn.cartosky.com` -> share-media public base backed by Cloudflare R2

## Current External State

Already completed:

- DNS records created in Cloudflare:
  - `A api 152.53.168.112`
  - `A cartosky.com 152.53.168.112`
  - `CNAME www cartosky.com`
  - R2 custom domain `cdn.cartosky.com -> cartosky-media`
- Cloudflare R2 custom domain configured for `cdn.cartosky.com`
- TWF OAuth callback changed to `https://api.cartosky.com/auth/twf/callback`

Important note:

- If Cloudflare proxying is enabled, `nslookup cartosky.com` will not return the VPS IP. It will return Cloudflare edge IPs. That is expected.

## Cutover Order

1. Confirm DNS is active enough for browser and TLS validation.
2. Add nginx vhosts and TLS coverage for `cartosky.com`, `www.cartosky.com`, and `api.cartosky.com`.
3. Update production env files on the VPS.
4. Deploy code changes for domain defaults and CartoSky branding.
5. Build the frontend and restart API and tile services.
6. Verify frontend, login, tiles, and share-media uploads.
7. Add redirects from old domains after the new hosts are confirmed healthy.

## Production Changes

### 1. Update API Env File

Edit `/etc/twf-v3/api.env` and set these values:

```env
TWF_REDIRECT_URI=https://api.cartosky.com/auth/twf/callback
FRONTEND_RETURN=https://cartosky.com
CORS_ORIGINS=https://cartosky.com,https://www.cartosky.com
R2_PUBLIC_BASE=https://cdn.cartosky.com
```

Keep these existing values unless you are intentionally rotating them:

- `TWF_BASE`
- `TWF_CLIENT_ID`
- `TWF_CLIENT_SECRET`
- `TOKEN_DB_PATH`
- `TOKEN_ENC_KEY`
- `TWM_TELEMETRY_DB_PATH`
- `TWM_ADMIN_MEMBER_IDS`

If R2 credentials are not already present in `/etc/twf-v3/api.env`, add them:

```env
R2_ENDPOINT=<your-cloudflare-r2-s3-endpoint>
R2_BUCKET=cartosky-media
R2_ACCESS_KEY=<access-key>
R2_SECRET_KEY=<secret-key>
R2_PUBLIC_BASE=https://cdn.cartosky.com
```

These are consumed by `backend/app/services/share_media.py`.

### 2. Update Tile Server Env File

Edit `/etc/twf-v3/tile-server.env` and set:

```env
TWF_V3_TILES_PUBLIC_BASE_URL=https://api.cartosky.com
```

Leave these unchanged unless your data layout moved:

- `TWF_V3_DATA_ROOT`
- `TWF_V3_BOUNDARIES_MBTILES`
- `TWF_V3_BOUNDARIES_TILESET_ID`
- `TWF_V3_BOUNDARIES_TILESET_NAME`

### 3. Nginx / Reverse Proxy

Configure these hostnames:

- `cartosky.com`
- `www.cartosky.com`
- `api.cartosky.com`

Recommended route layout:

- `cartosky.com` and `www.cartosky.com`
  - serve frontend build from `/opt/twf_v3/frontend/dist`
  - SPA fallback to `/index.html`
- `api.cartosky.com/auth/twf/*`
  - proxy to `http://127.0.0.1:8200`
- `api.cartosky.com/api/v4/*`
  - proxy to `http://127.0.0.1:8200`
- `api.cartosky.com/tiles/v3/*`
  - proxy to `http://127.0.0.1:8201`

### 4. TLS / Cloudflare

- Ensure the origin certificate covers:
  - `cartosky.com`
  - `www.cartosky.com`
  - `api.cartosky.com`
- In Cloudflare, use `Full (strict)` SSL mode.
- Turn on `Always Use HTTPS` only after origin certificates and nginx vhosts are verified.

### 5. Service Restarts

After code and env changes are deployed:

```bash
sudo systemctl daemon-reload
sudo systemctl restart twm-api.service
sudo systemctl restart twm-tile-server.service
```

Restart scheduler services only if you changed shared environment they depend on.

## Code Changes

### 1. Switch Default Runtime Domains

Update hardcoded production fallbacks from the old API host to `https://api.cartosky.com` in:

- `frontend/src/lib/config.ts`
- `frontend/src/pages/login.tsx`
- `frontend/src/components/SiteHeader.tsx`
- `frontend/index.html`
- `backend/app/services/tile_server.py`

### 2. Update Visible Branding to CartoSky

Change public branding copy in:

- `frontend/index.html`
- `frontend/src/pages/home.tsx`
- `frontend/src/pages/login.tsx`
- `frontend/src/components/SiteHeader.tsx`
- `frontend/src/components/SiteFooter.tsx`

Also update documentation and metadata later:

- `README.md`
- `docs/NGINX_V3.md`
- other public-facing docs that mention The Weather Models

### 3. Decide on Share Filename Branding

New share-image filenames currently use a `twm` prefix in `backend/app/services/share_media.py`.

Decide whether to:

- leave `twm_*` filenames as-is for now, or
- rename new uploads to a `cartosky_*` prefix

This only affects newly generated object names. Old share links should remain valid.

### 4. Update Tests

Adjust hardcoded old-domain fixtures in:

- `backend/tests/test_share_media_api.py`
- `backend/tests/test_twf_oauth_linkify.py`
- `backend/tests/test_twf_error_guards.py`

Tests should reflect the new canonical domains once the code defaults are updated.

## Frontend Build / Deploy

Build the production frontend after code changes:

```bash
cd /opt/twf_v3/frontend
npm install
npm run build
```

The output should land in:

- `/opt/twf_v3/frontend/dist`

## External / Non-Code Tasks

### 1. TWF OAuth

Already done:

- `https://api.cartosky.com/auth/twf/callback` is configured in TWF AdminCP

After deploy, verify the full browser round-trip works from CartoSky.

### 2. Old Domain Redirect Strategy

After CartoSky is confirmed healthy, add redirects:

- `theweathermodels.com` -> `https://cartosky.com$request_uri`
- `www.theweathermodels.com` -> `https://cartosky.com$request_uri`

For API:

- either keep `api.theweathermodels.com` proxying temporarily, or
- redirect to `https://api.cartosky.com$request_uri` after all active clients are cut over

For CDN:

- keep `cdn.theweathermodels.com` serving existing assets if possible
- do not break historic forum and share links that already point at the old CDN host

### 3. Session Expectations

Users will need to log in again on the new host.

Reason:

- session cookies are host-only and are not shared across domains

This is expected behavior.

## Validation Checklist

Run these checks immediately after deployment.

### Frontend

- [ ] `https://cartosky.com` loads successfully
- [ ] `https://www.cartosky.com` resolves as intended
- [ ] Page title and visible branding say `CartoSky`
- [ ] Viewer routes load correctly through SPA fallback

### API

- [ ] `https://api.cartosky.com/health` responds successfully
- [ ] `https://api.cartosky.com/api/v4/...` endpoints respond successfully
- [ ] CORS works from `https://cartosky.com`
- [ ] CORS works from `https://www.cartosky.com`

### OAuth

- [ ] Login starts from the CartoSky frontend
- [ ] TWF redirects back to `https://api.cartosky.com/auth/twf/callback`
- [ ] Browser returns to `https://cartosky.com`
- [ ] Logged-in status is visible in the frontend

### Tiles

- [ ] `https://api.cartosky.com/tiles/v3/boundaries/v1/tilejson.json` returns successfully
- [ ] TileJSON contains `api.cartosky.com` tile URLs
- [ ] Viewer tiles render normally

### Share Media

- [ ] Upload a share image successfully
- [ ] Returned share URL uses `https://cdn.cartosky.com`
- [ ] Shared image opens publicly

### Old Domains

- [ ] Old frontend domain redirects correctly
- [ ] Old API hostname behaves as planned
- [ ] Old CDN links still work

## Rollback Plan

If something fails during cutover:

1. Revert nginx host routing to the old domains.
2. Revert `/etc/twf-v3/api.env` values for `FRONTEND_RETURN`, `CORS_ORIGINS`, and `R2_PUBLIC_BASE` if needed.
3. Revert `/etc/twf-v3/tile-server.env` value for `TWF_V3_TILES_PUBLIC_BASE_URL`.
4. Rebuild and redeploy the previous frontend bundle if the branding/domain code changes caused issues.
5. Restart `twm-api.service` and `twm-tile-server.service`.

## Deferred Cleanup

Do not treat these as part of day-one cutover unless you explicitly want extra risk:

- renaming internal `twf` env vars
- renaming internal `twm` service names
- renaming `/opt/twf_v3` paths
- renaming `/etc/twf-v3` config directories
- renaming cookie names
- renaming repository/package identifiers

Those can be handled later as a separate internal cleanup project.