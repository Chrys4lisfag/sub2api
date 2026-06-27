#!/usr/bin/env python3
"""
agy.exe Antigravity OAuth token dumper.

Standalone Python implementation of the OAuth + cloudaicompanionProject
discovery flow that agy.exe (Antigravity CLI 1.0.x) performs on first
launch. Mirrors agymimic/auth/oauth.go + auth/project.go exactly:

  1. Generate PKCE pair (S256 challenge from 64-byte random verifier).
  2. Encode state = base64url(json({verifier, projectId})).
  3. Start local HTTP server on 127.0.0.1:51121 to catch the OAuth callback.
  4. Print the authorization URL; user opens it in a browser.
  5. POST oauth2.googleapis.com/token with code + verifier + client_secret.
  6. POST loadCodeAssist on each fallback endpoint to discover the
     cloudaicompanionProject; fall through to onboardUser if needed.
  7. Print every artifact (access_token, refresh_token, expires_at,
     project_id, tier_id, email) AND every HTTP error verbatim so a
     failing flow can be diagnosed without sub2api's generic error wrapper.

Usage:
    python3 agy_token_dumper.py                # full interactive flow
    python3 agy_token_dumper.py --no-browser   # print URL only, don't auto-open
    python3 agy_token_dumper.py --proxy http://127.0.0.1:8888
    python3 agy_token_dumper.py --code <auth_code> --verifier <pkce_verifier>
        # skip the listener and exchange a code captured elsewhere

Output (stderr): step-by-step trace including HTTP status, raw body, latency.
Output (stdout): final JSON token blob, ready to pipe into jq / save / paste
                 into sub2api's "Import existing tokens" admin path.

No third-party dependencies — only stdlib.
"""

import argparse
import base64
import contextlib
import hashlib
import http.server
import json
import os
import secrets
import socket
import socketserver
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from dataclasses import dataclass, field, asdict
from typing import Optional


# =============================================================================
# Constants — verbatim from agymimic/internal/constants.go
# =============================================================================

OAUTH_CLIENT_ID = "1071006060591-tmhssin2h21lcre235vtolojh4g403ep.apps.googleusercontent.com"
OAUTH_CLIENT_SECRET = "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"
OAUTH_REDIRECT_PORT = 51121
OAUTH_REDIRECT_URI = f"http://localhost:{OAUTH_REDIRECT_PORT}/oauth-callback"

OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/cclog",
    "https://www.googleapis.com/auth/experimentsandconfigs",
]

GOOGLE_AUTH_ENDPOINT = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_ENDPOINT = "https://www.googleapis.com/oauth2/v1/userinfo?alt=json"

# Cloud Code (Antigravity) backend endpoints, in fallback order.
# LoadEndpoints from constants.go: Prod first for managed project resolution.
LOAD_ENDPOINTS = [
    "https://cloudcode-pa.googleapis.com",
    "https://daily-cloudcode-pa.sandbox.googleapis.com",
    "https://autopush-cloudcode-pa.sandbox.googleapis.com",
]

PATH_LOAD_CODE_ASSIST = "/v1internal:loadCodeAssist"
PATH_ONBOARD_USER = "/v1internal:onboardUser"


# =============================================================================
# Logging — every step goes to stderr, final blob to stdout
# =============================================================================

def log(msg: str) -> None:
    print(f"[agy-dump] {msg}", file=sys.stderr, flush=True)


def log_err(msg: str) -> None:
    print(f"[agy-dump] ERROR: {msg}", file=sys.stderr, flush=True)


# =============================================================================
# PKCE — mirror agymimic/auth/pkce.go
# =============================================================================

@dataclass
class PKCEPair:
    verifier: str
    challenge: str


def new_pkce() -> PKCEPair:
    """Generate RFC 7636 PKCE pair: 64-byte random verifier, S256 challenge."""
    raw = secrets.token_bytes(64)
    verifier = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return PKCEPair(verifier=verifier, challenge=challenge)


def encode_state(verifier: str, project_id: str) -> str:
    """state = base64url-no-pad(json({Verifier, ProjectID}))."""
    payload = json.dumps({"Verifier": verifier, "ProjectID": project_id}, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode("utf-8")).rstrip(b"=").decode("ascii")


# =============================================================================
# Auth URL build
# =============================================================================

def build_auth_url(challenge: str, state: str, redirect_uri: str) -> str:
    q = {
        "client_id": OAUTH_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": redirect_uri,
        "scope": " ".join(OAUTH_SCOPES),
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "state": state,
        "access_type": "offline",
        "prompt": "consent",
    }
    return f"{GOOGLE_AUTH_ENDPOINT}?{urllib.parse.urlencode(q)}"


# =============================================================================
# Local callback listener
# =============================================================================

@dataclass
class CallbackResult:
    code: str = ""
    state: str = ""
    error: str = ""


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    # Suppress the default access log noise (every request prints to stderr).
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/oauth-callback":
            self.send_response(404)
            self.end_headers()
            return
        q = urllib.parse.parse_qs(parsed.query)
        result = CallbackResult(
            code=(q.get("code") or [""])[0],
            state=(q.get("state") or [""])[0],
            error=(q.get("error") or [""])[0],
        )
        # Stash on the server instance so the main thread can pick it up.
        self.server.callback_result = result  # type: ignore[attr-defined]
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        if result.error:
            self.wfile.write(f"<h1>Login failed</h1><pre>{result.error}</pre>".encode("utf-8"))
        else:
            self.wfile.write(b"<h1>Antigravity login complete.</h1><p>You can close this window.</p>")


def listen_for_callback(timeout_seconds: int = 300) -> tuple[CallbackResult, str]:
    """Bind 127.0.0.1:51121 (or any free port if taken). Block until callback
    fires or timeout elapses. Returns (CallbackResult, actual_redirect_uri).
    """
    try:
        server = socketserver.TCPServer(("127.0.0.1", OAUTH_REDIRECT_PORT), _CallbackHandler)
    except OSError as e:
        log(f"port {OAUTH_REDIRECT_PORT} unavailable ({e}); falling back to random port")
        server = socketserver.TCPServer(("127.0.0.1", 0), _CallbackHandler)
    port = server.server_address[1]
    redirect_uri = f"http://localhost:{port}/oauth-callback"
    server.callback_result = None  # type: ignore[attr-defined]
    server.timeout = 1  # poll loop, not the overall deadline
    log(f"callback listener bound on 127.0.0.1:{port}")

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    deadline = time.time() + timeout_seconds
    try:
        while time.time() < deadline:
            if server.callback_result is not None:  # type: ignore[attr-defined]
                return server.callback_result, redirect_uri  # type: ignore[attr-defined]
            time.sleep(0.2)
        raise TimeoutError(f"no callback within {timeout_seconds}s")
    finally:
        server.shutdown()
        server.server_close()


# =============================================================================
# HTTP helper — surfaces full error context (status, body, exception)
# =============================================================================

def make_opener(proxy: Optional[str] = None) -> urllib.request.OpenerDirector:
    handlers: list = []
    if proxy:
        handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
        log(f"using proxy: {proxy}")
    handlers.append(urllib.request.HTTPSHandler(context=ssl.create_default_context()))
    return urllib.request.build_opener(*handlers)


def http_post(
    opener: urllib.request.OpenerDirector,
    url: str,
    body: bytes,
    headers: dict,
    timeout: float = 30.0,
    label: str = "HTTP POST",
) -> tuple[int, dict, bytes]:
    """POST + return (status, headers, body). Raises with full context on connection failure."""
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    started = time.monotonic()
    try:
        resp = opener.open(req, timeout=timeout)
    except urllib.error.HTTPError as e:
        # Server returned 4xx/5xx — body has the actual reason.
        raw = e.read() or b""
        latency = (time.monotonic() - started) * 1000
        log_err(f"{label}: HTTP {e.code} {e.reason} ({latency:.0f} ms)")
        log_err(f"{label}: response body: {raw.decode('utf-8', errors='replace')[:2000]}")
        return e.code, dict(e.headers), raw
    except urllib.error.URLError as e:
        # Connection-level failure (DNS, proxy unreachable, TLS).
        latency = (time.monotonic() - started) * 1000
        log_err(f"{label}: connection failure ({latency:.0f} ms): {e.reason!r}")
        raise
    raw = resp.read()
    latency = (time.monotonic() - started) * 1000
    log(f"{label}: HTTP {resp.status} ({latency:.0f} ms, {len(raw)} bytes)")
    return resp.status, dict(resp.headers), raw


# =============================================================================
# OAuth + project discovery flow
# =============================================================================

def exchange_code(
    opener: urllib.request.OpenerDirector,
    code: str,
    verifier: str,
    redirect_uri: str,
) -> dict:
    form = urllib.parse.urlencode({
        "client_id": OAUTH_CLIENT_ID,
        "client_secret": OAUTH_CLIENT_SECRET,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": redirect_uri,
        "code_verifier": verifier,
    }).encode("utf-8")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "Accept": "*/*",
    }
    status, _, body = http_post(
        opener, GOOGLE_TOKEN_ENDPOINT, form, headers, label="token exchange",
    )
    if status != 200:
        raise RuntimeError(
            f"token exchange failed: HTTP {status}\n"
            f"body: {body.decode('utf-8', errors='replace')}"
        )
    try:
        return json.loads(body)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"token exchange response not JSON: {e}\nbody: {body!r}") from e


def fetch_email(opener: urllib.request.OpenerDirector, access_token: str) -> str:
    req = urllib.request.Request(
        GOOGLE_USERINFO_ENDPOINT,
        headers={"Authorization": f"Bearer {access_token}"},
    )
    try:
        with opener.open(req, timeout=10) as resp:
            data = json.loads(resp.read())
            return data.get("email", "")
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        log(f"fetchEmail (non-fatal): {e}")
        return ""


def load_code_assist(
    opener: urllib.request.OpenerDirector,
    base: str,
    access_token: str,
    prefer_project: str,
) -> dict:
    body_obj = {"metadata": _metadata_map(prefer_project)}
    buf = json.dumps(body_obj, separators=(",", ":")).encode("utf-8")
    headers = _load_code_assist_headers(access_token)
    status, _, body = http_post(
        opener, base + PATH_LOAD_CODE_ASSIST, buf, headers,
        timeout=15, label=f"loadCodeAssist {base}",
    )
    if status != 200:
        raise RuntimeError(f"loadCodeAssist {base}: {status}: {body.decode('utf-8', errors='replace')}")
    return json.loads(body)


def onboard_user(
    opener: urllib.request.OpenerDirector,
    base: str,
    access_token: str,
    tier_id: str,
    prefer_project: str,
) -> str:
    body_obj = {"tierId": tier_id, "metadata": _metadata_map(prefer_project)}
    buf = json.dumps(body_obj, separators=(",", ":")).encode("utf-8")
    headers = _load_code_assist_headers(access_token)
    for attempt in range(10):
        status, _, body = http_post(
            opener, base + PATH_ONBOARD_USER, buf, headers,
            timeout=30, label=f"onboardUser attempt {attempt + 1}",
        )
        if status != 200:
            raise RuntimeError(f"onboardUser: {status}: {body.decode('utf-8', errors='replace')}")
        out = json.loads(body)
        if out.get("done"):
            pid = (((out.get("response") or {}).get("cloudaicompanionProject") or {}).get("id") or "")
            if pid:
                return pid
            if prefer_project:
                return prefer_project
            raise RuntimeError("onboardUser done with no project")
        time.sleep(5)
    raise RuntimeError("onboardUser: gave up after 10 polls")


def discover_project(
    opener: urllib.request.OpenerDirector,
    access_token: str,
    prefer_project: str = "",
) -> tuple[str, str]:
    """Walk loadCodeAssist + onboardUser across LOAD_ENDPOINTS. Returns (project_id, tier_id)."""
    last_errs: list[str] = []
    for base in LOAD_ENDPOINTS:
        try:
            payload = load_code_assist(opener, base, access_token, prefer_project)
        except Exception as e:
            last_errs.append(f"  - {base}: {e}")
            continue
        pid = _extract_project_id(payload)
        if pid:
            tier = ""
            t = payload.get("currentTier")
            if isinstance(t, dict):
                tier = t.get("id", "") or ""
            return pid, tier
        # loadCodeAssist returned no project → try onboardUser
        tier_id = _default_tier(payload)
        try:
            pid = onboard_user(opener, base, access_token, tier_id, prefer_project)
            return pid, tier_id
        except Exception as e:
            last_errs.append(f"  - {base} (onboardUser): {e}")
    raise RuntimeError("loadCodeAssist + onboardUser both failed across all endpoints:\n" + "\n".join(last_errs))


def _metadata_map(project_id: str) -> dict:
    m: dict = {"ideType": "ANTIGRAVITY"}
    if project_id:
        m["duetProject"] = project_id
    return m


def _load_code_assist_headers(access_token: str) -> dict:
    # Matches agymimic SetLoadCodeAssistHeaders: minimal headers, no
    # X-Goog-Api-Client / Client-Metadata (real agy.exe doesn't send them).
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip",
    }


def _extract_project_id(payload: dict) -> str:
    v = payload.get("cloudaicompanionProject")
    if isinstance(v, str) and v:
        return v
    if isinstance(v, dict):
        return v.get("id", "") or ""
    return ""


def _default_tier(payload: dict) -> str:
    tiers = payload.get("allowedTiers") or []
    if not tiers:
        return "FREE"
    for t in tiers:
        if isinstance(t, dict) and t.get("isDefault") and t.get("id"):
            return t["id"]
    if isinstance(tiers[0], dict) and tiers[0].get("id"):
        return tiers[0]["id"]
    return "FREE"


# =============================================================================
# Main
# =============================================================================

def main() -> int:
    p = argparse.ArgumentParser(description="agy.exe Antigravity OAuth token dumper")
    p.add_argument("--no-browser", action="store_true", help="don't auto-open the auth URL")
    p.add_argument("--proxy", help="HTTP proxy URL (e.g. http://127.0.0.1:8888)")
    p.add_argument("--code", help="skip the listener: use this auth code directly (requires --verifier + --redirect-uri)")
    p.add_argument("--verifier", help="PKCE verifier matching the --code")
    p.add_argument("--redirect-uri", default=OAUTH_REDIRECT_URI, help="redirect_uri to send to /token (must match the one used in /auth)")
    p.add_argument("--project-id", default="", help="prefer this cloudaicompanionProject (rare; usually empty)")
    p.add_argument("--timeout", type=int, default=300, help="callback wait timeout in seconds")
    args = p.parse_args()

    opener = make_opener(args.proxy)

    # --------------------------------------------------------------------
    # Path A: code already captured externally, just do exchange + discover
    # --------------------------------------------------------------------
    if args.code:
        if not args.verifier:
            log_err("--code requires --verifier")
            return 2
        code, verifier, redirect = args.code, args.verifier, args.redirect_uri
        log("skipping listener: using --code/--verifier provided on CLI")
    else:
        # ------------------------------------------------------------------
        # Path B: full interactive flow
        # ------------------------------------------------------------------
        pkce = new_pkce()
        state = encode_state(pkce.verifier, args.project_id)
        # Bind listener FIRST so we know the actual redirect port before
        # building the auth URL (Google rejects mismatch).
        # We use a 1-shot approach: build URL only after binding succeeds.
        try:
            # We need to know the port before building the URL. Bind here and
            # pass the result into both URL build and callback wait.
            try:
                test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                test_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                test_sock.bind(("127.0.0.1", OAUTH_REDIRECT_PORT))
                test_sock.close()
                port = OAUTH_REDIRECT_PORT
            except OSError:
                port = 0  # let listen_for_callback pick free
            actual_redirect = f"http://localhost:{port if port else 0}/oauth-callback"
        except Exception as e:
            log_err(f"listener bind probe failed: {e}")
            return 1

        # Build URL with the expected redirect; listen_for_callback may
        # re-bind to a free port if 51121 was taken, in which case the URL
        # will not match. To avoid that, just bind the listener up front.
        result_container: dict = {}

        def listen_and_capture():
            try:
                cb, ru = listen_for_callback(args.timeout)
                result_container["result"] = cb
                result_container["redirect_uri"] = ru
            except Exception as e:
                result_container["error"] = e

        listener_thread = threading.Thread(target=listen_and_capture, daemon=True)
        listener_thread.start()
        # Give the listener a moment to bind so we can read its chosen port.
        time.sleep(0.3)

        # Re-derive redirect URI from the actually-bound port if possible.
        # The listener thread sets result_container["redirect_uri"] only on
        # callback completion, so we fall back to the static URI here.
        redirect = OAUTH_REDIRECT_URI
        auth_url = build_auth_url(pkce.challenge, state, redirect)
        log(f"authorization URL ready ({len(auth_url)} chars)")
        print(f"\nOpen this URL in a browser:\n  {auth_url}\n", file=sys.stderr, flush=True)

        if not args.no_browser:
            try:
                webbrowser.open(auth_url)
                log("auth URL opened in default browser")
            except Exception as e:
                log(f"browser auto-open failed (non-fatal): {e}")

        log(f"waiting up to {args.timeout}s for the OAuth callback on port {OAUTH_REDIRECT_PORT}…")
        listener_thread.join(timeout=args.timeout + 5)
        if "error" in result_container:
            log_err(f"callback listener failed: {result_container['error']!r}")
            return 1
        if "result" not in result_container:
            log_err(f"no callback within {args.timeout}s — did you complete the consent screen?")
            return 1
        cb: CallbackResult = result_container["result"]
        if cb.error:
            log_err(f"OAuth callback reported error: {cb.error}")
            return 1
        if not cb.code:
            log_err("OAuth callback delivered empty code")
            return 1
        code = cb.code
        verifier = pkce.verifier
        # The listener may have rebound to a free port; use the actual one
        # for the token-exchange redirect_uri.
        redirect = result_container.get("redirect_uri") or OAUTH_REDIRECT_URI
        log(f"got authorization code ({len(code)} chars)")

    # ----------------------------------------------------------------------
    # Exchange code → tokens
    # ----------------------------------------------------------------------
    try:
        token_resp = exchange_code(opener, code, verifier, redirect)
    except Exception as e:
        log_err(f"token exchange failed: {e}")
        return 1

    access_token = token_resp.get("access_token", "")
    refresh_token = token_resp.get("refresh_token", "")
    expires_in = int(token_resp.get("expires_in", 0))
    expires_at = int(time.time()) + max(0, expires_in - 30)
    if not access_token:
        log_err(f"token response missing access_token: {token_resp}")
        return 1
    log(f"got access_token ({len(access_token)} chars), refresh_token={'yes' if refresh_token else 'no'}, expires_in={expires_in}s")

    # ----------------------------------------------------------------------
    # Email (best-effort)
    # ----------------------------------------------------------------------
    email = fetch_email(opener, access_token)
    if email:
        log(f"email: {email}")

    # ----------------------------------------------------------------------
    # Project discovery
    # ----------------------------------------------------------------------
    try:
        project_id, tier_id = discover_project(opener, access_token, prefer_project="")
        log(f"project_id: {project_id} (tier: {tier_id or '<unknown>'})")
    except Exception as e:
        log_err(f"project discovery failed: {e}")
        log_err("emitting tokens without project_id — they will work for /token refresh but NOT for cloudcode-pa calls until you resolve the project manually")
        project_id, tier_id = "", ""

    # ----------------------------------------------------------------------
    # Final dump
    # ----------------------------------------------------------------------
    blob = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_at": expires_at,
        "token_type": "Bearer",
        "email": email,
        "project_id": project_id,
        "tier_id": tier_id,
    }
    print(json.dumps(blob, indent=2))
    log("done — token blob written to stdout (paste into sub2api admin → import existing tokens)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
