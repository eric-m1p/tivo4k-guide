#!/usr/bin/env python3
"""Per-site yield canaries: notice when a site breaks, pull it from the feed, alert.

A site "breaks" when its scraper suddenly yields nothing (StreamCorner's
worker rename, isportsurge's iframe change) or when nothing it lists plays.
Each run records, per site, the rows its scraper produced and what the probes
saw (``source_health`` ``site_probe`` counts). History and verdicts live in
``source_health.json`` under ``sites`` (per-site health/yield/last success for
the admin panel later).

Rules (numbers from the feed policy's ``canary`` block):

- **down** — zero rows for ``bad_runs`` runs in a row, on a site that normally
  has rows (median of the last ``baseline_runs`` > 0 and ≥90% of them non-empty).
  The feed's ``config.sites.<id>`` gets ``enabled: false, auto: true`` and an
  alert goes out. Also down: nothing plays (≥3 playback checks, 0 played, for
  ``bad_runs`` runs) *and* the apps agree (≥3 app attempts on the site in 24 h,
  none ok). The runner alone being refused never disables a site.
- **check** — alert only: rows below ``min_yield_ratio`` × the usual count, or
  nothing plays from the runner while the apps have no recent verdict.
- **recovered** — the first good run re-enables the site and closes the alert.

Alerts: ``alert: "github_issue"`` opens one issue per site (label
``site-canary``) in ``$GITHUB_REPOSITORY`` using ``$GITHUB_TOKEN``, and closes
it on recovery. Without a token (local runs) alerts are only printed.
"""

from __future__ import annotations

import json
import os
import statistics
import urllib.request

DEFAULTS = {"min_yield_ratio": 0.2, "baseline_runs": 48, "bad_runs": 3, "alert": "github_issue"}
APP_WINDOW_SEC = 24 * 3600
LABEL = "site-canary"


def site_rows(policy: dict, ppv_rows: int | None, sc_counts: dict | None, site_counts: dict | None) -> dict[str, int]:
    """Rows per site id for the sites this run actually fetched."""
    index = {p.upper(): site for site, meta in policy["sites"].items() for p in meta.get("providers") or []}
    rows: dict[str, int] = {}
    if ppv_rows is not None:
        rows["ppv"] = int(ppv_rows)
    if sc_counts is not None:
        rows["streamcorner"] = sum(int(n or 0) for n in sc_counts.values())
    for name, n in (site_counts or {}).items():
        site = index.get(str(name).upper(), str(name).lower())
        rows[site] = rows.get(site, 0) + int(n or 0)
    return rows


def scrape_by_site(policy: dict, scrape: dict | None) -> dict[str, dict]:
    """site_plugins stats per site id (plugin names map like site_rows)."""
    index = {p.upper(): site for site, meta in policy["sites"].items() for p in meta.get("providers") or []}
    return {index.get(str(name).upper(), str(name).lower()): stats for name, stats in (scrape or {}).items()}


def app_by_site(state: dict, site_of: dict[str, str], now: int) -> dict[str, dict]:
    """Recent app verdicts per site, from per-URL app counters."""
    out: dict[str, dict] = {}
    entries = state.get("sources") or {}
    for url, site in site_of.items():
        entry = entries.get(url)
        if not isinstance(entry, dict):
            continue
        recent_ok = now - int(entry.get("app_last_ok") or 0) <= APP_WINDOW_SEC
        recent_fail = now - int(entry.get("app_last_fail") or 0) <= APP_WINDOW_SEC
        if not (recent_ok or recent_fail):
            continue
        bucket = out.setdefault(site, {"ok": 0, "fail": 0})
        bucket["ok"] += int(entry.get("app_ok") or 0) if recent_ok else 0
        bucket["fail"] += int(entry.get("app_fail") or 0) if recent_fail else 0
    return out


def update(state: dict, rows: dict[str, int], site_probe: dict[str, dict],
           apps: dict[str, dict], now: int, policy: dict | None = None,
           scrape: dict[str, dict] | None = None) -> list[dict]:
    """Fold one run into state["sites"]; return transitions to alert on.

    ``scrape`` (site_plugins stats) only sharpens the reason: when the pages
    loaded but nothing parsed, a zero-row outage is reported as a likely
    layout change rather than the site being unreachable.
    """
    p = {**DEFAULTS, **(policy or {})}
    keep = max(3, int(p["baseline_runs"]))
    need = max(1, int(p["bad_runs"]))
    ratio = float(p["min_yield_ratio"])
    sites = state.setdefault("sites", {})
    alerts: list[dict] = []
    for site in sorted(set(rows) | set(site_probe)):
        s = sites.setdefault(site, {"rows": [], "bad_runs": 0, "play_bad_runs": 0, "status": "ok"})
        history = [int(n) for n in s.get("rows") or []]
        n = rows.get(site)
        probe = site_probe.get(site) or {}
        app = apps.get(site) or {}
        scraped = (scrape or {}).get(site) or {}
        if scraped:
            s["scrape"] = {k: scraped.get(k) for k in ("verdict", "pages", "error", "seconds") if k in scraped}
        layout = scraped.get("verdict") == "layout"

        baseline = statistics.median(history) if len(history) >= 3 else 0
        normally_full = bool(history) and sum(1 for h in history if h > 0) >= 0.9 * len(history)
        empty = n is not None and n == 0 and baseline > 0 and normally_full
        low = n is not None and n > 0 and baseline >= 5 and n < ratio * baseline
        page_dead = (
            int(probe.get("probed") or 0) >= 5 and not int(probe.get("ok") or 0)
            and int(probe.get("server_blocked") or 0) < int(probe.get("probed") or 0)
        )
        play_dead = (int(probe.get("play_tried") or 0) >= 3 and not int(probe.get("played") or 0)) or page_dead
        apps_ok = int(app.get("ok") or 0)
        apps_dead = int(app.get("fail") or 0) >= 3 and not apps_ok

        s["bad_runs"] = s.get("bad_runs", 0) + 1 if empty else 0
        # Real devices succeeding overrides datacenter runner playback refusals.
        s["play_bad_runs"] = (s.get("play_bad_runs", 0) + 1) if (play_dead and not apps_ok) else 0
        if n is not None and not empty:
            # An outage's zeros stay out of the history, so the usual count holds until it recovers.
            history.append(n)
            s["rows"] = history[-keep:]
            s["last_rows"] = n
            s["baseline"] = baseline
        played = int(probe.get("played") or 0) or apps_ok
        if (n or 0) > 0 and (played or not play_dead):
            s["last_success"] = now
        s["probe"] = probe
        s["apps"] = app

        previous = s.get("status", "ok")
        if s["bad_runs"] >= need:
            status, reason = "down", f"0 rows for {s['bad_runs']} runs (usually ~{baseline:g})"
            if layout:
                pages = (scraped.get("pages") or {}).get("ok", 0)
                reason += f"; layout change likely: {pages} pages loaded, nothing parsed"
            elif scraped.get("verdict") == "down":
                reason += "; site unreachable" + (f" ({scraped['error']})" if scraped.get("error") else "")
        elif s["play_bad_runs"] >= need and apps_dead:
            status, reason = "down", (
                f"nothing plays: {probe.get('play_tried', 0)} playback checks, "
                f"apps {app.get('fail', 0)} fails / 0 ok in 24 h"
            )
        elif s["play_bad_runs"] >= need and not apps_ok:
            status, reason = "check", f"no playback from the runner for {s['play_bad_runs']} runs (apps: no verdict)"
        elif low:
            status, reason = "check", f"{n} rows vs usual ~{baseline:g}"
        else:
            status, reason = "ok", ""
        s["status"], s["reason"] = status, reason
        if status != "ok" and previous == "ok":
            s["since"] = now
        if status == "ok":
            s.pop("since", None)
        if status != previous:
            alerts.append({"site": site, "status": status, "previous": previous, "reason": reason})
        print(
            f"[canary] {site}: rows={n} usual={baseline:g} probe={probe.get('ok', 0)}/{probe.get('probed', 0)} "
            f"played={probe.get('played', 0)}/{probe.get('play_tried', 0)} "
            f"apps={app.get('ok', 0)}ok/{app.get('fail', 0)}fail -> {status}"
            + (f" ({reason})" if reason else ""),
            flush=True,
        )
    return alerts


def overlay(policy: dict, state: dict) -> dict:
    """Mark canary-downed sites disabled in the feed config. A hand-set enabled:false always wins."""
    config_sites = policy.setdefault("config", {}).setdefault("sites", {})
    for site, s in (state.get("sites") or {}).items():
        if s.get("status") != "down":
            continue
        current = config_sites.get(site) or {"enabled": True}
        if not current.get("enabled", True):
            continue
        config_sites[site] = {
            "enabled": False,
            "auto": True,
            "reason": f"canary: {s.get('reason') or 'down'}"[:200],
            "since": int(s.get("since") or 0),
        }
    return policy


# ------------------------------------------------------------------ alerts

def _github(method: str, path: str, token: str, body: dict | None = None):
    req = urllib.request.Request(
        f"https://api.github.com{path}",
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "site-canary",
        },
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read() or b"null")


def send_alerts(alerts: list[dict], state: dict, policy: dict, site_names: dict[str, str]) -> None:
    """Open/close one GitHub issue per site; print when there is no token."""
    p = {**DEFAULTS, **(policy or {})}
    for a in alerts:
        print(f"[canary] ALERT {a['site']}: {a['previous']} -> {a['status']} {a['reason']}", flush=True)
    token, repo = os.environ.get("GITHUB_TOKEN", ""), os.environ.get("GITHUB_REPOSITORY", "")
    if p["alert"] != "github_issue" or not token or not repo:
        return
    active = {site for site, s in (state.get("sites") or {}).items() if s.get("status") != "ok"}
    if not alerts and not active:
        return
    try:
        issues = _github("GET", f"/repos/{repo}/issues?labels={LABEL}&state=open&per_page=100", token) or []
    except Exception as exc:  # noqa: BLE001 - alerts must never fail the feed
        print(f"[canary] alert lookup failed: {exc}", flush=True)
        return
    open_by_title = {i.get("title"): i for i in issues if isinstance(i, dict)}
    for site in sorted(set(active) | {a["site"] for a in alerts}):
        s = state["sites"].get(site) or {}
        title = f"Site down: {site_names.get(site, site)}"
        issue = open_by_title.get(title)
        try:
            if s.get("status") in ("down", "check") and issue is None:
                action = "removed from the feed" if s["status"] == "down" else "still in the feed (check only)"
                _github("POST", f"/repos/{repo}/issues", token, {
                    "title": title,
                    "labels": [LABEL],
                    "body": (
                        f"**{site}** is `{s['status']}`: {s.get('reason')}.\n\n"
                        f"The site is {action}. Rows this run: {s.get('last_rows')}, usual ~{s.get('baseline')}.\n"
                        f"Probe: {json.dumps(s.get('probe') or {})}; apps (24 h): {json.dumps(s.get('apps') or {})}.\n\n"
                        "This issue closes itself when the site recovers."
                    ),
                })
            elif s.get("status") == "ok" and issue is not None:
                number = issue["number"]
                _github("POST", f"/repos/{repo}/issues/{number}/comments", token,
                        {"body": f"Recovered: {s.get('last_rows')} rows this run; back in the feed."})
                _github("PATCH", f"/repos/{repo}/issues/{number}", token, {"state": "closed"})
        except Exception as exc:  # noqa: BLE001
            print(f"[canary] alert for {site} failed: {exc}", flush=True)
