#!/usr/bin/env python3
"""Source health v2: vantage quarantine, combined score, scheduler, site canary."""

from __future__ import annotations

import copy
import unittest

import feed_v2
import probe_scheduler
import site_canary
import source_health as sh

NOW = 1_800_000_000
H = 3600


def fresh() -> dict:
    return {"version": 1, "sources": {}}


class Quarantine(unittest.TestCase):
    url = "https://embed.example/a"

    def test_three_in_a_row_from_one_vantage(self):
        state = fresh()
        for i in range(2):
            sh._record(state, self.url, False, "timeout", NOW + i, True)
        self.assertFalse(sh.is_disabled(state["sources"][self.url], NOW + 2))
        sh._record(state, self.url, False, "timeout", NOW + 2, True)
        self.assertTrue(sh.is_disabled(state["sources"][self.url], NOW + 3))

    def test_two_vantages(self):
        state = fresh()
        sh._record(state, self.url, False, "timeout", NOW, True)
        self.assertFalse(sh.is_disabled(state["sources"][self.url], NOW))
        sh.record_app_result(state, self.url, "TiVo_Stream_4K", False, NOW + 60, NOW + 60)
        self.assertTrue(sh.is_disabled(state["sources"][self.url], NOW + 61))

    def test_old_failure_from_other_vantage_does_not_count(self):
        state = fresh()
        sh.record_app_result(state, self.url, "slang-darwin", False, NOW - 30 * H, NOW - 30 * H)
        sh._record(state, self.url, False, "timeout", NOW, True)
        self.assertFalse(sh.is_disabled(state["sources"][self.url], NOW))

    def test_ok_resets_the_streak(self):
        state = fresh()
        sh._record(state, self.url, False, "timeout", NOW, True)
        sh._record(state, self.url, False, "timeout", NOW + 1, True)
        sh._record(state, self.url, True, "200", NOW + 2, True)
        sh._record(state, self.url, False, "timeout", NOW + 3, True)
        self.assertFalse(sh.is_disabled(state["sources"][self.url], NOW + 4))

    def test_server_block_alone_is_not_a_failure(self):
        state = fresh()
        for i in range(5):
            sh._record(state, self.url, False, "403", NOW + i, True)
        entry = state["sources"][self.url]
        self.assertEqual(entry["fail"], 0)
        self.assertFalse(sh.is_disabled(entry, NOW + 10))
        self.assertEqual(entry["server_blocked"], 5)

    def test_server_block_counts_alongside_an_app_failure(self):
        state = fresh()
        sh.record_app_result(state, self.url, "slang-darwin", False, NOW, NOW)
        sh._record(state, self.url, False, "403", NOW + 1, True)
        entry = state["sources"][self.url]
        self.assertEqual(entry["fail"], 1)
        self.assertTrue(sh.is_disabled(entry, NOW + 2))

    def test_app_success_lifts_quarantine(self):
        state = fresh()
        for i in range(3):
            sh._record(state, self.url, False, "timeout", NOW + i, True)
        self.assertTrue(sh.is_disabled(state["sources"][self.url], NOW + 5))
        sh.record_app_result(state, self.url, "TiVo_Stream_4K", True, NOW + 6, NOW + 6, height=1080)
        entry = state["sources"][self.url]
        self.assertFalse(sh.is_disabled(entry, NOW + 7))
        self.assertEqual(entry["height"], 1080)

    def test_no_playback_cannot_quarantine(self):
        state = fresh()
        for i in range(5):
            sh.record_playback(state, self.url, {"playing": False, "status": "no-playback"}, NOW + i)
        entry = state["sources"][self.url]
        self.assertFalse(sh.is_disabled(entry, NOW + 10))
        self.assertTrue(sh.is_weak_source({"url": self.url}, entry, None))

    def test_legacy_streak_migrates_to_ci_vantage(self):
        entry = {"ok": 0, "fail": 2, "streak": 2, "last_fail": NOW}
        sh._migrate_entry(entry)
        self.assertEqual(entry["vantages"]["ci"]["streak"], 2)


class Score(unittest.TestCase):
    def test_app_results_outweigh_probes(self):
        probe_good_app_bad = {"ok": 6, "fail": 0, "app_ok": 0, "app_fail": 3}
        probe_bad_app_good = {"ok": 0, "fail": 3, "app_ok": 3, "app_fail": 0}
        self.assertLess(sh.health_score(probe_good_app_bad), 0.4)
        self.assertGreater(sh.health_score(probe_bad_app_good), 0.6)
        self.assertEqual(sh.health_score({}), 0.5)

    def test_selection_prefers_app_verified_over_a_better_label(self):
        state = {"sources": {
            "https://a.example/1": {"app_ok": 4, "app_fail": 0},
            "https://b.example/1": {"app_ok": 0, "app_fail": 4},
        }}
        good = {"url": "https://a.example/1", "label": "SD"}
        bad = {"url": "https://b.example/1", "label": "1080p"}
        k_good = sh._selection_key(good, state["sources"][good["url"]], None, None, {}, state, 1)
        k_bad = sh._selection_key(bad, state["sources"][bad["url"]], None, None, {}, state, 0)
        self.assertLess(k_good, k_bad)


def event(key, status, starts, n=6, ends=0):
    return {
        "canonical_key": key, "status": status, "starts_at": starts, "ends_at": ends,
        "sources": [{"url": f"https://{key}.example/{i}", "label": "HD", "origin": "PPV", "kind": "embed"} for i in range(n)],
    }


class Scheduler(unittest.TestCase):
    policy = {"budget_per_hour": 1200, "live_before_min": 30, "live_after_min": 15,
              "top_n": 2, "always_on_every_hours": 6}

    def test_budget_scales_with_time_since_last_run(self):
        self.assertEqual(probe_scheduler.run_budget(self.policy, {}, NOW), 600)
        self.assertEqual(probe_scheduler.run_budget(self.policy, {"last_probe_run": NOW - 1800}, NOW), 600)
        self.assertEqual(probe_scheduler.run_budget(self.policy, {"last_probe_run": NOW - 60}, NOW), 300)
        self.assertEqual(probe_scheduler.run_budget(self.policy, {"last_probe_run": NOW - 5 * H}, NOW), 1200)

    def test_window(self):
        w = probe_scheduler.in_window
        self.assertTrue(w({"status": "live"}, NOW, 30, 15))
        self.assertTrue(w({"status": "future", "starts_at": NOW + 20 * 60}, NOW, 30, 15))
        self.assertFalse(w({"status": "future", "starts_at": NOW + 2 * H}, NOW, 30, 15))
        self.assertFalse(w({"status": "always_live"}, NOW, 30, 15))

    def test_tiers_and_rotation(self):
        events = [
            event("live", "live", NOW - H),
            event("soon", "future", NOW + 10 * 60),
            event("later", "future", NOW + 5 * H),
            event("chan", "always_live", 0, n=3),
        ]
        state = fresh()
        plan = probe_scheduler.plan_probes(events, state, NOW, policy=self.policy)
        page = plan["page"]
        # live window: top 2 of each in-window event first
        self.assertEqual(page[:4], ["https://live.example/0", "https://live.example/1",
                                    "https://soon.example/0", "https://soon.example/1"])
        self.assertIn("https://chan.example/2", page)
        self.assertIn("https://later.example/0", page)
        self.assertNotIn("https://later.example/2", page)  # upcoming spill is top_n only
        # 30 min later: live window again, but 24/7 and upcoming are not due yet
        plan2 = probe_scheduler.plan_probes(events, state, NOW + 1800, policy=self.policy)
        self.assertIn("https://live.example/0", plan2["page"])
        self.assertNotIn("https://chan.example/0", plan2["page"])
        self.assertNotIn("https://later.example/0", plan2["page"])
        # 6 h later the 24/7 channel is due again
        plan3 = probe_scheduler.plan_probes(events, state, NOW + 6 * H + 1, policy=self.policy)
        self.assertIn("https://chan.example/0", plan3["page"])

    def test_budget_caps_the_plan(self):
        events = [event(f"e{i}", "live", NOW - H) for i in range(10)]
        plan = probe_scheduler.plan_probes(events, fresh(), NOW, policy={**self.policy, "budget_per_hour": 10})
        self.assertEqual(len(plan["page"]), 5)


class Canary(unittest.TestCase):
    canary = {"min_yield_ratio": 0.2, "baseline_runs": 48, "bad_runs": 3, "alert": "none"}

    def run_rows(self, state, values, site="isportsurge", probe=None, apps=None):
        alerts = []
        for i, n in enumerate(values):
            alerts += site_canary.update(state, {site: n}, {site: probe or {}}, {site: apps or {}},
                                         NOW + i * 1800, self.canary)
        return alerts

    def test_zero_rows_takes_a_normally_full_site_down_and_back(self):
        state = fresh()
        self.run_rows(state, [20] * 10)
        alerts = self.run_rows(state, [0, 0])
        self.assertEqual(alerts, [])
        alerts = self.run_rows(state, [0])
        self.assertEqual(alerts[0]["status"], "down")
        policy = site_canary.overlay(copy.deepcopy(feed_v2.load_policy()), state)
        self.assertEqual(policy["config"]["sites"]["isportsurge"]["enabled"], False)
        self.assertTrue(policy["config"]["sites"]["isportsurge"]["auto"])
        alerts = self.run_rows(state, [15])
        self.assertEqual(alerts[0]["status"], "ok")
        policy = site_canary.overlay(copy.deepcopy(feed_v2.load_policy()), state)
        self.assertTrue(policy["config"]["sites"]["isportsurge"]["enabled"])

    def test_often_empty_site_is_not_judged(self):
        state = fresh()
        self.run_rows(state, [5, 0, 6, 0, 4, 0, 7, 0, 0, 0, 0])
        self.assertEqual(state["sites"]["isportsurge"]["status"], "ok")

    def test_low_yield_is_check_only(self):
        state = fresh()
        self.run_rows(state, [50] * 10)
        alerts = self.run_rows(state, [3])
        self.assertEqual(alerts[0]["status"], "check")
        policy = site_canary.overlay(copy.deepcopy(feed_v2.load_policy()), state)
        self.assertTrue(policy["config"]["sites"]["isportsurge"]["enabled"])

    def test_runner_cannot_play_is_check_unless_apps_agree(self):
        dead = {"probed": 6, "ok": 6, "play_tried": 4, "played": 0}
        state = fresh()
        self.run_rows(state, [20] * 6, probe=dead)
        self.assertEqual(state["sites"]["isportsurge"]["status"], "check")
        state = fresh()
        self.run_rows(state, [20] * 6, probe=dead, apps={"ok": 0, "fail": 4})
        self.assertEqual(state["sites"]["isportsurge"]["status"], "down")

    def test_apps_playing_overrides_runner_playback_failure(self):
        dead = {"probed": 6, "ok": 6, "play_tried": 4, "played": 0}
        state = fresh()
        self.run_rows(state, [20] * 6, probe=dead, apps={"ok": 2, "fail": 0})
        self.assertEqual(state["sites"]["isportsurge"]["status"], "ok")
        self.assertEqual(state["sites"]["isportsurge"]["play_bad_runs"], 0)

    def test_manual_disable_wins(self):
        state = fresh()
        self.run_rows(state, [20] * 10 + [0, 0, 0])
        policy = copy.deepcopy(feed_v2.load_policy())
        policy["config"]["sites"]["isportsurge"] = {"enabled": False}
        site_canary.overlay(policy, state)
        self.assertEqual(policy["config"]["sites"]["isportsurge"], {"enabled": False})

    def test_site_rows_maps_modules_to_sites(self):
        policy = feed_v2.load_policy()
        rows = site_canary.site_rows(policy, 40, {"EXTRA": 3, "NBA": 2}, {"totalsporteki": 7, "isportsurge": 0})
        self.assertEqual(rows, {"ppv": 40, "streamcorner": 5, "totalsportek": 7, "isportsurge": 0})


class Schema(unittest.TestCase):
    def test_feed_with_canary_overlay_validates(self):
        state = fresh()
        state["sites"] = {"roxiestreams": {"status": "down", "reason": "0 rows for 3 runs", "since": NOW}}
        policy = site_canary.overlay(feed_v2.load_policy(), state)
        ev = event("x", "live", NOW - H, n=1)
        ev.update({"title": "A vs B", "category": "Soccer", "league": "", "time_label": "LIVE", "updated_at": NOW})
        payload = feed_v2.finalize([ev], NOW, policy)
        self.assertEqual(feed_v2.validate(payload), [])
        self.assertFalse(payload["config"]["sites"]["roxiestreams"]["enabled"])


def char_array_js(name: str, text: str, k1: int, k2: int, fa: str, fb: str) -> str:
    """Build a streamxhd-style scrambled string: shuffled [index, base64(junk+digits+junk)] pairs."""
    import base64
    import random

    pairs = [(i * 7 + 3, base64.b64encode(f"aB{ord(ch) + k1 + k2}xY".encode()).decode()) for i, ch in enumerate(text)]
    random.Random(len(text)).shuffle(pairs)
    body = ",".join(f'[{i},"{v}"]' for i, v in pairs)
    return (f'var {name}="",arr{name}=[]; arr{name}=[{body}]; arr{name}.sort((a,b)=>a[0]-b[0]); '
            f"var k={fa}()+{fb}(); arr{name}.forEach(e=>{{}}); "
            f"function {fa}(){{return {k1};}} function {fb}(){{return {k2};}}")


class PlaylistFetch(unittest.TestCase):
    url = "https://khala.example/global/appletv2/index.m3u8?ip=1.2.3.4&token=33998049ba6b-1795343132"

    def test_scrambled_url_decodes_whole(self):
        page = (char_array_js("adsPayload", '{"enabled":true,"url":"https://ads.example/x"}', 64995, 696565, "tA", "jB")
                + char_array_js("playbackURL", self.url, 75710, 823288, "sM", "jI"))
        self.assertEqual(sh._manifest_from_text(page), self.url)

    def test_worker_players_are_browser_only(self):
        page = 'const cdnUrl = "https://edge.example/sh/access/U0hBQ0NFU1M/cdn/hls/x.json"; const playlistUrl = "https://edge.example/sh/cdn/hls/x.m3u8";'
        self.assertTrue(sh._browser_only_player(page))
        meta = sh._quality_meta("https://embed.example/hls_co.php", page, sh.PROBE_HEADERS, 5)
        self.assertEqual(meta["manifest_status"], "")
        self.assertTrue(meta["manifest"].endswith("x.m3u8"))

    def test_playlist_403_is_a_hint_not_a_failure(self):
        info = {"ok": True, "status": "200", "height": 0, "manifest_status": "403"}
        self.assertEqual(sh.apply_manifest_failures({"u": info}), 1)
        self.assertTrue(info["ok"])
        self.assertEqual(info["manifest_dead"], "403")
        state = {"sources": {"https://a.example/1": {"ok": 1, "fail": 0, "height": 720, "verified": True}}}
        for i in range(4):
            sh._record(state, "https://a.example/1", True, "200", NOW + i, True, manifest_dead="404")
        entry = state["sources"]["https://a.example/1"]
        self.assertEqual((entry["fail"], entry.get("dead_fails", 0), entry.get("blocked", False)), (0, 0, False))
        self.assertEqual(entry["height"], 0)  # stale measurement dropped
        self.assertFalse(sh.is_disabled(entry, NOW + 10))

    def test_old_playlist_quarantines_are_forgiven_once(self):
        state = {"sources": {
            "https://a.example/1": {"last_status": "manifest-403", "fail": 4, "streak": 4, "quarantines": 2,
                                    "disabled_until": NOW + H, "vantages": {"ci": {"streak": 4}}},
            "https://b.example/1": {"last_status": "manifest-404", "blocked": True, "blocked_reason": "HTTP manifest-404 x2",
                                    "dead_fails": 2, "fail": 2},
            "https://c.example/1": {"last_status": "manifest-403", "fail": 3, "app_fail": 2, "disabled_until": NOW + H},
            "https://d.example/1": {"last_status": "timeout", "fail": 3, "disabled_until": NOW + H},
        }}
        self.assertEqual(sh._forgive_manifest_failures(state), 2)
        src = state["sources"]
        self.assertFalse(sh.is_disabled(src["https://a.example/1"], NOW))
        self.assertFalse(sh.is_disabled(src["https://b.example/1"], NOW))
        self.assertTrue(sh.is_disabled(src["https://c.example/1"], NOW))  # an app failed it too
        self.assertTrue(sh.is_disabled(src["https://d.example/1"], NOW))
        src["https://a.example/1"].update({"last_status": "manifest-403", "fail": 5})
        self.assertEqual(sh._forgive_manifest_failures(state), 0)  # runs once


class LongTermRankingAndTelemetry(unittest.TestCase):
    def test_url_family_normalizes_ephemeral_segments(self):
        self.assertEqual(
            sh.url_family("https://amazon.com.getsugatensho.sbs/beta?p=abc123token"),
            "amazon.com.getsugatensho.sbs/beta",
        )
        self.assertEqual(
            sh.url_family("https://taifood-blog.asia/embed/cfb/2026-11-21/ark-tex/skycast"),
            "taifood-blog.asia/embed/cfb/{date}",
        )
        self.assertEqual(
            sh.url_family("https://104game.click/player/2/138"),
            "104game.click/player/{n}/{n}",
        )
        self.assertEqual(
            sh.url_family("https://live6.totalsportek.gives/Amari-Jones-vs-DeAundre-Pettus/81392"),
            "live6.totalsportek.gives/{slug}/{n}",
        )

    def test_page_200_never_beats_proven_app_history(self):
        state = {
            "sources": {},
            "hosts": {
                "good.example": {"ok": 50, "fail": 5, "updated_at": NOW},
                "dead.example": {"ok": 0, "fail": 20, "updated_at": NOW},
            },
        }
        good_src = {"url": "https://good.example/game/1", "label": "Stream 1"}
        dead_src = {"url": "https://dead.example/beta?p=xyz", "label": "Stream 1"}
        k_good = sh._selection_key(good_src, {}, None, None, {}, state, 1, now=NOW)
        k_dead = sh._selection_key(dead_src, {}, {"ok": True, "latency_ms": 50}, True, {}, state, 0, now=NOW)
        self.assertLess(k_good, k_dead)
        self.assertTrue(sh.is_weak_source(dead_src, {}, True, state=state, now=NOW))
        self.assertFalse(sh.is_weak_source(good_src, {}, None, state=state, now=NOW))

    def test_host_interleaving_prevents_same_host_clumping(self):
        state = {"sources": {}, "hosts": {}}
        raw = [
            {"url": "https://a.example/1", "label": "HD"},
            {"url": "https://a.example/2", "label": "HD"},
            {"url": "https://a.example/3", "label": "HD"},
            {"url": "https://b.example/1", "label": "HD"},
            {"url": "https://c.example/1", "label": "HD"},
        ]
        cands = [
            {
                "src": s,
                "host": sh.host_of(s["url"]),
                "ok": True,
                "weak": False,
                "key": sh._selection_key(s, {}, None, True, {}, state, i, now=NOW),
            }
            for i, s in enumerate(raw)
        ]
        selected = sh._select_best_sources(cands, limit=4)
        hosts = [sh.host_of(s["url"]) for s in selected]
        self.assertEqual(hosts, ["a.example", "b.example", "c.example", "a.example"])

    def test_bad_host_does_not_cut_ahead_of_second_good_mirror(self):
        state = {
            "sources": {},
            "hosts": {
                "good.example": {"ok": 40, "fail": 2, "updated_at": NOW},
                "dead.example": {"ok": 0, "fail": 20, "updated_at": NOW},
            },
        }
        raw = [
            {"url": "https://good.example/1", "label": "HD"},
            {"url": "https://good.example/2", "label": "HD"},
            {"url": "https://dead.example/1", "label": "HD"},
        ]
        cands = [
            {
                "src": s,
                "host": sh.host_of(s["url"]),
                "ok": None,
                "weak": False,
                "key": sh._selection_key(s, {}, None, None, {}, state, i, now=NOW),
            }
            for i, s in enumerate(raw)
        ]
        selected = sh._select_best_sources(cands, limit=3)
        self.assertEqual([sh.host_of(s["url"]) for s in selected], ["good.example", "good.example", "dead.example"])

    def test_time_decay_fades_stale_host_failures(self):
        stale = {"hosts": {"old.example": {"ok": 0, "fail": 20, "updated_at": NOW - 60 * 86400}}}
        fresh_bad = {"hosts": {"old.example": {"ok": 0, "fail": 20, "updated_at": NOW}}}
        s_stale = sh.health_score({}, stale, "https://old.example/1", now=NOW)
        s_fresh = sh.health_score({}, fresh_bad, "https://old.example/1", now=NOW)
        self.assertGreater(s_stale, 0.45)
        self.assertLess(s_fresh, 0.25)

    def test_telemetry_ingest_accepts_source_url_per_file_cursors_and_scans(self):
        ch_url = "https://cdn.example/live/ch1.m3u8"
        t_now = sh._iso_epoch("2026-11-21T20:05:00Z")
        state = {
            "version": 1,
            "sources": {},
            "channels": {"urls": {sh._channel_url_key(ch_url): {"demoted": True, "fail_streak": 3}}},
        }
        # Desktop writes a newer timestamp first
        res1 = sh.ingest_telemetry_rows(state, {
            "telemetry/slang-darwin.jsonl": [
                {
                    "ts": "2026-11-21T20:00:00Z", "type": "source_result",
                    "device_id": "mac-1", "url": "https://x.example/live/1",
                    "source_host": "x.example", "ok": False,
                },
            ],
        }, now=t_now)
        self.assertEqual(res1["rows"], 1)
        # TiVo flushes delayed older rows using Android TV's `source_url` + `device_id`
        res2 = sh.ingest_telemetry_rows(state, {
            "telemetry/slang-darwin.jsonl": [
                {
                    "ts": "2026-11-21T20:00:00Z", "type": "source_result",
                    "device_id": "mac-1", "url": "https://x.example/live/1",
                    "source_host": "x.example", "ok": False,
                },
            ],
            "telemetry/tivo-abc.jsonl": [
                {
                    "ts": "2026-11-21T19:30:00Z", "type": "source_result",
                    "device_id": "tivo-abc", "source_url": "https://x.example/live/1",
                    "host": "x.example", "ok": False,
                },
                {
                    "ts": "2026-11-21T19:31:00Z", "type": "source_scan",
                    "device_id": "tivo-abc", "source_url": "https://scan.example/1",
                    "host": "scan.example", "ok": False,
                },
                {
                    "ts": "2026-11-21T19:32:00Z", "type": "source_result",
                    "device_id": "tivo-abc", "source_url": ch_url,
                    "host": "cdn.example", "ok": True,
                },
            ],
        }, now=t_now)
        # All 3 delayed TiVo rows were ingested despite older timestamp than mac-1!
        self.assertEqual(res2["rows"], 3)
        # Two distinct vantages (mac-1 + tivo-abc) quarantined https://x.example/live/1
        self.assertTrue(sh.is_disabled(state["sources"]["https://x.example/live/1"], t_now))
        # source_scan failure did NOT quarantine or poison host counts
        scan_ent = state["sources"]["https://scan.example/1"]
        self.assertFalse(sh.is_disabled(scan_ent, t_now))
        self.assertEqual(scan_ent["scan_fail"], 1.0)
        self.assertEqual(state["hosts"]["scan.example"]["fail"], 0.0)
        # Channel app success lifted demotion
        self.assertFalse(state["channels"]["urls"][sh._channel_url_key(ch_url)]["demoted"])

    def test_qoe_no_audio_scores_below_clean_watch(self):
        state = fresh()
        sh.record_app_result(state, "https://a.example/clean", "tv1", True, NOW, NOW, audio_ok=True)
        sh.record_app_result(state, "https://a.example/muted", "tv1", True, NOW, NOW, reason="video_no_audio", audio_ok=False)
        self.assertGreater(
            sh.health_score(state["sources"]["https://a.example/clean"], state, "https://a.example/clean", now=NOW),
            sh.health_score(state["sources"]["https://a.example/muted"], state, "https://a.example/muted", now=NOW),
        )

    def test_scheduler_probes_diverse_hosts_first(self):
        ev = {
            "canonical_key": "ev1", "status": "live", "starts_at": NOW - H, "ends_at": 0,
            "sources": [
                {"url": "https://h1.example/1", "label": "HD", "origin": "PPV", "kind": "embed"},
                {"url": "https://h1.example/2", "label": "HD", "origin": "PPV", "kind": "embed"},
                {"url": "https://h2.example/1", "label": "HD", "origin": "PPV", "kind": "embed"},
            ],
        }
        plan = probe_scheduler.plan_probes([ev], fresh(), NOW, policy={"budget_per_hour": 100, "top_n": 2})
        self.assertEqual(plan["page"][:2], ["https://h1.example/1", "https://h2.example/1"])


if __name__ == "__main__":
    unittest.main()
