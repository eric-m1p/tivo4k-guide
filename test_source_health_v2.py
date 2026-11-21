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


if __name__ == "__main__":
    unittest.main()
