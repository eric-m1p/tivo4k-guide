"""python3 -m unittest test_channel_health"""
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import channel_alts  # noqa: E402
import channel_health as chh  # noqa: E402

NOW = 1_800_000_000
POLICY = {**chh.DEFAULTS, "per_run": 3, "every_hours": 24, "demote_after_fails": 2}


def ch(cid, *urls):
    return {"id": cid, "name": cid, "sources": [{"url": u} for u in urls]}


class PlanTest(unittest.TestCase):
    def test_oldest_first_main_before_backup_and_not_due_skipped(self):
        channels = [ch("A", "https://a1", "https://a2"), ch("B", "https://b1", "https://b2")]
        urls = {
            chh.url_key("https://a1"): {"last_probe": NOW - 3600},       # checked an hour ago: not due
            chh.url_key("https://b1"): {"last_probe": NOW - 3 * 86400},  # oldest
        }
        picked = [s["url"] for s in chh.plan(channels, urls, NOW, POLICY)]
        # never-probed URLs (last 0) come first, mains before backups; a1 is not due
        self.assertEqual(picked, ["https://a2", "https://b2", "https://b1"])

    def test_budget_and_dedupe(self):
        channels = [ch("A", "https://x"), ch("B", "https://x", "https://y"), ch("C", "ftp://z")]
        picked = [s["url"] for s in chh.plan(channels, {}, NOW, {**POLICY, "per_run": 10})]
        self.assertEqual(picked, ["https://x", "https://y"])

    def test_policy_from_feed(self):
        p = chh.policy_from({"policy": {"probe": {"channels": {"per_run": 5, "_doc": "x"}}}})
        self.assertEqual(p["per_run"], 5)
        self.assertEqual(p["every_hours"], 24)
        self.assertNotIn("_doc", p)


class RecordTest(unittest.TestCase):
    def test_streak_demotes_and_success_clears(self):
        urls, src = {}, {"url": "https://a", "channel": "A"}
        chh.record(urls, src, False, "x", NOW, 2)
        self.assertFalse(urls[chh.url_key("https://a")]["demoted"])
        chh.record(urls, src, False, "x", NOW, 2)
        self.assertTrue(urls[chh.url_key("https://a")]["demoted"])
        chh.record(urls, src, True, "ok", NOW, 2)
        rec = urls[chh.url_key("https://a")]
        self.assertFalse(rec["demoted"])
        self.assertEqual((rec["ok"], rec["fail"], rec["fail_streak"]), (1, 2, 0))

    def test_reorder_and_down(self):
        channels = [ch("A", "https://a1", "https://a2"), ch("B", "https://b1")]
        urls = {chh.url_key("https://a1"): {"demoted": True}, chh.url_key("https://b1"): {"demoted": True}}
        self.assertEqual(chh.reorder(channels, urls), 2)
        self.assertEqual([s["url"] for s in channels[0]["sources"]], ["https://a2", "https://a1"])
        self.assertNotIn("down", channels[0])
        self.assertTrue(channels[1]["down"])
        self.assertEqual(chh.reorder(channels, urls), 0)  # stable: nothing moves twice
        urls[chh.url_key("https://b1")]["demoted"] = False
        self.assertEqual(chh.reorder(channels, urls), 1)
        self.assertNotIn("down", channels[1])

    def test_prune(self):
        urls = {chh.url_key("https://a"): {}, chh.url_key("https://gone"): {}}
        self.assertEqual(chh.prune(urls, [ch("A", "https://a")]), 1)
        self.assertEqual(list(urls), [chh.url_key("https://a")])


class MainTest(unittest.TestCase):
    def test_run_writes_state_and_keeps_other_keys(self):
        with tempfile.TemporaryDirectory() as d:
            paths = {k: os.path.join(d, k + ".json") for k in ("channels", "state", "policy")}
            json.dump({"v": 1, "channels": [ch("A", "https://a1", "https://a2")]}, open(paths["channels"], "w"))
            json.dump({"version": 1, "sources": {"keep": {}}}, open(paths["state"], "w"))
            json.dump({"policy": {"probe": {"channels": {"demote_after_fails": 1}}}}, open(paths["policy"], "w"))
            argv = ["channel_health.py"] + [f"--{k}={v}" for k, v in paths.items()]
            fake = lambda src, timeout=8: (src["url"].endswith("2"), "fake")
            with mock.patch.object(sys, "argv", argv), mock.patch.object(chh, "probe", fake):
                self.assertEqual(chh.main(), 0)
            state = json.load(open(paths["state"]))
            self.assertIn("keep", state["sources"])
            self.assertEqual(state["channels"]["stats"]["probed"], 2)
            self.assertEqual(state["channels"]["stats"]["demoted"], 1)
            doc = json.load(open(paths["channels"]))
            self.assertEqual([s["url"] for s in doc["channels"][0]["sources"]], ["https://a2", "https://a1"])


class AltsTest(unittest.TestCase):
    def test_match_filter_and_cap(self):
        app = [{"id": "s1", "name": "Wine, Watches & Whiskey"}, {"id": "s2", "name": "ABC News Live"},
               {"id": "s3", "name": "Nothing Like It"}, {"id": "s4", "name": "Down Channel"}]
        live = [
            {"id": "W", "name": "Wine Watches & Whiskey", "sources": [
                {"url": "https://w/1.m3u8", "height": 1080},
                {"url": "https://w/2.m3u8", "user_agent": "UA"},        # needs a UA: skipped
                {"url": "http://w/3.m3u8"},                             # not https: skipped
                {"url": "https://w/4.ts"},                              # not HLS: skipped
                {"url": "https://w/5.m3u8", "referrer": "https://r/"},
                {"url": "https://w/6.m3u8"}, {"url": "https://w/7.m3u8"}]},
            {"id": "K", "name": "ABC News Live", "local": True, "sources": [{"url": "https://k.m3u8"}]},
            {"id": "D", "name": "Down Channel", "down": True, "sources": [{"url": "https://d.m3u8"}]},
        ]
        health = {chh.url_key("https://w/6.m3u8"): {"demoted": True}}
        out = channel_alts.build(app, live, health)
        self.assertEqual(list(out), ["s1"])
        self.assertEqual(out["s1"], [["https://w/1.m3u8", "", 1080], ["https://w/5.m3u8", "https://r/", 0],
                                     ["https://w/7.m3u8", "", 0]])


if __name__ == "__main__":
    unittest.main()
