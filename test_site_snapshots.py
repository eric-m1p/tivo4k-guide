"""python3 -m unittest test_site_snapshots

Scraper plugins against saved pages (snapshots/, see site_snapshots.py), plus
the plugin harness: page counters, backoff, layout-vs-down verdicts and the
canary's layout alert.
"""
import os
import sys
import unittest
import urllib.error
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import live_sites_common as C  # noqa: E402
import site_canary  # noqa: E402
import site_plugins  # noqa: E402
import site_snapshots as snaps  # noqa: E402

NOW = 1_800_000_000


class SnapshotTest(unittest.TestCase):
    """Same pages in, same rows out. A failure here means a parser change altered
    what a known page produces: check the diff, then re-record if it is intended."""

    def check(self, site):
        if not os.path.exists(snaps.path_for(site)):
            self.skipTest(f"no snapshot for {site}")
        with mock.patch("builtins.print"):
            ok, note = snaps.matches(site)
        self.assertTrue(ok, f"{note}: replayed output differs from the saved output")

    def test_isportsurge(self):
        self.check("isportsurge")

    def test_buffstreams(self):
        self.check("buffstreams")

    def test_totalsporteki(self):
        self.check("totalsporteki")

    def test_roxiestreams(self):
        self.check("roxiestreams")

    def test_streamcorner(self):
        self.check("streamcorner")

    def test_snapshots_are_not_empty(self):
        for site in snaps.SITES:
            if not os.path.exists(snaps.path_for(site)):
                continue
            exp = snaps.load(site)["expected"]
            n = sum(len(v) for v in exp.values()) if isinstance(exp, dict) else len(exp)
            self.assertGreater(n, 0, f"{site} snapshot pins an empty result")

    def test_layout_change_is_seen_on_replay(self):
        """Pages that load but no longer match the parser give 0 rows with pages counted."""
        snap = snaps.load("isportsurge")
        snap["pages"] = {u: "<html><body><main>redesigned</main></body></html>" for u in snap["pages"]}
        state = {}
        with mock.patch.object(C, "get", lambda url, referer="", timeout=25, retries=2: (
                C.note_page(True), snap["pages"].get(url, "<html></html>"))[1]), \
                mock.patch("builtins.print"):
            rows, stats = site_plugins.run("isportsurge", state, NOW, {})
        self.assertEqual(rows, [])
        self.assertEqual(stats["verdict"], "layout")
        self.assertGreater(stats["pages"]["ok"], 0)


class HarnessTest(unittest.TestCase):
    def test_verdicts(self):
        self.assertEqual(site_plugins.verdict(3, {"ok": 0}), "ok")
        self.assertEqual(site_plugins.verdict(0, {"ok": 5, "fail": 1}), "layout")
        self.assertEqual(site_plugins.verdict(0, {"ok": 0, "fail": 4}), "down")
        self.assertEqual(site_plugins.verdict(0, {"ok": 0, "throttled": 2}), "down")
        self.assertEqual(site_plugins.verdict(0, {}, "TimeoutError"), "down")
        self.assertEqual(site_plugins.verdict(0, {}), "empty")

    def test_backoff_doubles_caps_and_resets(self):
        entry, policy = {}, {"backoff_after_runs": 2, "backoff_max_min": 120}
        site_plugins.record(entry, "down", NOW, policy)
        self.assertNotIn("skip_until", entry)                      # 1 down run: keep trying
        site_plugins.record(entry, "down", NOW, policy)
        self.assertEqual(entry["skip_until"], NOW + 30 * 60)       # 2nd: skip one run
        site_plugins.record(entry, "down", NOW, policy)
        self.assertEqual(entry["skip_until"], NOW + 60 * 60)       # then 2 runs
        for _ in range(5):
            site_plugins.record(entry, "down", NOW, policy)
        self.assertEqual(entry["skip_until"], NOW + 120 * 60)      # capped
        site_plugins.record(entry, "layout", NOW, policy)          # site is up: no backoff
        self.assertNotIn("skip_until", entry)
        self.assertEqual(entry["down_runs"], 0)

    def test_run_skips_while_backing_off_and_survives_exceptions(self):
        state = {"scrapers": {"isportsurge": {"skip_until": NOW + 60, "down_runs": 3}}}
        fake = mock.Mock(fetch=mock.Mock(return_value=[{"x": 1}]))
        with mock.patch.object(site_plugins, "module_for", return_value=fake), mock.patch("builtins.print"):
            rows, stats = site_plugins.run("isportsurge", state, NOW, {})
            self.assertTrue(stats["skipped"])
            fake.fetch.assert_not_called()
            fake.fetch.side_effect = RuntimeError("boom")
            rows, stats = site_plugins.run("isportsurge", state, NOW + 120, {})
        self.assertEqual((rows, stats["verdict"]), ([], "down"))
        self.assertIn("boom", stats["error"])

    def test_get_counts_pages_and_does_not_retry_404(self):
        calls = []

        def fake_urlopen(req, timeout=0, context=None):
            calls.append(req.full_url)
            raise urllib.error.HTTPError(req.full_url, 404, "nf", {}, None)

        C.begin_site("t")
        with mock.patch.object(C.urllib.request, "urlopen", fake_urlopen), mock.patch("builtins.print"):
            self.assertEqual(C.get("https://x/a", retries=2), "")
        stats = C.end_site()
        self.assertEqual(len(calls), 1)
        self.assertEqual(stats, {"ok": 0, "fail": 1, "throttled": 0})

    def test_get_honours_retry_after_on_429(self):
        calls, sleeps = [], []

        class Resp:
            headers = {}

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def read(self):
                return b"<html>ok</html>"

        def fake_urlopen(req, timeout=0, context=None):
            calls.append(1)
            if len(calls) == 1:
                raise urllib.error.HTTPError(req.full_url, 429, "slow", {"Retry-After": "3"}, None)
            return Resp()

        C.begin_site("t")
        with mock.patch.object(C.urllib.request, "urlopen", fake_urlopen), \
                mock.patch.object(C.time, "sleep", sleeps.append):
            self.assertEqual(C.get("https://x/a"), "<html>ok</html>")
        self.assertEqual(sleeps, [3.0])
        self.assertEqual(C.end_site(), {"ok": 1, "fail": 0, "throttled": 1})


class CanaryLayoutTest(unittest.TestCase):
    def test_down_reason_names_layout_change(self):
        state = {"sites": {"isportsurge": {"rows": [40] * 10, "bad_runs": 2, "status": "ok"}}}
        scrape = {"isportsurge": {"verdict": "layout", "pages": {"ok": 12, "fail": 0}}}
        with mock.patch("builtins.print"):
            alerts = site_canary.update(state, {"isportsurge": 0}, {}, {}, NOW, {"bad_runs": 3}, scrape=scrape)
        s = state["sites"]["isportsurge"]
        self.assertEqual(s["status"], "down")
        self.assertIn("layout change likely: 12 pages loaded", s["reason"])
        self.assertEqual(alerts[0]["status"], "down")

    def test_down_reason_names_unreachable(self):
        state = {"sites": {"buff": {"rows": [10] * 10, "bad_runs": 2, "status": "ok"}}}
        scrape = {"buff": {"verdict": "down", "pages": {"ok": 0, "fail": 9}, "error": ""}}
        with mock.patch("builtins.print"):
            site_canary.update(state, {"buff": 0}, {}, {}, NOW, {"bad_runs": 3}, scrape=scrape)
        self.assertIn("site unreachable", state["sites"]["buff"]["reason"])


if __name__ == "__main__":
    unittest.main()
