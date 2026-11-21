"""Tests for the v2 feed pass: python3 -m unittest test_feed_v2 (no network)."""

import datetime
import unittest

import feed_v2 as v2

NOW = int(datetime.datetime(2026, 11, 21, 3, 0, tzinfo=datetime.timezone.utc).timestamp())  # a Friday evening in the US


def ev(title, category="Sports", league="", status="live", start=NOW - 1800, sources=None, key=None, score=""):
    return {
        "canonical_key": key or "ev:" + title.lower().replace(" ", "_"),
        "title": title,
        "category": category,
        "league": league,
        "status": status,
        "starts_at": start,
        "ends_at": start + 3 * 3600 if start else 0,
        "starts_at_iso": "",
        "time_label": "LIVE",
        "score_summary": score,
        "poster_url": "",
        "origins": ["extra"],
        "sources": sources if sources is not None else [src(f"https://x.test/{abs(hash(title))}")],
        "updated_at": NOW,
    }


def src(url, origin="EXTRA", label="STREAM 1"):
    return {"origin": origin, "label": label, "url": url, "kind": "embed", "referer": "https://streamcorner.cloud/"}


def run(events, **kw):
    return v2.enrich(events, NOW, scoreboards=False, **kw)


class TitleTests(unittest.TestCase):
    def test_clean_title(self):
        self.assertEqual(v2.clean_title("Juarez – Atlas"), "Juarez vs Atlas")
        self.assertEqual(v2.clean_title("#7 Texas vs. Baylor"), "Texas vs Baylor")
        self.assertEqual(v2.clean_title("NBA Basketball: Lakers vs Celtics"), "Lakers vs Celtics")
        self.assertEqual(v2.clean_title("Brisbane Roar - Melbourne City | A-League Women"), "Brisbane Roar vs Melbourne City")
        self.assertEqual(v2.clean_title("UFC 323: Prochazka vs Stirling"), "UFC 323: Prochazka vs Stirling")
        self.assertEqual(v2.clean_title("Formula 1 - Las Vegas GP - Race"), "Formula 1 - Las Vegas GP - Race")

    def test_sides(self):
        self.assertEqual(v2.split_sides("FC Juárez vs Atlas"), [["juarez"], ["atlas"]])
        self.assertEqual(v2.split_sides("UFC 323: Jiri Prochazka vs Jamahal Stirling"), [["jiri", "prochazka"], ["jamahal", "stirling"]])
        self.assertEqual(v2.split_sides("WWE Friday Night SmackDown"), [])
        self.assertTrue(v2.sides_match(v2.split_sides("Clemson vs Duke"), v2.split_sides("Duke Blue Devils vs Clemson Tigers")))
        self.assertTrue(v2.sides_match(v2.split_sides("St Mary's vs Saint Louis"), v2.split_sides("Saint Mary's Gaels vs Saint Louis Billikens")))
        self.assertFalse(v2.sides_match(v2.split_sides("Western United vs Melbourne City"), v2.split_sides("Melbourne City vs Western Sydney Wanderers")))
        self.assertFalse(v2.sides_match(v2.split_sides("Melbourne City vs Sydney FC"), v2.split_sides("Melbourne Victory vs Sydney FC")))


class DedupeTests(unittest.TestCase):
    def titles(self, events):
        return sorted(e["title"] for e in run(events))

    def test_team_name_variants(self):
        out = run([ev("Clemson vs Duke", "Ncaa", "NCAA"), ev("Duke Blue Devils vs Clemson Tigers", "Basketball", "NCAAB")])
        self.assertEqual(len(out), 1)
        self.assertEqual(len(out[0]["sources"]), 2)
        self.assertEqual(len(out[0]["aliases"]), 1)

    def test_separators_accents_and_brands(self):
        self.assertEqual(len(run([ev("FC Juárez vs Atlas", "Soccer", "Liga MX"), ev("Juarez – Atlas", "Football", "Liga Mx")])), 1)
        self.assertEqual(len(run([ev("WWE Friday Night SmackDown", "Wrestling", "WWE"), ev("WWE Smackdown", "Wrestling", "WWE")])), 1)
        ufc = [
            ev("UFC 323: Prochazka vs Stirling", "Fighting", "UFC"),
            ev("Jiri Prochazka vs Jamahal Stirling", "MMA", "UFC"),
            ev("UFC 323 Prochazka Stirling", "Fighting", "UFC"),
            ev("Prochazka – Stirling", "Fighting", "UFC"),
        ]
        out = run(ufc)
        self.assertEqual(len(out), 1)
        self.assertTrue(v2.split_sides(out[0]["title"]))  # a two-sided title wins over "UFC 323 …"

    def test_keeps_different_games(self):
        out = run([
            ev("Brisbane Roar vs Melbourne City", "Soccer", "Australian A-League"),
            ev("Brisbane Roar - Melbourne City | A-League Women", "Football", "A-League Women"),
            ev("Western United vs Melbourne City", "Soccer", "A-League"),
            ev("Melbourne City vs Western Sydney Wanderers", "Soccer", "A-League"),
        ])
        self.assertEqual(len(out), 4)

    def test_bare_ap_rank_merges_and_preserves_team_numbers(self):
        out = run([
            ev("15 Iowa vs Illinois", "American Football", "NCAA Division 1 Football", key="ev:ranked"),
            ev("Iowa Hawkeyes at Illinois Fighting Illini", "American Football", "NCAAF", key="ev:full"),
        ])
        self.assertEqual(len(out), 1)
        self.assertEqual(len(out[0]["sources"]), 2)
        self.assertEqual(v2.clean_title("Chattanooga vs 3 Alabama"), "Chattanooga vs Alabama")
        self.assertEqual(v2.clean_title("1. FC Koln vs Schalke 04"), "1. FC Koln vs Schalke 04")
        self.assertEqual(v2.clean_title("49ers vs Rams"), "49ers vs Rams")

    def test_far_apart_times_stay_apart(self):
        out = run([ev("Lakers vs Celtics", "Basketball", "NBA"), ev("Lakers vs Celtics", "Basketball", "NBA", start=NOW + 2 * 86400, status="future", key="ev:later")])
        self.assertEqual(len(out), 2)


class ClassifyTests(unittest.TestCase):
    def one(self, *args, **kw):
        return run([ev(*args, **kw)])[0]

    def test_groups_and_icons(self):
        e = self.one("Chiefs vs Bills", "American Football", "NFL")
        self.assertEqual((e["sport"], e["group"], e["icon"]), ("american_football", "NFL", "sports_football"))
        e = self.one("Texas vs Baylor", "American Football", "NCAAF")
        self.assertEqual(e["group"], "College Football")
        e = self.one("Arsenal vs Chelsea", "Football", "Premier League")
        self.assertEqual((e["sport"], e["category"], e["group"]), ("soccer", "Soccer", "Soccer"))
        e = self.one("Leinster vs Munster", "Rugby", "URC")
        self.assertEqual((e["group"], e["detail"], e["icon"]), ("More Sports", "Rugby", "sports_rugby"))
        e = self.one("Commanders vs Eagles", "American Football", "NFL")
        self.assertEqual(e["sport"], "american_football")
        e = self.one("WWE Smackdown", "Wrestling", "WWE")
        self.assertEqual((e["group"], e["icon"]), ("Wrestling", "boxing_ring"))
        e = self.one("Real Madrid vs Baskonia", "Basketball", "Spanish Liga ACB")
        self.assertEqual((e["group"], e["detail"]), ("More Sports", "Basketball"))

    def test_ncaa_is_not_always_football(self):
        # QA #6: St Mary's vs Saint Louis was filed as American Football by the site mapping.
        e = self.one("St Mary's Gaels vs Saint Louis Billikens", "American Football", "NCAA", score="SLU 74 - SMC 86")
        self.assertEqual((e["sport"], e["group"]), ("basketball", "College Basketball"))
        e = self.one("Ohio State vs Michigan", "American Football", "NCAAF")
        self.assertEqual(e["group"], "College Football")

    def test_league_codes(self):
        self.assertEqual(self.one("Deportivo Cali vs Millonarios", "Soccer", "COL_1")["league"], "Colombian Primera A")
        self.assertEqual(self.one("Escorpiones Belén vs AD San Carlos", "Soccer", "CRC_1")["league"], "Costa Rican Primera División")
        self.assertEqual(v2.pretty_league("HON_1"), "Honduras Division 1")
        self.assertEqual(self.one("Team A vs Team B", "Serie-a", "Laliga")["category"], "Soccer")

    def test_channels(self):
        fg = ev("Family Guy", "Entertainment", "24/7 Show", status="always_live", start=0, key="ch:family_guy", sources=[
            src("https://a.test/fg", "PPV", "HD | 24/7 channel"),
            src("https://a.test/fg-ad", "ADMIN", "FAMILY GUY | AMERICAN DAD | THE CLEVELAND SHOW"),
            src("https://a.test/simpsons", "ADMIN", "THE SIMPSONS"),
            src("https://a.test/cn", "ADMIN", "CARTOON NETWORK CLASSICS"),
        ])
        simpsons = ev("The Simpsons", "Entertainment", "24/7 Show", status="always_live", start=0, key="ch:simpsons", sources=[src("https://a.test/s1", "PPV", "HD")])
        out = {e["canonical_key"]: e for e in run([fg, simpsons])}
        self.assertEqual([s["url"] for s in out["ch:family_guy"]["sources"]], ["https://a.test/fg", "https://a.test/fg-ad"])
        self.assertIn("https://a.test/simpsons", [s["url"] for s in out["ch:simpsons"]["sources"]])
        cn = out["ch:cartoon_network_classics"]
        self.assertEqual((cn["title"], cn["group"], cn["icon"]), ("Cartoon Network Classics", "24/7", "live_tv"))


class SourceTests(unittest.TestCase):
    def test_slug_names_other_game(self):
        a = ev("Auckland FC vs Melbourne Victory", "Soccer", "A-League", sources=[
            src("https://l.test/live24.html?url=auckland-vs-melbourne-victory-stream-1"),
            src("https://l.test/live24.html?url=central-coast-vs-perth-stream-1"),
        ])
        b = ev("Central Coast Mariners vs Perth Glory", "Soccer", "A-League", sources=[src("https://l.test/live24.html?url=central-coast-vs-perth-stream-2")])
        out = {e["title"]: e for e in run([a, b])}
        self.assertEqual(len(out["Auckland FC vs Melbourne Victory"]["sources"]), 1)
        self.assertEqual(len(out["Central Coast Mariners vs Perth Glory"]["sources"]), 2)

    def test_shared_channel_flagged_and_ranked_last(self):
        tnt = src("https://c.test/tnt1", "BETA", "TNT Sports 1")
        a = ev("Auckland FC vs Melbourne Victory", "Soccer", "A-League", sources=[dict(tnt), src("https://l.test/a")])
        b = ev("Central Coast Mariners vs Perth Glory", "Soccer", "A-League", sources=[dict(tnt), src("https://l.test/b")])
        payload = v2.finalize(run([a, b]), NOW)
        for e in payload["events"]:
            self.assertEqual(e["sources"][-1]["url"], "https://c.test/tnt1")
            self.assertTrue(e["sources"][-1]["shared_channel"])
            self.assertNotIn("shared_channel", e["sources"][0])

    def test_sites_and_kill_switch(self):
        e = ev("Lakers vs Celtics", "Basketball", "NBA", sources=[
            src("https://p.test/1", "PPV"), src("https://s.test/1", "CHARLIE"), src("https://r.test/1", "ROXIESTREAMS"), src("https://n.test/1", "NEWSITE"),
        ])
        policy = v2.load_policy()
        policy["config"]["sites"]["roxiestreams"]["enabled"] = False
        payload = v2.finalize(run([e]), NOW, policy)
        out = payload["events"][0]
        self.assertEqual(out["sites"], ["ppv", "streamcorner", "newsite"])
        self.assertIn("newsite", payload["sites"])
        self.assertEqual(v2.validate(payload), [])


class EspnTests(unittest.TestCase):
    def board(self, state="in", start=NOW - 3600, home=("Saint Mary's Gaels", "SMC", "86"), away=("Saint Louis Billikens", "SLU", "74")):
        def team(name, abbr, score, side):
            return {"homeAway": side, "score": score, "team": {"displayName": name, "shortDisplayName": name.split()[0], "abbreviation": abbr, "logo": f"https://logo.test/{abbr}.png"}}
        return {"events": [{
            "id": "401", "date": datetime.datetime.fromtimestamp(start, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
            "competitions": [{"status": {"type": {"state": state, "shortDetail": "2nd 5:12"}},
                              "competitors": [team(*home, "home"), team(*away, "away")]}],
        }]}

    def fetcher(self, board, league_path="mens-college-basketball"):
        return lambda url: board if league_path in url else {"events": []}

    def test_match_sets_teams_score_and_group(self):
        e = ev("Saint Louis vs St Mary's", "American Football", "NCAA", start=NOW - 5400)
        out = v2.enrich([e], NOW, fetch=self.fetcher(self.board()))
        self.assertEqual(len(out), 1)
        got = out[0]
        self.assertEqual((got["group"], got["espn"]["league"], got["espn"]["id"]), ("College Basketball", "ncaam", "401"))
        self.assertEqual(got["score_summary"], "SLU 74 - SMC 86 • 2nd 5:12")
        self.assertEqual([t["abbr"] for t in got["teams"]], ["SLU", "SMC"])
        self.assertTrue(got["start_checked"])
        self.assertEqual(got["starts_at"], NOW - 3600)  # corrected from the feed's NOW - 5400

    def test_final_is_dropped(self):
        e = ev("Saint Louis vs St Mary's", "Basketball", "NCAA", start=NOW - 3600)
        self.assertEqual(v2.enrich([e], NOW, fetch=self.fetcher(self.board(state="post"))), [])

    def test_two_rows_one_game_merge(self):
        # Title rules can't pair these; both match the same scoreboard game.
        rows = [ev("Saint Louis vs St Mary's", "Basketball", "NCAA"), ev("Billikens vs Gaels", "Basketball", "NCAAB", start=NOW - 3500)]
        out = v2.enrich(rows, NOW, fetch=self.fetcher(self.board()))
        self.assertEqual(len(out), 1)
        self.assertEqual(len(out[0]["sources"]), 2)
        self.assertEqual(out[0]["espn"]["id"], "401")


class SchemaTests(unittest.TestCase):
    def test_payload_validates(self):
        events = run([ev("Lakers vs Celtics", "Basketball", "NBA"), ev("Family Guy", "Entertainment", "24/7 Show", status="always_live", start=0, key="ch:family_guy")])
        payload = v2.finalize(events, NOW)
        self.assertEqual(payload["schema_version"], 2)
        self.assertEqual(v2.validate(payload), [])
        self.assertIn("ncaam", payload["policy"]["scoreboards"])


if __name__ == "__main__":
    unittest.main()
