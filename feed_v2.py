"""Feed contract v2: the pipeline decides, the apps render.

distill_events.py calls two passes:

* ``enrich(events, now_sec)`` before the source-health probe: cleans titles,
  pretty-prints league codes, merges duplicate rows of one fixture, classifies
  each event (sport, group, detail, icon), moves wrong-show 24/7 sources and
  wrong-game slug sources, flags channel streams shared by two games at once,
  and matches ESPN scoreboards (scores, team logos, start-time check).
* ``finalize(events, now_sec)`` after it: maps every source to its site, applies
  the site kill switches, ranks shared-channel sources last and wraps the events
  with ``schema_version``, ``sites``, ``policy`` and ``config`` from
  feed_policy.json. ``validate(payload)`` checks the result against
  schema/events.v2.schema.json before it is published.

The rules here replace the per-app copies in Slang (live-sports-groups.ts,
live-sports-icons.ts, live-sports-scores.util.ts) and Stream Sites TV
(EventDistiller / EventDatasetStore score matching).
"""

from __future__ import annotations

import concurrent.futures
import copy
import datetime
import html
import json
import os
import re
import sys
import unicodedata
import urllib.parse
import urllib.request

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
POLICY_PATH = os.path.join(BASE_DIR, "feed_policy.json")
SCHEMA_PATH = os.path.join(BASE_DIR, "schema", "events.v2.schema.json")
SCHEMA_VERSION = 2

MORE_SPORTS = "More Sports"
CHANNEL_GROUP = "24/7"


def log(msg: str) -> None:
    print(f"[v2] {msg}", flush=True)


def load_policy(path: str = POLICY_PATH) -> dict:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    data.pop("_comment", None)
    return data


# --------------------------------------------------------------------------- text

def fold(text: str) -> str:
    """Lowercase, strip accents, unify quotes and dashes."""
    text = unicodedata.normalize("NFKD", html.unescape(text or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("’", "'").replace("‘", "'").replace("–", "-").replace("—", "-")
    return text.lower().replace("&", " and ")


# Between two sides: " vs ", " v ", " vs. ", " @ ", " at ", " x ", or a lone " - ".
_VS_RE = re.compile(r"\s+(?:vs\.?|v\.?|versus|@|at|x)\s+", re.I)
_DASH_RE = re.compile(r"\s+[-–—]\s+")
_SEP_RE = re.compile(r"\s+(?:vs\.?|v\.?|versus|@|at|x|-|–|—)\s+", re.I)
_DISPLAY_VS_RE = re.compile(r"\s+(?:vs\.?|v\.?|versus)\s+", re.I)
_RANK_RE = re.compile(r"(?:(?<=\s)|^)(?:#\d{1,2}|\(\d{1,2}\))\s+")
# Bare AP/CFP top-25 rank at the start of a matchup side: "15 Iowa vs Illinois", "Chattanooga vs 3 Alabama".
_BARE_RANK_RE = re.compile(r"^(?:[1-9]|1\d|2[0-5])\s+(?=[A-Za-z])")
# "NBA Basketball: A vs B", "UFC 323: A vs B", "LIVE - A vs B"
_PREFIX_RE = re.compile(r"^\s*(?:live|watch|stream)\s*[:\-|]\s*", re.I)

TEAM_STOP = {
    "fc", "cf", "sc", "afc", "ac", "cd", "fk", "sk", "bk", "if", "cp", "ca", "club", "de", "del", "la",
    "the", "and", "of", "vs", "v", "at", "live", "stream", "hd", "game", "match",
    "women", "womens", "woman", "w", "men", "mens",
}
TOKEN_ALIASES = {
    "st": "saint", "ste": "sainte", "utd": "united", "intl": "international",
    "man": "manchester", "wolves": "wolverhampton", "spurs": "tottenham",
    "czechia": "czech", "turkiye": "turkey", "usa": "usa", "us": "usa",
}
# Words that brand a broadcast rather than name the fixture.
BRAND_TOKENS = {
    "nfl", "nba", "nhl", "mlb", "wnba", "mls", "ncaa", "ncaaf", "ncaab", "cfb", "ufc", "wwe", "aew",
    "f1", "formula", "1", "football", "basketball", "hockey", "baseball", "soccer", "college",
    "live", "full", "hd", "replay", "official", "broadcast", "coverage", "show", "race", "gp",
}

_WOMEN_RE = re.compile(r"\bwomen'?s?\b|\(w\)|\bw\b|\bwnba\b|\bncaaw\b|\bnwsl\b|\bwsl\b|\bliga f\b|\bfrauen|\bfemenin|\bfeminin|\bwbb\b|\bwbbl\b|\baflw\b")


def is_womens(*texts: str) -> bool:
    return any(_WOMEN_RE.search(fold(t)) for t in texts if t)


def tokens(text: str) -> list[str]:
    cleaned = re.sub(r"[^a-z0-9 ]", " ", fold(text).replace("'", ""))
    out: list[str] = []
    for part in cleaned.split():
        part = TOKEN_ALIASES.get(part, part)
        if part in TEAM_STOP or not part:
            continue
        if part not in out:
            out.append(part)
    return out


def clean_title(title: str) -> str:
    """Display title: one separator style, no rank markers or trailing league tags."""
    t = html.unescape(title or "").strip()
    t = _PREFIX_RE.sub("", t)
    if "|" in t:
        # "UFC Fight Night | A vs B | Prelims" -> the part that names the fixture
        sided = [p.strip() for p in t.split("|") if _SEP_RE.search(p.strip())]
        if sided:
            t = sided[0]
    t = _RANK_RE.sub("", t)
    if ":" in t:
        head, tail = t.split(":", 1)
        if _SEP_RE.search(tail) and not _SEP_RE.search(head):
            # keep numbered cards ("UFC 323: A vs B"), drop sport labels ("NBA Basketball: A vs B")
            if not re.search(r"\d", head):
                t = tail.strip()
    if len(_DISPLAY_VS_RE.findall(t)) == 1:
        t = _DISPLAY_VS_RE.sub(" vs ", t)
    elif not _VS_RE.search(t) and len(_DASH_RE.findall(t)) == 1:
        t = _DASH_RE.sub(" vs ", t)  # "Juarez - Atlas"; "F1 - Las Vegas GP - Race" stays
    vs_matches = list(_VS_RE.finditer(t))
    if len(vs_matches) == 1:
        m = vs_matches[0]
        left = _BARE_RANK_RE.sub("", t[: m.start()].strip())
        right = _BARE_RANK_RE.sub("", t[m.end():].strip())
        t = f"{left}{m.group(0)}{right}"
    return re.sub(r"\s{2,}", " ", t).strip()


def split_sides(title: str) -> list[list[str]]:
    """Token lists for the two sides of "A vs B", or [] for a one-sided title."""
    t = clean_title(title)
    found = list(_VS_RE.finditer(t))
    if len(found) == 1:
        left, right = t[: found[0].start()], t[found[0].end():]
        left = re.split(r":|\s+[-–—|]\s+", left)[-1]  # "UFC 323 - A vs B"
        right = re.split(r"\s+[-–—|]\s+|\s+\(", right)[0]  # "A vs B - Week 12"
    elif not found and len(_DASH_RE.findall(t)) == 1:
        left, right = _DASH_RE.split(t)
    else:
        return []
    a, b = tokens(left), tokens(right)
    if not a or not b:
        return []
    return [a, b]


def _side_match(a: list[str], b: list[str]) -> bool:
    sa, sb = set(a), set(b)
    if not sa or not sb:
        return False
    if sa <= sb or sb <= sa:
        return True
    return len(sa & sb) >= 2


def sides_match(a: list[list[str]], b: list[list[str]]) -> bool:
    if len(a) != 2 or len(b) != 2:
        return False
    return (_side_match(a[0], b[0]) and _side_match(a[1], b[1])) or (
        _side_match(a[0], b[1]) and _side_match(a[1], b[0])
    )


# --------------------------------------------------------------------------- sport

SPORT_NAMES = {
    "american_football": "American Football", "aussie_rules": "Australian Football", "rugby": "Rugby",
    "basketball": "Basketball", "ice_hockey": "Ice Hockey", "baseball": "Baseball", "tennis": "Tennis",
    "table_tennis": "Table Tennis", "badminton": "Badminton", "cricket": "Cricket", "wrestling": "Wrestling",
    "mma": "MMA", "boxing": "Boxing", "kickboxing": "Kickboxing", "motorsport": "Motorsports", "golf": "Golf",
    "skiing": "Winter Sports", "sailing": "Sailing", "cycling": "Cycling", "volleyball": "Volleyball",
    "handball": "Handball", "esports": "Esports", "darts": "Darts", "snooker": "Snooker",
    "equestrian": "Equestrian", "soccer": "Soccer", "entertainment": "Entertainment", "news": "News",
    "other": "Sports",
}

SPORT_ICONS = {
    "american_football": "sports_football", "aussie_rules": "sports_rugby", "rugby": "sports_rugby",
    "basketball": "sports_basketball", "ice_hockey": "sports_hockey", "baseball": "sports_baseball",
    "tennis": "sports_tennis", "cricket": "sports_cricket", "wrestling": "boxing_ring",
    "mma": "sports_mma", "boxing": "sports_mma", "kickboxing": "sports_mma",
    "motorsport": "sports_motorsports", "golf": "sports_golf", "skiing": "downhill_skiing",
    "sailing": "sailing", "cycling": "pedal_bike", "volleyball": "sports_volleyball",
    "handball": "sports_handball", "esports": "sports_esports", "soccer": "sports_soccer",
    "entertainment": "live_tv", "news": "live_tv",
}

# First match wins. Short codes match whole words only ("Commanders" is not MMA). The feed's
# "Football" is soccer, so American football is matched first by its own names, and a bare
# "NCAA" is left for the scoreboard (or the score) to decide.
SPORT_RULES: list[tuple[str, re.Pattern]] = [(k, re.compile(p)) for k, p in [
    ("equestrian", r"equestrian|show jumping|dressage"),
    ("table_tennis", r"table tennis|ping pong|\bwtt\b"),
    ("american_football", r"american football|college football|ncaa ?(division 1 )?football|\b(nfl|ncaaf|cfl|cfb|xfl|ufl|fbs|fcs)\b|redzone|red zone"),
    ("aussie_rules", r"australian football|aussie rules|\b(afl|aflw)\b"),
    ("rugby", r"rugby|\bnrl\b|six nations|super rugby"),
    ("basketball", r"basketball|baloncesto|basketbol|\b(nba|wnba|ncaab|ncaaw|euroleague|eurocup|nbl|wbb|acb)\b|g league|ncaa wom[ae]n"),
    ("ice_hockey", r"hockey|\b(nhl|ahl|khl|ohl|whl|shl|iihf)\b"),
    ("baseball", r"baseball|\b(mlb|npb|kbo|abl)\b"),
    ("badminton", r"badminton|\bbwf\b"),
    ("tennis", r"tennis|billie jean king|davis cup|\b(atp|wta|bjk)\b"),
    ("cricket", r"cricket|big bash|\b(ipl|bbl|wbbl|psl|odi|t20|test match)\b"),
    ("wrestling", r"wrestling|smackdown|\braw\b|\b(wwe|aew|tna|njpw|nxt|roh)\b"),
    ("mma", r"\b(ufc|mma|bellator|pfl|lfa|one fc|cage warriors|ksw)\b"),
    ("kickboxing", r"kickboxing|glory \d|\bmuay thai\b"),
    ("boxing", r"boxing|knockout|\bfighting\b|fight night|\bbkfc\b"),
    ("motorsport", r"formula|motorsport|moto ?sport|motogp|moto2|moto3|nascar|indycar|grand prix|super gt|superbike|supercars|rally|\b(f1|f2|f3|f4|wec|imsa|wrc|dtm)\b"),
    ("golf", r"golf|\b(pga|lpga|liv|dp world)\b"),
    ("skiing", r"\bski|slalom|snowboard|biathlon|bobsleigh|luge|skeleton|speed skating|figure skating"),
    ("sailing", r"sailing|sailgp|regatta|america'?s cup"),
    ("cycling", r"cycling|cyclo-?cross|tour de france|giro d|vuelta|\buci\b"),
    ("volleyball", r"volleyball|\bvnl\b"),
    ("handball", r"handball|\bhand\b"),
    ("esports", r"esports|e-sports|league of legends|counter-strike|\bcs2\b|valorant|dota"),
    ("darts", r"darts|\bpdc\b"),
    ("snooker", r"snooker|\bpool\b|billiards"),
    ("soccer", r"soccer|futbol|football|uefa|fifa|premier league|la ?liga|serie[ -]a|bundesliga|ligue [12]|eredivisie|primeira|a-league|liga mx|liga f\b|champions league|europa league|conference league|copa |coupe |cup of nations|nations league|ekstraklasa|allsvenskan|super lig|brasileir|eliteserien|superliga|primera|\b(mls|epl|nwsl|efl|wsl|usl|afc|caf|concacaf|conmebol)\b|english league|scottish|\bfa cup\b|carabao|premiership|campeonato|\bliga\b|\bcopa\b|\bcup\b|feminin|\bnational\b|[a-z]{3}_\d"),
]]

GROUP_FOR_SPORT = {
    "baseball": "MLB", "ice_hockey": "NHL", "soccer": "Soccer", "mma": "UFC / MMA", "boxing": "Boxing",
    "wrestling": "Wrestling", "motorsport": "Motorsports", "golf": "Golf", "tennis": "Tennis",
}

_NCAA_RE = re.compile(r"\bncaa\b|\bcollege\b")
_COLLEGE_FB_RE = re.compile(r"college football|ncaa ?(division 1 )?football|\b(ncaaf|cfb|fbs|fcs)\b|\bbowl\b")
_COLLEGE_BB_RE = re.compile(r"college basketball|ncaa ?(wom[ae]n|basketball)|\b(ncaab|ncaaw|wbb|ncaam)\b")


def _text(ev: dict) -> str:
    return fold(f"{ev.get('category', '')} {ev.get('league', '')} {ev.get('title', '')}")


def looks_like_basketball_score(score: str) -> bool:
    nums = [int(n) for n in re.findall(r"\d+", score or "")[:2]]
    return len(nums) == 2 and all(40 <= n <= 160 for n in nums)


def detect_sport(ev: dict, now_sec: int) -> str:
    league = fold(ev.get("league", ""))
    if ev.get("status") == "always_live":
        if league in ("24/7 show", "24/7 channel") or fold(ev.get("category", "")) == "entertainment":
            return "entertainment"
        if league == "news network" or fold(ev.get("category", "")) == "news":
            return "news"
    text = _text(ev)
    for sport, pattern in SPORT_RULES:
        if pattern.search(text):
            # Old site mapping filed bare "NCAA" as American Football; let the score decide.
            if sport == "american_football" and _NCAA_RE.search(text) and not _COLLEGE_FB_RE.search(text):
                return _ncaa_guess(ev, now_sec)
            return sport
    if _NCAA_RE.search(text):
        return _ncaa_guess(ev, now_sec)
    return "entertainment" if ev.get("status") == "always_live" else "other"


def _ncaa_guess(ev: dict, now_sec: int) -> str:
    ev["_sport_guess"] = True  # dedupe treats it as any sport; a scoreboard match settles it
    if looks_like_basketball_score(ev.get("score_summary", "")):
        return "basketball"
    # US Eastern-ish day, so a Friday-night game is not a Saturday one.
    when = datetime.datetime.fromtimestamp((ev.get("starts_at") or now_sec) - 5 * 3600, datetime.timezone.utc)
    # College football is a Saturday sport (and bowls); basketball runs Nov-Apr every day.
    if when.month in (8, 9, 10) or (when.month in (11, 12) and when.weekday() == 5):
        return "american_football"
    if when.month in (11, 12, 1, 2, 3, 4):
        return "basketball"
    return "american_football"


def detect_group(ev: dict, sport: str) -> tuple[str, str]:
    """(group, detail) for an event of the given sport."""
    if ev.get("status") == "always_live":
        return CHANNEL_GROUP, ev.get("league", "") or SPORT_NAMES.get(sport, "")
    text = _text(ev)
    league = ev.get("league", "")
    if sport == "american_football":
        college = _COLLEGE_FB_RE.search(text) or _NCAA_RE.search(text)
        return ("College Football" if college else "NFL"), league
    if sport == "basketball":
        if re.search(r"\bwnba\b", text):
            return "WNBA", league
        if _COLLEGE_BB_RE.search(text) or _NCAA_RE.search(text):
            return "College Basketball", league
        if re.search(r"\bnba\b", text):
            return "NBA", league
        return MORE_SPORTS, SPORT_NAMES[sport]
    group = GROUP_FOR_SPORT.get(sport)
    if group:
        return group, league
    return MORE_SPORTS, SPORT_NAMES.get(sport, "Sports")


def icon_for(ev: dict, sport: str) -> str:
    return SPORT_ICONS.get(sport) or ("live_tv" if ev.get("status") == "always_live" else "sports")


# --------------------------------------------------------------------------- leagues

COUNTRIES = {
    "ARG": "Argentina", "AUS": "Australia", "AUT": "Austria", "BEL": "Belgium", "BOL": "Bolivia",
    "BRA": "Brazil", "BUL": "Bulgaria", "CAN": "Canada", "CHI": "Chile", "CHN": "China", "COL": "Colombia",
    "CRC": "Costa Rica", "CRO": "Croatia", "CYP": "Cyprus", "CZE": "Czech", "DEN": "Denmark",
    "ECU": "Ecuador", "EGY": "Egypt", "ENG": "English", "ESP": "Spanish", "FIN": "Finnish", "FRA": "French",
    "GER": "German", "GRE": "Greek", "GUA": "Guatemala", "HON": "Honduras", "HUN": "Hungarian",
    "IRL": "Irish", "ISR": "Israeli", "ITA": "Italian", "JAM": "Jamaica", "JPN": "Japanese", "KOR": "Korean",
    "KSA": "Saudi", "MAR": "Moroccan", "MEX": "Mexican", "NED": "Dutch", "NOR": "Norwegian",
    "PAN": "Panama", "PAR": "Paraguay", "PER": "Peru", "POL": "Polish", "POR": "Portuguese", "QAT": "Qatar",
    "ROU": "Romanian", "RSA": "South African", "RUS": "Russian", "SCO": "Scottish", "SLV": "El Salvador",
    "SRB": "Serbian", "SUI": "Swiss", "SVK": "Slovak", "SVN": "Slovenian", "SWE": "Swedish",
    "TUR": "Turkish", "UAE": "UAE", "UKR": "Ukrainian", "URU": "Uruguay", "USA": "USA", "VEN": "Venezuela",
    "WAL": "Welsh",
}
LEAGUE_NAMES = {
    "ARG_1": "Argentine Primera División", "BRA_1": "Brasileirão Série A", "BRA_2": "Brasileirão Série B",
    "CHI_1": "Chilean Primera División", "COL_1": "Colombian Primera A", "COL_2": "Colombian Primera B",
    "CRC_1": "Costa Rican Primera División", "ECU_1": "Ecuador LigaPro", "MEX_1": "Liga MX",
    "MEX_2": "Liga de Expansión MX", "PER_1": "Peruvian Liga 1", "URU_1": "Uruguayan Primera División",
    "PAR_1": "Paraguayan Primera División", "BOL_1": "Bolivian Primera División", "USA_1": "MLS",
    "ENG_1": "Premier League", "ENG_2": "English League Championship", "ESP_1": "Spanish La Liga",
    "ESP_2": "Spanish La Liga 2", "ITA_1": "Italian Serie A", "GER_1": "Bundesliga", "FRA_1": "French Ligue 1",
}
LEAGUE_FIXUPS = {"laliga": "LaLiga", "liga mx": "Liga MX", "serie-a": "Serie A", "mls": "MLS", "ncaaf": "NCAAF"}
_LEAGUE_CODE_RE = re.compile(r"^([A-Z]{3})_(\d)$")


def pretty_league(league: str) -> str:
    raw = (league or "").strip()
    m = _LEAGUE_CODE_RE.match(raw)
    if m:
        if raw in LEAGUE_NAMES:
            return LEAGUE_NAMES[raw]
        country = COUNTRIES.get(m.group(1), m.group(1).title())
        return f"{country} Division {m.group(2)}"
    return LEAGUE_FIXUPS.get(raw.lower(), raw)


GENERIC_LEAGUES = {"", "sports", "football", "soccer", "basketball", "national", "other", "live", "stream"}


# --------------------------------------------------------------------------- dedupe

def _match_info(ev: dict) -> dict:
    title = ev.get("title", "")
    return {
        "sides": split_sides(title),
        "tokens": set(t for t in tokens(clean_title(title)) if t not in BRAND_TOKENS),
        "women": is_womens(title, ev.get("league", ""), ev.get("category", "")),
        "sport": "other" if ev.get("_sport_guess") else (ev.get("sport") or "other"),
        "all_tokens": set(tokens(clean_title(title))),
    }


def _same_event(a: dict, b: dict, ia: dict, ib: dict) -> bool:
    if a.get("status") == "always_live" or b.get("status") == "always_live":
        return False
    if ia["women"] != ib["women"]:
        return False
    if ia["sport"] != ib["sport"] and "other" not in (ia["sport"], ib["sport"]):
        return False
    sa, sb = int(a.get("starts_at") or 0), int(b.get("starts_at") or 0)
    two_sided = bool(ia["sides"]) and bool(ib["sides"])
    window = 4 * 3600 if two_sided else 2 * 3600
    if sa and sb:
        if abs(sa - sb) > window:
            return False
    elif a.get("status") != b.get("status"):
        return False
    if two_sided:
        return sides_match(ia["sides"], ib["sides"])
    if ia["sides"] or ib["sides"]:
        sided, flat = (ia, ib) if ia["sides"] else (ib, ia)
        return all(set(side) & flat["tokens"] for side in sided["sides"]) and len(flat["tokens"]) >= 2
    # "WWE Smackdown" = "WWE Friday Night SmackDown": every word of the shorter title, at
    # least one of them not a brand word.
    small, big = sorted((ia["all_tokens"], ib["all_tokens"]), key=len)
    return len(small) >= 2 and small <= big and bool(small - BRAND_TOKENS)


_STATUS_RANK = {"always_live": 3, "live": 2, "future": 1}


def merge_group(rows: list[dict]) -> dict:
    """Fold duplicate rows of one fixture into the row with the most sources."""
    rows = sorted(rows, key=lambda r: (-len(r.get("sources") or []), -int(bool(r.get("starts_at"))), r.get("canonical_key", "")))
    out = copy.deepcopy(rows[0])
    aliases = list(out.get("aliases") or [])
    seen_urls = {s.get("url") for s in out.get("sources") or []}
    for row in rows[1:]:
        aliases.append(row.get("canonical_key", ""))
        aliases.extend(row.get("aliases") or [])
        for src in row.get("sources") or []:
            if src.get("url") and src["url"] not in seen_urls:
                seen_urls.add(src["url"])
                out["sources"].append(copy.deepcopy(src))
        for origin in row.get("origins") or []:
            if origin not in out["origins"]:
                out["origins"].append(origin)
        if _STATUS_RANK.get(row.get("status"), 0) > _STATUS_RANK.get(out.get("status"), 0):
            out["status"] = row["status"]
        for key in ("poster_url", "score_summary", "starts_at_iso"):
            if not out.get(key) and row.get(key):
                out[key] = row[key]
        if not out.get("starts_at") and row.get("starts_at"):
            out["starts_at"], out["ends_at"] = row["starts_at"], row.get("ends_at", 0)
        if out.get("league", "").strip().lower() in GENERIC_LEAGUES and row.get("league", "").strip().lower() not in GENERIC_LEAGUES:
            out["league"] = row["league"]
        out["updated_at"] = max(int(out.get("updated_at") or 0), int(row.get("updated_at") or 0))
    # A two-sided title reads better than a show name ("Prochazka vs Stirling" over "UFC 323").
    if not split_sides(out["title"]):
        better = next((r["title"] for r in rows if split_sides(r.get("title", ""))), None)
        if better:
            out["title"] = clean_title(better)
    out["aliases"] = sorted({a for a in aliases if a and a != out["canonical_key"]})
    return out


def dedupe_events(events: list[dict]) -> list[dict]:
    infos = [_match_info(e) for e in events]
    parent = list(range(len(events)))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    by_sport: dict[str, list[int]] = {}
    for i, info in enumerate(infos):
        by_sport.setdefault(info["sport"], []).append(i)
    others = by_sport.get("other", [])
    for sport, idxs in by_sport.items():
        pool = idxs + ([] if sport == "other" else others)
        for i in idxs:
            for j in pool:
                # same-sport pairs once; "other" rows against every sport
                if j == i or (j < i and infos[j]["sport"] == sport) or find(i) == find(j):
                    continue
                if _same_event(events[i], events[j], infos[i], infos[j]):
                    parent[find(j)] = find(i)
    groups: dict[int, list[dict]] = {}
    for i, ev in enumerate(events):
        groups.setdefault(find(i), []).append(ev)
    out = [merge_group(rows) if len(rows) > 1 else rows[0] for rows in groups.values()]
    merged = len(events) - len(out)
    if merged:
        log(f"dedupe: merged {merged} duplicate rows ({len(events)} -> {len(out)} events)")
    return out


# --------------------------------------------------------------------------- source checks

_GENERIC_LABEL_RE = re.compile(
    r"^\W*(hd|sd|fhd|uhd|4k|1080p?|720p?|stream|backup|primary|source|link|mirror|server|channel|option|alt|feed|main|english|spanish|24/7)\b|24/7 channel|^\W*\d+\W*$",
    re.I,
)


def split_wrong_show_sources(events: list[dict]) -> list[dict]:
    """A 24/7 show's source labeled as another show becomes (or joins) that show's channel."""
    by_key = {e["canonical_key"]: e for e in events}
    moved = 0
    for ev in list(events):
        if ev.get("status") != "always_live" or ev.get("sport") not in ("entertainment", "news"):
            continue
        title_tokens = set(tokens(ev["title"]))
        keep = []
        for src in ev.get("sources") or []:
            label = (src.get("label") or "").strip()
            label_tokens = set(tokens(label)) - {"24", "7", "channel", "tv"}
            if (
                not label
                or _GENERIC_LABEL_RE.search(label)
                or not any(len(t) >= 3 and t.isalpha() for t in label_tokens)
                or label_tokens & title_tokens
            ):
                keep.append(src)
                continue
            name = " ".join(
                w if w.isupper() and w.isalpha() and len(w) <= 3 and w not in ("THE", "AND") else w.capitalize()
                for w in label.split()
            )
            key = "ch:" + "_".join([t for t in tokens(label) if t in label_tokens][:5])
            target = by_key.get(key) or next(
                (e for e in events if e.get("status") == "always_live" and set(tokens(e["title"])) == label_tokens), None
            )
            if target is None:
                target = {
                    **copy.deepcopy({k: v for k, v in ev.items() if k not in ("sources", "aliases", "origins")}),
                    "canonical_key": key, "title": name, "sources": [], "origins": [],
                    "league": "24/7 Channel", "category": "Entertainment",
                }
                events.append(target)
                by_key[key] = target
            if src.get("url") not in {s.get("url") for s in target["sources"]}:
                target["sources"].append(src)
                if src.get("origin") and src["origin"] not in target["origins"]:
                    target["origins"].append(src["origin"])
            moved += 1
        ev["sources"] = keep
    if moved:
        log(f"24/7: moved {moved} sources labeled as another show to that show")
    return [e for e in events if e.get("sources")]


_SLUG_RE = re.compile(r"([a-z0-9]+(?:-[a-z0-9]+)*?)-vs?-([a-z0-9]+(?:-[a-z0-9]+)*?)(?:-stream-?\d*|-live|-hd|-\d+)?(?:$|[/?&#.])")


def slug_sides(url: str) -> list[list[str]]:
    """Team slugs in a mirror URL ("…?url=adelaide-vs-brisbane-stream-2")."""
    parts = urllib.parse.urlsplit((url or "").lower())
    for chunk in [urllib.parse.unquote(parts.query), parts.path]:
        for piece in re.split(r"[=/&]", chunk):
            m = _SLUG_RE.search(piece + "/")
            if m:
                a = tokens(m.group(1).replace("-", " "))
                b = tokens(m.group(2).replace("-", " "))
                if a and b:
                    return [a, b]
    return []


def _slug_fits(slug: list[list[str]], sides: list[list[str]]) -> bool:
    # Slugs abbreviate ("central-coast-vs-perth"), so one shared word per side is enough.
    def fit(x, y):
        return bool(set(x) & set(y))
    return (fit(slug[0], sides[0]) and fit(slug[1], sides[1])) or (fit(slug[0], sides[1]) and fit(slug[1], sides[0]))


def _overlaps(a: dict, b: dict, pad: int = 1800) -> bool:
    sa, sb = int(a.get("starts_at") or 0), int(b.get("starts_at") or 0)
    if not sa or not sb:
        return a.get("status") == b.get("status") == "live"
    ea = int(a.get("ends_at") or sa + 3 * 3600)
    eb = int(b.get("ends_at") or sb + 3 * 3600)
    return sa < eb + pad and sb < ea + pad


def move_wrong_game_sources(events: list[dict]) -> int:
    """A mirror whose URL names another fixture at the same time moves to that fixture."""
    sided = [(e, split_sides(e["title"])) for e in events if e.get("status") != "always_live"]
    moved = 0
    for ev, sides in sided:
        if not sides:
            continue
        keep = []
        for src in ev.get("sources") or []:
            slug = slug_sides(src.get("url", ""))
            if not slug or _slug_fits(slug, sides):
                keep.append(src)
                continue
            home = next(
                (o for o, os_ in sided if o is not ev and os_ and _slug_fits(slug, os_) and _overlaps(o, ev, 6 * 3600)),
                None,
            )
            if home is None:
                keep.append(src)  # abbreviated slug ("ccm-vs-per"): can't tell, leave it
                continue
            if src.get("url") not in {s.get("url") for s in home["sources"]}:
                home["sources"].append(src)
            moved += 1
        ev["sources"] = keep
    if moved:
        log(f"wrong-game mirrors: moved {moved} to the fixture their URL names")
    return moved


def flag_shared_channels(events: list[dict], ident=None) -> int:
    """Flag one channel stream attached to two overlapping games: it can only show one."""
    ident = ident or (lambda u: (u or "").split("#", 1)[0].rstrip("/").lower())
    owners: dict[str, list[dict]] = {}
    for ev in events:
        if ev.get("status") == "always_live":
            continue
        for src in ev.get("sources") or []:
            owners.setdefault(ident(src.get("url", "")), []).append(ev)
    flagged = 0
    for key, evs in owners.items():
        if len(evs) < 2:
            continue
        clash = {id(a) for a in evs for b in evs if a is not b and _overlaps(a, b)}
        for ev in evs:
            if id(ev) not in clash:
                continue
            for src in ev["sources"]:
                if ident(src.get("url", "")) == key and not src.get("shared_channel"):
                    src["shared_channel"] = True
                    flagged += 1
    if flagged:
        log(f"shared channels: flagged {flagged} sources attached to overlapping games")
    return flagged


# --------------------------------------------------------------------------- ESPN

ESPN = "https://site.api.espn.com/apis/site/v2/sports"
# league key -> (path, sport, group, women)
SCOREBOARDS: dict[str, tuple[str, str, str, bool]] = {
    "nfl": ("football/nfl/scoreboard", "american_football", "NFL", False),
    "ncaaf": ("football/college-football/scoreboard?groups=80&limit=300", "american_football", "College Football", False),
    "nba": ("basketball/nba/scoreboard", "basketball", "NBA", False),
    "wnba": ("basketball/wnba/scoreboard", "basketball", "WNBA", True),
    "ncaam": ("basketball/mens-college-basketball/scoreboard?groups=50&limit=400", "basketball", "College Basketball", False),
    "ncaaw": ("basketball/womens-college-basketball/scoreboard?groups=50&limit=400", "basketball", "College Basketball", True),
    "nhl": ("hockey/nhl/scoreboard", "ice_hockey", "NHL", False),
    "mlb": ("baseball/mlb/scoreboard", "baseball", "MLB", False),
    "mls": ("soccer/usa.1/scoreboard", "soccer", "Soccer", False),
    "nwsl": ("soccer/usa.nwsl/scoreboard", "soccer", "Soccer", True),
    "epl": ("soccer/eng.1/scoreboard", "soccer", "Soccer", False),
    "laliga": ("soccer/esp.1/scoreboard", "soccer", "Soccer", False),
    "seriea": ("soccer/ita.1/scoreboard", "soccer", "Soccer", False),
    "bundesliga": ("soccer/ger.1/scoreboard", "soccer", "Soccer", False),
    "ligue1": ("soccer/fra.1/scoreboard", "soccer", "Soccer", False),
    "ligamx": ("soccer/mex.1/scoreboard", "soccer", "Soccer", False),
    "ucl": ("soccer/uefa.champions/scoreboard", "soccer", "Soccer", False),
    "uel": ("soccer/uefa.europa/scoreboard", "soccer", "Soccer", False),
}
LEAGUE_LABELS = {
    "nfl": "NFL", "ncaaf": "NCAAF", "nba": "NBA", "wnba": "WNBA", "ncaam": "NCAA Men's Basketball",
    "ncaaw": "NCAA Women's Basketball", "nhl": "NHL", "mlb": "MLB", "mls": "MLS", "nwsl": "NWSL",
    "epl": "Premier League", "laliga": "Spanish La Liga", "seriea": "Italian Serie A", "bundesliga": "Bundesliga",
    "ligue1": "French Ligue 1", "ligamx": "Liga MX", "ucl": "UEFA Champions League", "uel": "UEFA Europa League",
}


def scoreboard_url(league: str, overrides: dict[str, str] | None = None) -> str:
    if overrides and isinstance(overrides.get(league), str) and overrides[league].startswith("https://"):
        return overrides[league]
    return f"{ESPN}/{SCOREBOARDS[league][0]}"


def resolved_scoreboards(policy: dict | None = None) -> dict[str, str]:
    pol = (policy or {}).get("policy") or {}
    disabled = {str(k).strip().lower() for k in (pol.get("scoreboards_disabled") or []) if str(k).strip()}
    overrides = pol.get("scoreboards") if isinstance(pol.get("scoreboards"), dict) else {}
    out: dict[str, str] = {}
    for league in SCOREBOARDS:
        if league.lower() in disabled:
            continue
        out[league] = scoreboard_url(league, overrides)
    for league, url in overrides.items():
        lk = str(league).strip()
        if not lk or lk.lower() in disabled or lk in out:
            continue
        if isinstance(url, str) and url.startswith("https://"):
            out[lk] = url
    return out


def poster_hosts(url: str) -> set[str]:
    hosts: set[str] = set()
    raw = str(url or "").strip()
    if not raw:
        return hosts
    try:
        parsed = urllib.parse.urlparse(raw)
        if parsed.hostname:
            hosts.add(parsed.hostname.lower())
        qs = urllib.parse.parse_qs(parsed.query)
        for key in ("url", "src", "img", "image"):
            for val in qs.get(key) or []:
                inner = urllib.parse.urlparse(val if "://" in val else f"https://{val}")
                if inner.hostname:
                    hosts.add(inner.hostname.lower())
    except Exception:
        pass
    return hosts


def apply_image_policy(ev: dict, images_cfg: dict | None) -> bool:
    """Clear poster_url per policy.images rules. Returns True if cleared."""
    if not isinstance(images_cfg, dict) or not ev.get("poster_url"):
        return False
    if images_cfg.get("posters_enabled") is False:
        ev["poster_url"] = ""
        return True
    if images_cfg.get("prefer_team_logos") is True:
        teams = ev.get("teams") or []
        if len(teams) >= 2 and all(isinstance(t, dict) and t.get("logo") for t in teams):
            ev["poster_url"] = ""
            return True
    blocked = {str(h).strip().lower() for h in (images_cfg.get("blocked_poster_hosts") or []) if str(h).strip()}
    if blocked and (poster_hosts(ev["poster_url"]) & blocked):
        ev["poster_url"] = ""
        return True
    return False


def _get_json(url: str, timeout: int = 12):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _parse_iso(text: str) -> int:
    try:
        return int(datetime.datetime.fromisoformat((text or "").replace("Z", "+00:00")).timestamp())
    except ValueError:
        return 0


def parse_scoreboard(league: str, raw: dict) -> list[dict]:
    out = []
    for ev in (raw or {}).get("events") or []:
        comp = (ev.get("competitions") or [{}])[0]
        competitors = comp.get("competitors") or []
        if len(competitors) < 2:
            continue
        away = next((c for c in competitors if c.get("homeAway") == "away"), competitors[0])
        home = next((c for c in competitors if c.get("homeAway") == "home"), competitors[1])
        st = ((comp.get("status") or ev.get("status") or {}).get("type") or {})
        teams = []
        for c, is_home in ((away, False), (home, True)):
            t = c.get("team") or {}
            names = " ".join(str(t.get(k) or "") for k in ("displayName", "shortDisplayName", "location", "name", "nickname"))
            teams.append({
                "name": t.get("displayName") or t.get("shortDisplayName") or t.get("abbreviation") or "",
                "short": t.get("shortDisplayName") or "",
                "abbr": t.get("abbreviation") or "",
                "logo": t.get("logo") or ((t.get("logos") or [{}])[0].get("href") or ""),
                "score": str(c.get("score") or ""),
                "home": is_home,
                "_tokens": set(tokens(names)) | ({t["abbreviation"].lower()} if t.get("abbreviation") else set()),
                "_nick": set(tokens(t.get("name") or "")),
            })
        out.append({
            "league": league,
            "id": str(ev.get("id") or ""),
            "start": _parse_iso(ev.get("date") or comp.get("date") or ""),
            "state": (st.get("state") or "pre").lower(),
            "detail": st.get("shortDetail") or st.get("detail") or "",
            "teams": teams,
        })
    return out


def fetch_scoreboards(now_sec: int, leagues: list[str] | None = None, fetch=_get_json, policy: dict | None = None) -> list[dict]:
    # ESPN days are US days and it rejects date ranges: ask for yesterday, today, tomorrow (ET).
    et_today = datetime.datetime.fromtimestamp(now_sec - 5 * 3600, datetime.timezone.utc)
    days = [f"{et_today + datetime.timedelta(days=d):%Y%m%d}" for d in (-1, 0, 1)]
    boards_map = resolved_scoreboards(policy)
    leagues = leagues or [lg for lg in boards_map if lg in SCOREBOARDS]

    def one(job: tuple[str, str]) -> list[dict]:
        league, day = job
        url = boards_map.get(league) or scoreboard_url(league)
        url += ("&" if "?" in url else "?") + f"dates={day}"
        try:
            return parse_scoreboard(league, fetch(url))
        except Exception as exc:  # noqa: BLE001 - one league down must not stop the feed
            log(f"espn {league} {day}: {exc}")
            return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        rows = pool.map(one, [(lg, day) for lg in leagues for day in days])
        unique = {(g["league"], g["id"]): g for r in rows for g in r}
    games = list(unique.values())
    log(f"espn: {len(games)} games from {len(leagues)} scoreboards")
    return games


def _team_fit(side: list[str], team: dict) -> float:
    s, t = set(side), team["_tokens"]
    if not s or not t:
        return 0.0
    common = s & t
    if not common:
        return 0.0
    if s <= t:
        return 1.0 + len(common) / len(t)
    # "North Carolina State Wolfpack" vs ESPN "NC State Wolfpack": nickname plus another word.
    nick = team.get("_nick") or set()
    if nick and nick <= s and len(common) >= 2:
        return 1.0
    return len(common) / len(s | t) if len(common) >= 2 or (len(s) == 1 and common) else 0.0


def _game_fit(sides: list[list[str]], game: dict) -> float:
    a, h = game["teams"]
    straight = min(_team_fit(sides[0], a), _team_fit(sides[1], h))
    swapped = min(_team_fit(sides[0], h), _team_fit(sides[1], a))
    return max(straight, swapped)


def _compatible(ev: dict, league: str) -> bool:
    _, sport, group, women = SCOREBOARDS[league]
    if is_womens(ev.get("title", ""), ev.get("league", ""), ev.get("category", "")) != women:
        return False
    ev_sport = ev.get("sport", "other")
    text = _text(ev)
    if league in ("ncaam", "ncaaw", "ncaaf"):
        if _COLLEGE_FB_RE.search(text):
            return league == "ncaaf"
        if _COLLEGE_BB_RE.search(text):
            return league != "ncaaf"
        # A bare NCAA row may be filed under the wrong sport; the scoreboard settles it.
        return ev_sport in (sport, "other") or bool(_NCAA_RE.search(text))
    if ev_sport not in (sport, "other"):
        return False
    if league in ("nfl", "nba", "nhl", "mlb", "wnba"):
        return not _NCAA_RE.search(text)
    return True


ESPN_FIELDS = (
    "sport", "group", "category", "icon", "league", "detail", "teams", "espn", "status", "time_label",
    "score_summary", "starts_at", "ends_at", "starts_at_iso", "start_checked",
)


def apply_scoreboards(events: list[dict], games: list[dict], now_sec: int, time_label=None) -> list[dict]:
    """Attach ESPN ids, teams and scores; fix start times; drop finished games."""
    if not games:
        return events
    claimed: dict[str, dict] = {}
    finished = matched = corrected = 0
    kept = []
    for ev in events:
        sides = split_sides(ev.get("title", "")) if ev.get("status") != "always_live" else []
        best, best_rank, best_fit = None, (0.0, 0), 0.0
        if sides:
            start = int(ev.get("starts_at") or 0)
            ref = start or now_sec
            for g in games:
                if g["start"] and abs(g["start"] - ref) > (4 if start else 12) * 3600:
                    continue
                if not _compatible(ev, g["league"]):
                    continue
                fit = _game_fit(sides, g)
                rank = (round(fit, 3), -abs((g["start"] or ref) - ref))
                if fit >= 0.5 and rank > best_rank:
                    best, best_rank, best_fit = g, rank, fit
        if best is None:
            kept.append(ev)
            continue
        if best["state"] == "post":
            finished += 1
            continue
        key = f"{best['league']}:{best['id']}"
        if key in claimed:
            # Two rows matched one game: they are duplicates the title rules missed.
            first = claimed[key]
            espn_fields = {k: first[k] for k in ESPN_FIELDS if k in first}
            first.update(merge_group([first, ev]))
            first.update(espn_fields)
            continue
        matched += 1
        _, sport, group, _w = SCOREBOARDS[best["league"]]
        a, h = best["teams"]
        ev["sport"], ev["group"] = sport, group
        ev["category"] = SPORT_NAMES[sport]
        ev["icon"] = SPORT_ICONS[sport]
        if fold(ev.get("league", "")).strip() in GENERIC_LEAGUES or best["league"].startswith("ncaa"):
            ev["league"] = LEAGUE_LABELS[best["league"]]
        ev["detail"] = ev.get("league", "")
        ev["teams"] = [{k: v for k, v in t.items() if not k.startswith("_") and v != ""} for t in (a, h)]
        ev["espn"] = {"league": best["league"], "id": best["id"], "state": best["state"]}
        if best["state"] == "in":
            ev["status"] = "live"
            ev["time_label"] = "LIVE"
            base = f"{a['abbr']} {a['score'] or '0'} - {h['abbr']} {h['score'] or '0'}"
            ev["score_summary"] = f"{base} • {best['detail']}" if best["detail"] else base
        if best["start"]:
            start = int(ev.get("starts_at") or 0)
            if abs(best["start"] - start) > 600:
                length = max(0, int(ev.get("ends_at") or 0) - start) if start else 0
                ev["starts_at"] = best["start"]
                ev["ends_at"] = best["start"] + (length or 3 * 3600)
                ev["starts_at_iso"] = datetime.datetime.fromtimestamp(best["start"], datetime.timezone.utc).isoformat()
                if time_label and ev["status"] != "live":
                    ev["time_label"] = time_label(ev["status"], best["start"], now_sec)
                corrected += 1
            ev["start_checked"] = True
        claimed[key] = ev
        kept.append(ev)
    log(f"espn: matched {matched}, dropped {finished} finished, corrected {corrected} start times")
    return kept


# --------------------------------------------------------------------------- passes

def classify(ev: dict, now_sec: int) -> None:
    ev.pop("_sport_guess", None)
    sport = detect_sport(ev, now_sec)
    group, detail = detect_group(ev, sport)
    ev["sport"] = sport
    ev["group"] = group
    ev["detail"] = detail
    ev["icon"] = icon_for(ev, sport)
    if sport not in ("other",) and ev.get("status") != "always_live":
        ev["category"] = SPORT_NAMES[sport]


def enrich(events: list[dict], now_sec: int, *, scoreboards: bool = True, time_label=None, ident=None, fetch=_get_json, policy: dict | None = None) -> list[dict]:
    for ev in events:
        if ev.get("status") != "always_live":
            ev["title"] = clean_title(ev.get("title", "")) or ev.get("title", "")
        ev["league"] = pretty_league(ev.get("league", ""))
        classify(ev, now_sec)
    events = split_wrong_show_sources(events)
    events = dedupe_events(events)
    move_wrong_game_sources(events)
    if scoreboards:
        pol = policy if policy is not None else (load_policy() if os.path.exists(POLICY_PATH) else None)
        events = apply_scoreboards(events, fetch_scoreboards(now_sec, fetch=fetch, policy=pol), now_sec, time_label)
    flag_shared_channels(events, ident)
    return [e for e in events if e.get("sources")]


SOURCE_KEYS = {"origin", "site", "label", "url", "kind", "referer", "height", "bitrate", "fps", "verified", "quality", "shared_channel"}
EVENT_KEYS = {
    "canonical_key", "aliases", "title", "category", "league", "status", "starts_at", "ends_at", "starts_at_iso",
    "time_label", "score_summary", "poster_url", "origins", "sources", "updated_at", "sport", "group", "detail",
    "icon", "sites", "teams", "espn", "start_checked",
}


def site_index(policy: dict) -> dict[str, str]:
    return {p.upper(): site for site, meta in policy["sites"].items() for p in meta.get("providers") or []}


def finalize(events: list[dict], now_sec: int, policy: dict | None = None) -> dict:
    policy = copy.deepcopy(policy or load_policy())
    sites = policy["sites"]
    origin_site = site_index(policy)
    enabled = {k: bool(v.get("enabled", True)) for k, v in policy["config"]["sites"].items()}
    out_events = []
    killed = 0
    images_cfg = policy["policy"].get("images") if isinstance(policy["policy"].get("images"), dict) else None
    posters_cleared = 0
    for ev in events:
        if "sport" not in ev:
            classify(ev, now_sec)
        sources = []
        for src in ev.get("sources") or []:
            origin = str(src.get("origin") or "").upper()
            site = origin_site.get(origin)
            if site is None:
                site = re.sub(r"[^a-z0-9]+", "_", origin.lower()).strip("_") or "unknown"
                sites.setdefault(site, {"name": origin.title() or "Unknown", "providers": [origin]})
                origin_site[origin] = site
            if not enabled.get(site, True):
                killed += 1
                continue
            clean = {k: v for k, v in src.items() if k in SOURCE_KEYS and v is not None}
            clean["site"] = site
            if not clean.get("shared_channel"):
                clean.pop("shared_channel", None)
            sources.append(clean)
        # Shared channel streams last: they may be showing the other game.
        sources.sort(key=lambda s: bool(s.get("shared_channel")))
        if not sources:
            continue
        ev = {k: v for k, v in ev.items() if k in EVENT_KEYS}
        ev["sources"] = sources
        ev["sites"] = list(dict.fromkeys(s["site"] for s in sources))
        origins = list(ev.get("origins") or [])
        known = {o.upper() for o in origins}
        origins += [o for o in dict.fromkeys(s["origin"] for s in sources) if o.upper() not in known]
        ev["origins"] = origins
        for key in ("starts_at_iso", "time_label", "score_summary", "poster_url", "league", "category", "detail"):
            if ev.get(key) is None:
                ev[key] = ""
        if apply_image_policy(ev, images_cfg):
            posters_cleared += 1
        for key in ("starts_at", "ends_at", "updated_at"):
            ev[key] = max(0, int(ev.get(key) or 0))
        if not ev.get("aliases"):
            ev.pop("aliases", None)
        if not ev.get("detail"):
            ev.pop("detail", None)
        out_events.append(ev)
    if killed:
        log(f"config: dropped {killed} sources from disabled sites")
    if posters_cleared:
        log(f"images: cleared {posters_cleared} posters per policy.images")
    policy["policy"]["scoreboards"] = resolved_scoreboards(policy)
    return {
        "schema_version": SCHEMA_VERSION,
        "synced_at": now_sec,
        "synced_at_iso": datetime.datetime.fromtimestamp(now_sec, datetime.timezone.utc).isoformat(),
        "total": len(out_events),
        "events": out_events,
        "sites": sites,
        "policy": policy["policy"],
        "config": policy["config"],
    }


def validate(payload: dict, schema_path: str = SCHEMA_PATH) -> list[str]:
    """Schema errors (empty = valid). Needs the jsonschema package; without it, checks the basics."""
    with open(schema_path, encoding="utf-8") as f:
        schema = json.load(f)
    try:
        import jsonschema
    except ImportError:
        errors = [f"missing top-level {k}" for k in schema["required"] if k not in payload]
        need = schema["$defs"]["event"]["required"]
        for i, ev in enumerate(payload.get("events") or []):
            errors += [f"events[{i}] missing {k}" for k in need if k not in ev]
        if not errors:
            print("[v2] jsonschema not installed; checked required fields only", file=sys.stderr)
        return errors
    validator = jsonschema.Draft202012Validator(schema)
    return [
        f"{'/'.join(str(p) for p in err.absolute_path) or '(root)'}: {err.message}"
        for err in sorted(validator.iter_errors(payload), key=lambda e: list(e.absolute_path))
    ][:50]


def main(argv: list[str]) -> int:
    """python3 feed_v2.py validate [events.json]"""
    if len(argv) >= 2 and argv[1] == "validate":
        path = argv[2] if len(argv) > 2 else os.path.join(BASE_DIR, "events.json")
        if path.startswith(("http://", "https://")):
            payload = _get_json(path, timeout=30)
        else:
            with open(path, encoding="utf-8") as f:
                payload = json.load(f)
        errors = validate(payload)
        for err in errors:
            print(err)
        print(f"{path}: {'OK' if not errors else f'{len(errors)} errors'} ({len(payload.get('events') or [])} events)")
        return 1 if errors else 0
    print(main.__doc__)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
