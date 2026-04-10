#!/usr/bin/env python3
"""Scrape Sequoia guild raid data from Wynncraft API every 6 hours."""

import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
WEEKS_DIR = os.path.join(DATA_DIR, "weeks")
GUILD_URL = "https://api.wynncraft.com/v3/guild/Sequoia"
PLAYER_URL = "https://api.wynncraft.com/v3/player/{name}"
# Delay between player fetches. Wynncraft's limit is ~180/min; 0.5s gives
# ~120/min which leaves headroom for request latency and occasional retries.
PLAYER_FETCH_DELAY = 0.5

# Week boundary: Sunday 23:00 UTC = Monday 01:00 CEST
BOUNDARY_WEEKDAY = 6  # Sunday (Monday=0 in Python)
BOUNDARY_HOUR = 23


def fetch_guild():
    """Fetch Sequoia guild data from Wynncraft API."""
    req = urllib.request.Request(GUILD_URL, headers={"User-Agent": "SEQ-Raids-Tracker"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def extract_players(guild_data):
    """Extract player raid counts from guild API response.

    Returns (players, uuids) where players maps name -> raid counts and
    uuids maps name -> uuid (needed to disambiguate duplicate usernames in
    the player endpoint).
    """
    players = {}
    uuids = {}
    for role in ["owner", "chief", "strategist", "captain", "recruiter", "recruit"]:
        for name, info in guild_data.get("members", {}).get(role, {}).items():
            raids = info.get("guildRaids", {}).get("list", {})
            players[name] = {
                "tcc": raids.get("The Canyon Colossus", 0),
                "nol": raids.get("Orphion's Nexus of Light", 0),
                "notg": raids.get("Nest of the Grootslangs", 0),
                "tna": raids.get("The Nameless Anomaly", 0),
            }
            if info.get("uuid"):
                uuids[name] = info["uuid"]
    return players, uuids


def _fetch_twp_once(identifier):
    req = urllib.request.Request(
        PLAYER_URL.format(name=identifier),
        headers={"User-Agent": "SEQ-Raids-Tracker"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.loads(resp.read())
    raids_list = data.get("globalData", {}).get("raids", {}).get("list", {})
    return int(raids_list.get("unknown", 0))


def fetch_player_twp(identifier):
    """Fetch TWP count for a player by UUID (preferred) or username.

    TWP is not tracked as a guild raid — it only appears in the per-player
    endpoint as raids.list.unknown. Returns int on success, None on failure.
    Uses UUIDs to avoid HTTP 300 on duplicate usernames.
    """
    try:
        return _fetch_twp_once(identifier)
    except urllib.error.HTTPError as e:
        if e.code == 429:
            # Rate limited — back off and retry once
            time.sleep(15)
            try:
                return _fetch_twp_once(identifier)
            except Exception:
                return None
        return None
    except Exception:
        return None


def attach_twp(players, uuids):
    """Fetch and attach TWP counts to each player in-place.

    Sequential with a small delay to stay under the Wynncraft API rate limit.
    Missing fetches leave twp=None so merge_with_previous can fall back.
    """
    names = list(players.keys())
    print(f"Fetching TWP for {len(names)} players...")
    failed = 0
    for i, name in enumerate(names):
        identifier = uuids.get(name, name)
        count = fetch_player_twp(identifier)
        if count is None:
            players[name]["twp"] = None
            failed += 1
        else:
            players[name]["twp"] = count
        time.sleep(PLAYER_FETCH_DELAY)
        if (i + 1) % 50 == 0:
            print(f"  TWP progress: {i + 1}/{len(names)}")
    if failed:
        print(f"  TWP: {failed} fetches failed (will fall back to previous)")


def merge_with_previous(new_players, previous_path):
    """Handle API bugs and missing TWP fetches by falling back to previous data.

    - Guild raid reset bug: if all 4 guild raid counts hit 0 but previously had
      data, revert those 4 fields to the previous values.
    - TWP fallback: if the TWP fetch failed (None), use the previous value if
      available, otherwise leave the twp field missing entirely. Leaving it
      missing is important so a later successful fetch doesn't look like a
      massive one-scrape delta against a poisoned baseline of 0.
    - TWP is monotonically increasing: if current < previous, keep previous
      (treats glitches the same as failures).
    """
    previous = {}
    if os.path.exists(previous_path):
        with open(previous_path) as f:
            previous = json.load(f)
    prev_players = previous.get("players", {})

    for name, data in new_players.items():
        prev = prev_players.get(name)

        # Guild raid reset bug
        guild_total = data["tcc"] + data["nol"] + data["notg"] + data["tna"]
        if guild_total == 0 and prev:
            prev_guild = prev.get("tcc", 0) + prev.get("nol", 0) + prev.get("notg", 0) + prev.get("tna", 0)
            if prev_guild > 0:
                data["tcc"] = prev.get("tcc", 0)
                data["nol"] = prev.get("nol", 0)
                data["notg"] = prev.get("notg", 0)
                data["tna"] = prev.get("tna", 0)

        # TWP fallback
        prev_twp = prev.get("twp") if prev else None
        cur_twp = data.get("twp")
        if cur_twp is None:
            if prev_twp is not None:
                data["twp"] = prev_twp
            else:
                data.pop("twp", None)  # still unknown — leave missing
        elif prev_twp is not None and cur_twp < prev_twp:
            data["twp"] = prev_twp

    return new_players


def get_week_start(dt):
    """Get the most recent Sunday 23:00 UTC before or at dt."""
    # Find the most recent Sunday
    days_since_sunday = (dt.weekday() + 1) % 7  # Sunday=0 offset
    candidate = (dt - timedelta(days=days_since_sunday)).replace(
        hour=BOUNDARY_HOUR, minute=0, second=0, microsecond=0
    )
    # If we haven't reached the boundary yet today (it's Sunday but before 23:00)
    if candidate > dt:
        candidate -= timedelta(days=7)
    return candidate


def get_next_week_boundary(dt):
    """Get next week boundary after dt."""
    start = get_week_start(dt)
    return start + timedelta(days=7)


def get_week_number(dt):
    """Get ISO week string for the Monday following this boundary.
    Sunday 23:00 UTC belongs to the week that starts on that Monday."""
    monday = dt + timedelta(hours=1)  # Sunday 23:00 + 1h = Monday 00:00
    return f"{monday.isocalendar()[0]}-W{monday.isocalendar()[1]:02d}"


def format_week_label(start, end):
    """Format like 'Mar 30 - Apr 6, 2026' (showing the Monday dates).
    start/end are Sunday 23:00 UTC boundaries, +2h gives Monday 01:00 CEST."""
    mon_start = start + timedelta(hours=2)  # Sunday 23:00 UTC → Monday 01:00 CEST
    mon_end = end + timedelta(hours=2)
    s = mon_start.strftime("%b %d")
    e = mon_end.strftime("%b %d, %Y")
    return f"{s} - {e}"


def compute_leaderboards(baseline_players, latest_players, top_n=10):
    """Compute top N deltas per raid type. Only counts players present in baseline."""
    raid_types = ["tcc", "nol", "notg", "tna", "twp"]
    deltas = {rt: [] for rt in raid_types}
    deltas["all"] = []

    for player in latest_players:
        if player not in baseline_players:
            continue  # New player — no baseline, skip (they'll be added to baseline)

        end = latest_players[player]
        start = baseline_players[player]

        total_delta = 0
        for rt in raid_types:
            diff = end.get(rt, 0) - start.get(rt, 0)
            if diff < 0:
                diff = 0  # Data reset, can't determine delta
            if diff > 0:
                deltas[rt].append({"name": player, "delta": diff})
                total_delta += diff

        if total_delta > 0:
            deltas["all"].append({"name": player, "delta": total_delta})

    total_raids = sum(p["delta"] for p in deltas["all"])
    active = len(deltas["all"])

    result = {}
    for key in deltas:
        deltas[key].sort(key=lambda x: -x["delta"])
        result[key.upper()] = deltas[key][:top_n]

    return result, active, total_raids


def sync_baseline_with_latest(baseline, latest_players):
    """Add new players and fix API-bug gaps in baseline.
    - New players get current counts as baseline (delta starts at 0)
    - Players with 0 in baseline but data in latest get synced (prevents fake deltas)
    - Players missing the twp field in baseline get it backfilled from latest
      (so their TWP delta starts at 0 the week the field was added)
    """
    baseline_players = baseline.get("players", {})
    changed = 0
    for name, data in latest_players.items():
        bp = baseline_players.get(name)
        if bp is None:
            baseline_players[name] = dict(data)
            changed += 1
            continue

        bp_guild = bp.get("tcc", 0) + bp.get("nol", 0) + bp.get("notg", 0) + bp.get("tna", 0)
        l_guild = data["tcc"] + data["nol"] + data["notg"] + data["tna"]
        if bp_guild == 0 and l_guild > 0:
            baseline_players[name] = dict(data)
            changed += 1
            continue

        # Backfill twp on existing baseline entries (pre-TWP data)
        if "twp" not in bp and "twp" in data:
            bp["twp"] = data["twp"]
            changed += 1

    if changed > 0:
        baseline["players"] = baseline_players
        print(f"Synced {changed} players in baseline")
    return baseline, changed > 0


def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))


def load_json(path):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def main():
    now = datetime.now(timezone.utc)
    print(f"Scraping at {now.isoformat()}")

    # Fetch guild data
    guild_data = fetch_guild()
    players, uuids = extract_players(guild_data)
    print(f"Fetched {len(players)} members")

    # Fetch per-player TWP counts (not in the guild endpoint)
    attach_twp(players, uuids)

    # Merge with previous to handle API bugs and TWP fetch failures
    latest_path = os.path.join(DATA_DIR, "latest.json")
    players = merge_with_previous(players, latest_path)

    # Save latest
    latest = {"scraped_at": now.isoformat(), "players": players}
    save_json(latest_path, latest)

    # Load meta
    meta_path = os.path.join(DATA_DIR, "meta.json")
    meta = load_json(meta_path) or {
        "current_week": get_week_number(now),
        "week_start": get_week_start(now).isoformat(),
        "available_weeks": [],
        "last_scrape": now.isoformat(),
    }

    # Load baseline
    baseline_path = os.path.join(DATA_DIR, "baseline.json")
    baseline = load_json(baseline_path)

    # Check if we need to roll over the week
    week_boundary = get_next_week_boundary(
        datetime.fromisoformat(meta["week_start"])
    )

    if now >= week_boundary and baseline:
        print(f"Week boundary crossed! Archiving week...")
        week_key = meta["current_week"]
        week_start = datetime.fromisoformat(meta["week_start"])

        # Compute final leaderboards for the completed week
        leaderboards, active, total_raids = compute_leaderboards(
            baseline.get("players", {}), players
        )
        week_data = {
            "week": week_key,
            "start": meta["week_start"],
            "end": week_boundary.isoformat(),
            "label": format_week_label(week_start, week_boundary),
            "leaderboards": leaderboards,
            "total_players": len(players),
            "active_players": active,
            "total_raids": total_raids,
        }

        week_path = os.path.join(WEEKS_DIR, f"{week_key}.json")
        save_json(week_path, week_data)
        print(f"Saved {week_path}")

        # New baseline = current latest
        save_json(baseline_path, latest)

        # Update meta for new week
        new_week_start = week_boundary
        meta["current_week"] = get_week_number(new_week_start)
        meta["week_start"] = new_week_start.isoformat()
        if week_key not in meta["available_weeks"]:
            meta["available_weeks"].append(week_key)

    elif not baseline:
        # First run — seed baseline
        print("No baseline found, seeding...")
        save_json(baseline_path, latest)
        baseline = latest
    else:
        # Normal scrape — sync baseline (new players + API-bug fixes)
        baseline, changed = sync_baseline_with_latest(baseline, players)
        if changed:
            save_json(baseline_path, baseline)

    meta["last_scrape"] = now.isoformat()
    save_json(meta_path, meta)
    print("Done!")


if __name__ == "__main__":
    main()
