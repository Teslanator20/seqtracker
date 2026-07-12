#!/usr/bin/env python3
"""Scrape Sequoia guild raid data from Wynncraft API every 6 hours."""

import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
WEEKS_DIR = os.path.join(DATA_DIR, "weeks")
GUILD_URL = "https://api.wynncraft.com/v3/guild/Sequoia?identifier=username"

# Week boundary: Sunday 23:00 UTC = Monday 01:00 CEST
BOUNDARY_WEEKDAY = 6  # Sunday (Monday=0 in Python)
BOUNDARY_HOUR = 23

RAID_KEYS = {
    "tcc": "The Canyon Colossus",
    "nol": "Orphion's Nexus of Light",
    "notg": "Nest of the Grootslangs",
    "tna": "The Nameless Anomaly",
    "twp": "The Wartorn Palace",
}


def fetch_guild():
    """Fetch Sequoia guild data from Wynncraft API."""
    req = urllib.request.Request(GUILD_URL, headers={"User-Agent": "SEQ-Raids-Tracker"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def notify_level_up(prev_level, new_level, xp_percent):
    """Post a Discord webhook message when the guild levels up.
    No-op if DISCORD_WEBHOOK_URL is not set."""
    url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not url:
        return
    payload = {
        "content": (
            f":tada: **Sequoia** just leveled up: "
            f"**{prev_level} → {new_level}** (xp {xp_percent}%)"
        )
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            resp.read()
        print(f"Webhook sent: level {prev_level} → {new_level}")
    except urllib.error.HTTPError as e:
        print(f"Webhook HTTP {e.code}: {e.read()[:200]}")
    except Exception as e:
        print(f"Webhook error: {e}")


def extract_players(guild_data):
    """Extract per-member raid counts from guild API response, keyed by UUID.

    Reads globalData.guildRaids.list. Members with privacy restrictions
    (mainAccess: true) return empty globalData; their counts will all be 0
    and merge_with_previous restores them from the previous snapshot.

    Keying by UUID (stable) instead of name (mutable) is required because
    Wynncraft usernames can change — a name-keyed dict would treat a renamed
    player as a brand-new player and silently drop their in-progress week.
    """
    players = {}
    for role in ["owner", "chief", "strategist", "captain", "recruiter", "recruit"]:
        for name, info in guild_data.get("members", {}).get(role, {}).items():
            uuid = info.get("uuid")
            if not uuid:
                continue
            raids = info.get("globalData", {}).get("guildRaids", {}).get("list", {})
            players[uuid] = {
                "name": name,
                **{short: raids.get(full, 0) for short, full in RAID_KEYS.items()},
            }
    return players


def is_legacy_players(players):
    """Pre-UUID-migration snapshots keyed players by name and had no "name" field."""
    return any("name" not in data for data in players.values())


def migrate_legacy_players(players, name_to_uuid):
    """One-time migration: old snapshots keyed by name -> keyed by UUID.

    Members who left the guild before migration can't be resolved to a UUID
    (they're no longer in the current roster fetch) and are dropped — they
    were already unreachable dead weight in the name-keyed baseline/latest.
    """
    migrated = {}
    dropped = []
    for name, data in players.items():
        uuid = name_to_uuid.get(name)
        if not uuid:
            dropped.append(name)
            continue
        migrated[uuid] = {"name": name, **data}
    if dropped:
        print(f"Migration: dropped {len(dropped)} departed/unresolved members: {dropped}")
    return migrated


def merge_with_previous(new_players, previous_path, name_to_uuid):
    """If a player's raid totals are all 0 but they previously had data, revert
    to the previous values. Covers two cases: the Wynncraft guild-raid reset
    bug, and members with privacy restrictions whose data is now hidden.
    """
    previous = {}
    if os.path.exists(previous_path):
        with open(previous_path) as f:
            previous = json.load(f)
    prev_players = previous.get("players", {})
    if is_legacy_players(prev_players):
        prev_players = migrate_legacy_players(prev_players, name_to_uuid)

    for uuid, data in new_players.items():
        prev = prev_players.get(uuid)
        if not prev:
            continue
        if sum(data[k] for k in RAID_KEYS) == 0 and sum(prev.get(k, 0) for k in RAID_KEYS) > 0:
            for k in RAID_KEYS:
                data[k] = prev.get(k, 0)

    return new_players


def track_name_changes(players, meta_players_path, now_iso):
    """Persist a UUID -> name-history log so renames stay auditable.

    Separate file from baseline/latest.json (those are rewritten wholesale
    every scrape and must stay lean); this one only grows on actual renames.
    """
    log = load_json(meta_players_path) or {}
    changed = False
    for uuid, data in players.items():
        name = data["name"]
        entry = log.get(uuid)
        if entry is None:
            log[uuid] = {"name": name, "history": []}
            changed = True
        elif entry["name"] != name:
            entry["history"].append({"name": entry["name"], "until": now_iso})
            entry["name"] = name
            changed = True
    if changed:
        save_json(meta_players_path, log)
    return log


def build_name_index(rename_log):
    """name (any name ever seen, current or old, lowercased) -> uuid.

    Lets a caller resolve a player by whatever name they know them by, even
    if that name was later renamed away from. Names are only unique at a
    point in time on Wynncraft; if two different people historically held
    the same name, the most recently seen owner wins here.
    """
    index = {}
    for uuid, entry in rename_log.items():
        for old in entry["history"]:
            index[old["name"].lower()] = uuid
        index[entry["name"].lower()] = uuid
    return index


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
    """Compute top N deltas per raid type. Only counts players present in baseline.

    Players and baseline are keyed by UUID; display name always comes from the
    *latest* snapshot so a mid-week rename shows the player's current name
    while still crediting all their raids (same UUID = same delta series).
    """
    raid_types = list(RAID_KEYS.keys())
    deltas = {rt: [] for rt in raid_types}
    deltas["all"] = []

    for uuid, end in latest_players.items():
        start = baseline_players.get(uuid)
        if start is None:
            continue  # New player — no baseline, skip (they'll be added to baseline)

        display_name = end["name"]
        total_delta = 0
        for rt in raid_types:
            diff = end.get(rt, 0) - start.get(rt, 0)
            if diff < 0:
                diff = 0  # Data reset, can't determine delta
            if diff > 0:
                deltas[rt].append({"name": display_name, "delta": diff})
                total_delta += diff

        if total_delta > 0:
            deltas["all"].append({"name": display_name, "delta": total_delta})

    total_raids = sum(p["delta"] for p in deltas["all"])
    active = len(deltas["all"])

    result = {}
    for key in deltas:
        deltas[key].sort(key=lambda x: -x["delta"])
        result[key.upper()] = deltas[key][:top_n]

    return result, active, total_raids


def sync_baseline_with_latest(baseline, latest_players, name_to_uuid):
    """Add new players and fix gaps in baseline. Keyed by UUID (see extract_players).
    - New players get current counts as baseline (delta starts at 0)
    - Players with 0 in baseline but data in latest get synced (prevents fake deltas)
    - Baseline's stored display name is refreshed on rename (cosmetic only —
      compute_leaderboards always displays the *latest* name regardless)
    - One-time TWP correction: if baseline.twp > latest.twp, schema flipped from
      old mixed (raids.list.unknown) to new guild-only — resync baseline.twp so
      this week's delta starts at 0 instead of being permanently clamped negative.
    """
    baseline_players = baseline.get("players", {})
    changed = 0
    if is_legacy_players(baseline_players):
        baseline_players = migrate_legacy_players(baseline_players, name_to_uuid)
        changed += 1
    for uuid, data in latest_players.items():
        bp = baseline_players.get(uuid)
        if bp is None:
            baseline_players[uuid] = dict(data)
            changed += 1
            continue

        if bp.get("name") != data["name"]:
            bp["name"] = data["name"]
            changed += 1

        bp_total = sum(bp.get(k, 0) for k in RAID_KEYS)
        l_total = sum(data[k] for k in RAID_KEYS)
        if bp_total == 0 and l_total > 0:
            baseline_players[uuid] = dict(data)
            changed += 1
            continue

        if bp.get("twp", 0) > data.get("twp", 0):
            bp["twp"] = data["twp"]
            changed += 1

    baseline["players"] = baseline_players
    if changed > 0:
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

    # Fetch guild data (now includes per-member globalData.guildRaids when
    # ?identifier=username is set — replaces the old per-player TWP loop)
    guild_data = fetch_guild()
    players = extract_players(guild_data)
    print(f"Fetched {len(players)} members")

    # Current roster's name -> uuid map, used to migrate any legacy
    # name-keyed baseline/latest.json still on disk from before UUID keying.
    name_to_uuid = {data["name"]: uuid for uuid, data in players.items()}

    # Track renames (uuid stays, display name changes) in a standalone log,
    # plus a name->uuid index (old and current names) for lookups by name.
    rename_log = track_name_changes(players, os.path.join(DATA_DIR, "players.json"), now.isoformat())
    save_json(os.path.join(DATA_DIR, "name_index.json"), build_name_index(rename_log))

    guild_level = guild_data.get("level")
    guild_xp = guild_data.get("xpPercent")

    # Merge with previous to fill in restricted/missing members
    latest_path = os.path.join(DATA_DIR, "latest.json")
    players = merge_with_previous(players, latest_path, name_to_uuid)

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

    # Guild level-up notification
    prev_level = meta.get("guild_level")
    if guild_level is not None:
        if prev_level is not None and guild_level > prev_level:
            notify_level_up(prev_level, guild_level, guild_xp)
        meta["guild_level"] = guild_level
        meta["guild_xp_percent"] = guild_xp

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
        baseline_players = baseline.get("players", {})
        if is_legacy_players(baseline_players):
            baseline_players = migrate_legacy_players(baseline_players, name_to_uuid)
        leaderboards, active, total_raids = compute_leaderboards(
            baseline_players, players
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
        baseline, changed = sync_baseline_with_latest(baseline, players, name_to_uuid)
        if changed:
            save_json(baseline_path, baseline)

    meta["last_scrape"] = now.isoformat()
    save_json(meta_path, meta)
    print("Done!")


if __name__ == "__main__":
    main()
