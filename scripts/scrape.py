#!/usr/bin/env python3
"""Scrape Sequoia guild raid data from Wynncraft API every 6 hours."""

import json
import os
import urllib.request
from datetime import datetime, timezone, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
WEEKS_DIR = os.path.join(DATA_DIR, "weeks")
GUILD_URL = "https://api.wynncraft.com/v3/guild/Sequoia"

RAID_KEYS = {
    "The Canyon Colossus": "tcc",
    "Orphion's Nexus of Light": "nol",
    "Nest of the Grootslangs": "notg",
    "The Nameless Anomaly": "tna",
}


def fetch_guild():
    """Fetch Sequoia guild data from Wynncraft API."""
    req = urllib.request.Request(GUILD_URL, headers={"User-Agent": "SEQ-Raids-Tracker"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def extract_players(guild_data):
    """Extract player raid counts from guild API response."""
    players = {}
    for role in ["owner", "chief", "strategist", "captain", "recruiter", "recruit"]:
        for name, info in guild_data.get("members", {}).get(role, {}).items():
            raids = info.get("guildRaids", {}).get("list", {})
            players[name] = {
                "tcc": raids.get("The Canyon Colossus", 0),
                "nol": raids.get("Orphion's Nexus of Light", 0),
                "notg": raids.get("Nest of the Grootslangs", 0),
                "tna": raids.get("The Nameless Anomaly", 0),
            }
    return players


def merge_with_previous(new_players, previous_path):
    """Handle API bug: keep previous data for players whose counts reset to 0."""
    if not os.path.exists(previous_path):
        return new_players

    with open(previous_path) as f:
        previous = json.load(f)

    prev_players = previous.get("players", {})
    for name, data in new_players.items():
        total = data["tcc"] + data["nol"] + data["notg"] + data["tna"]
        if total == 0 and name in prev_players:
            prev_total = sum(prev_players[name].values())
            if prev_total > 0:
                new_players[name] = prev_players[name]

    return new_players


def get_week_number(dt):
    """Get ISO week string like '2026-W14'."""
    return f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"


def get_week_start(dt):
    """Get the most recent Sunday 1 AM UTC before or at dt."""
    # Find the most recent Sunday
    days_since_sunday = dt.weekday() + 1  # Monday=0, Sunday=6 → +1
    if days_since_sunday == 7:
        days_since_sunday = 0
    sunday = dt.replace(hour=1, minute=0, second=0, microsecond=0) - timedelta(days=days_since_sunday)
    # If we're on Sunday but before 1 AM, go back to previous Sunday
    if dt.weekday() == 6 and dt.hour < 1:
        sunday -= timedelta(days=7)
    return sunday


def get_next_week_boundary(dt):
    """Get next Sunday 1 AM UTC after dt."""
    start = get_week_start(dt)
    return start + timedelta(days=7)


def format_week_label(start, end):
    """Format like 'Mar 30 - Apr 6, 2026'."""
    s = start.strftime("%b %d")
    e = end.strftime("%b %d, %Y")
    return f"{s} - {e}"


def compute_leaderboards(baseline_players, latest_players, top_n=10):
    """Compute top N deltas per raid type."""
    raid_types = ["tcc", "nol", "notg", "tna"]
    deltas = {rt: [] for rt in raid_types}
    deltas["all"] = []

    all_players = set(latest_players.keys())

    for player in all_players:
        end = latest_players.get(player, {"tcc": 0, "nol": 0, "notg": 0, "tna": 0})
        start = baseline_players.get(player, {"tcc": 0, "nol": 0, "notg": 0, "tna": 0})

        total_delta = 0
        for rt in raid_types:
            diff = end[rt] - start[rt]
            if diff > 0:
                deltas[rt].append({"name": player, "delta": diff})
                total_delta += diff
            elif diff < 0:
                # Player data reset, use end value as delta (new guild member)
                if end[rt] > 0:
                    deltas[rt].append({"name": player, "delta": end[rt]})
                    total_delta += end[rt]

        if total_delta > 0:
            deltas["all"].append({"name": player, "delta": total_delta})

    # Sort and trim
    result = {}
    for key in deltas:
        deltas[key].sort(key=lambda x: -x["delta"])
        result[key.upper()] = deltas[key][:top_n]

    active = len([p for p in deltas["all"] if p["delta"] > 0])
    return result, active


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
    players = extract_players(guild_data)
    print(f"Fetched {len(players)} members")

    # Merge with previous to handle API bugs
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
        leaderboards, active = compute_leaderboards(
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

    meta["last_scrape"] = now.isoformat()
    save_json(meta_path, meta)
    print("Done!")


if __name__ == "__main__":
    main()
