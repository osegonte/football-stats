import os
import json
import pandas as pd
import requests
from dotenv import load_dotenv
from datetime import datetime
import time
import sys

# ─── CONFIGURATION ─────────────────────────────────────────────────────────
load_dotenv()
API_KEY = os.getenv("FOOTBALL_API_KEY")
if not API_KEY:
    print("ERROR: No API key found. Please set FOOTBALL_API_KEY in .env file")
    sys.exit(1)

API_BASE = "https://api.football-data.org/v4"  # Using v4 of the API

# Determine project directory (where this script resides)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "past7_matches_with_league_date_sorted.csv")
MAPPING_PATH = os.path.join(BASE_DIR, "id_mappings.json")

# Define which statistics to extract
STATS_FIELDS = [
    "Goals for",
    "Goals against", 
    "Total shots",
    "Shots on target",
    "Possession (%)"
]

# Rate limiting
REQUEST_DELAY = 6  # seconds between requests (API limit is 10 calls/minute)
# ─────────────────────────────────────────────────────────────────────────────

def generate_skeleton_mappings():
    # Create a skeleton id_mappings.json with all leagues and teams from the CSV
    df = pd.read_csv(CSV_PATH)
    league_keys = df['League'].fillna('').map(lambda x: x.split(',')[0].strip()).unique()
    team_keys = df['Team'].dropna().unique()
    skeleton = {
        'leagues': {league: None for league in sorted(league_keys) if league},
        'teams':   {team: None for team in sorted(team_keys)}
    }
    with open(MAPPING_PATH, 'w', encoding='utf-8') as f:
        json.dump(skeleton, f, indent=2, ensure_ascii=False)
    print(f"Generated skeleton id_mappings.json with {len(skeleton['leagues'])} leagues and {len(skeleton['teams'])} teams. Please fill in IDs.")

# Load or initialize mapping file (must include 'leagues' & optional 'teams')
if not os.path.exists(MAPPING_PATH):
    generate_skeleton_mappings()
    exit(0)

with open(MAPPING_PATH, "r", encoding="utf-8") as f:
    id_map = json.load(f)

league_map = id_map.get("leagues", {})
team_map   = id_map.get("teams", {})

# Persist helper
def save_mappings():
    id_map["leagues"] = league_map
    id_map["teams"]   = team_map
    with open(MAPPING_PATH, "w", encoding="utf-8") as f:
        json.dump(id_map, f, indent=2, ensure_ascii=False)


def fetch_competition_teams(comp_code: str):
    """Retrieve all teams in a competition and cache their IDs."""
    if not comp_code:
        return
        
    print(f"Fetching teams for competition code: {comp_code}")
    url = f"{API_BASE}/competitions/{comp_code}/teams"
    
    try:
        resp = requests.get(url, headers={"X-Auth-Token": API_KEY})
        resp.raise_for_status()
        
        teams_data = resp.json().get("teams", [])
        count = 0
        
        for t in teams_data:
            team_name = t.get("name")
            team_id = t.get("id")
            if team_name and team_id:
                team_map[team_name] = team_id
                count += 1
        
        print(f"Found {count} teams for competition {comp_code}")
        time.sleep(REQUEST_DELAY)  # Respect rate limits
        
    except requests.exceptions.HTTPError as e:
        print(f"⚠️ API error for competition {comp_code}: {e}")
    except Exception as e:
        print(f"⚠️ Unexpected error fetching teams for {comp_code}: {e}")


def fetch_team_by_search(team_name: str) -> int | None:
    """
    The API doesn't support direct name-based team searches with the ?name= parameter.
    This function will return None since we can't search for teams directly.
    
    In a production environment, we would instead:
    1. Fetch all teams from major competitions once
    2. Build a local database/cache of team names and IDs
    3. Use fuzzy matching to find the closest team name match
    
    For now, we'll just return None and handle the missing team gracefully.
    """
    # The previous implementation was receiving 400 errors
    return None


def lookup_team_id(team_name: str) -> int | None:
    """Get a team's ID, if known or retrievable."""
    # First check our mapping
    team_id = team_map.get(team_name)
    if team_id:
        return team_id
        
    # Try to find it by API search
    return fetch_team_by_search(team_name)


def fetch_last_matches(team_id: int, n: int = 7) -> list:
    """Fetch the last N matches for a team."""
    if not team_id:
        return []
        
    try:
        url = f"{API_BASE}/teams/{team_id}/matches"
        params = {
            "status": "FINISHED", 
            "limit": n
        }
        
        resp = requests.get(
            url, 
            headers={"X-Auth-Token": API_KEY},
            params=params
        )
        resp.raise_for_status()
        
        data = resp.json().get("matches", [])
        
        # Sort by date, most recent first
        data.sort(key=lambda m: m.get("utcDate", ""), reverse=True)
        
        time.sleep(REQUEST_DELAY)  # Respect rate limits
        return data[:n]
        
    except requests.exceptions.HTTPError as e:
        print(f"⚠️ API error fetching matches for team {team_id}: {e}")
        return []
    except Exception as e:
        print(f"⚠️ Error fetching matches for team ID {team_id}: {e}")
        return []


def extract_stats(match: dict, team_name: str) -> dict:
    if not match:
        return {field: pd.NA for field in STATS_FIELDS}
        
    try:
        is_home = (match.get("homeTeam", {}).get("name") == team_name)
        match_id = match.get("id")
        
        if not match_id:
            return {field: pd.NA for field in STATS_FIELDS}
            
        stats_url = f"{API_BASE}/matches/{match_id}"
        resp = requests.get(stats_url, headers={"X-Auth-Token": API_KEY})
        resp.raise_for_status()
        
        match_data = resp.json()
        score = match_data.get("score", {}).get("fullTime", {})
        home_goals = score.get("home", 0) or 0
        away_goals = score.get("away", 0) or 0
        
        # Extract available stats (v4 API may have different structure)
        stats = {}
        stats["Goals for"] = home_goals if is_home else away_goals
        stats["Goals against"] = away_goals if is_home else home_goals
        
        # For other stats, try to extract from the match data if available
        stats_data = match_data.get("homeTeam" if is_home else "awayTeam", {}).get("statistics", {})
        
        stats["Total shots"] = stats_data.get("shots", pd.NA)
        stats["Shots on target"] = stats_data.get("shotsOnGoal", pd.NA)
        stats["Possession (%)"] = stats_data.get("possession", pd.NA)
        
        time.sleep(REQUEST_DELAY)  # Respect rate limits
        return stats
        
    except Exception as e:
        print(f"⚠️ Error extracting stats for match: {e}")
        return {field: pd.NA for field in STATS_FIELDS}


def update_csv():
    # Read CSV
    df = pd.read_csv(CSV_PATH)

    # Preload team IDs by league
    leagues = df['League'].fillna('').map(lambda x: x.split(',')[0].strip()).unique()
    for league in leagues:
        code = league_map.get(league)
        if code:
            fetch_competition_teams(code)
        else:
            print(f"⚠️ No competition code for league '{league}', will try to search for teams individually.")
    save_mappings()

    # Process each row
    out_rows = []
    processed = 0
    total = len(df)
    
    for _, row in df.iterrows():
        processed += 1
        if processed % 10 == 0:
            print(f"Processing {processed}/{total} teams...")
            # Save progress periodically
            save_mappings()
        
        team = row.get("Team")
        stats = {}
        tid = lookup_team_id(team)
        
        if not tid:
            print(f"⚠️ Team ID not found for '{team}', filling with NA.")
            for i in range(1, 8):
                for field in STATS_FIELDS:
                    stats[f"Match {i} {field}"] = pd.NA
        else:
            matches = fetch_last_matches(tid)
            for i in range(1, 8):
                if i <= len(matches):
                    rs = extract_stats(matches[i-1], team)
                else:
                    rs = {field: pd.NA for field in STATS_FIELDS}
                for field, val in rs.items():
                    stats[f"Match {i} {field}"] = val

        out_rows.append({**row.to_dict(), **stats})

    updated = pd.DataFrame(out_rows)
    if len(updated) != len(df):
        print("⚠️ Row count mismatch; CSV NOT overwritten.")
        print(f"Original: {len(df)}, Updated: {len(updated)}")
        # Save to a different file instead
        updated.to_csv(CSV_PATH + ".new", index=False)
        return

    # Final save of mappings
    save_mappings()
    
    # Backup original file
    backup_path = CSV_PATH + f".bak.{datetime.now():%Y%m%d_%H%M%S}"
    os.rename(CSV_PATH, backup_path)
    
    # Save updated file
    updated.to_csv(CSV_PATH, index=False)
    print(f"[{datetime.now():%Y-%m-%d %H:%M}] CSV updated for {len(out_rows)} teams.")
    print(f"Original file backed up to {backup_path}")


if __name__ == "__main__":
    update_csv()