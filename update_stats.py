import os
import json
import pandas as pd
import requests
from dotenv import load_dotenv
from datetime import datetime

# ─── CONFIGURATION ─────────────────────────────────────────────────────────
load_dotenv()
API_KEY      = os.getenv("FOOTBALL_API_KEY")
API_BASE     = "https://api.football-data.org/v2"

# Determine project directory (where this script resides)
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
CSV_PATH     = os.path.join(BASE_DIR, "past7_matches_with_league_date_sorted.csv")
MAPPING_PATH = os.path.join(BASE_DIR, "id_mappings.json")

# Define which statistics to extract
STATS_FIELDS = [
    "Goals for",
    "Goals against",
    "Total shots",
    "Shots on target",
    "Possession (%)"
]
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
    url = f"{API_BASE}/competitions/{comp_code}/teams"
    resp = requests.get(url, headers={"X-Auth-Token": API_KEY})
    resp.raise_for_status()
    for t in resp.json().get("teams", []):
        team_map[t.get("name")] = t.get("id")


def lookup_team_id(team_name: str) -> int | None:
    """Get a team's ID, if known."""
    return team_map.get(team_name)


def fetch_last_matches(team_id: int, n: int = 7) -> list:
    url = f"{API_BASE}/teams/{team_id}/matches?status=FINISHED&limit={n}"
    resp = requests.get(url, headers={"X-Auth-Token": API_KEY})
    resp.raise_for_status()
    data = resp.json().get("matches", [])
    data.sort(key=lambda m: m.get("utcDate"), reverse=True)
    return data[:n]


def extract_stats(match: dict, team_name: str) -> dict:
    is_home = (match.get("homeTeam", {}).get("name") == team_name)
    link = match.get("_links", {}).get("self", {}).get("href", "")
    stats_url = link + "/statistics"
    resp = requests.get(stats_url, headers={"X-Auth-Token": API_KEY})
    resp.raise_for_status()
    stats_data = resp.json()
    team_stats = stats_data.get("homeStatistics") if is_home else stats_data.get("awayStatistics")
    key_map = {
        "Goals for":       "goalsFor",
        "Goals against":   "goalsAgainst",
        "Total shots":     "shotsTotal",
        "Shots on target": "shotsOnTarget",
        "Possession (%)":  "possession"
    }
    return {field: team_stats.get(key_map[field]) for field in STATS_FIELDS}


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
            print(f"⚠️ No competition code for league '{league}', team IDs will be missing.")
    save_mappings()

    # Process each row
    out_rows = []
    for _, row in df.iterrows():
        team = row.get("Team")
        stats = {}
        tid  = lookup_team_id(team)
        if not tid:
            print(f"⚠️ Team ID not found for '{team}', filling with NA.")
            for i in range(1, 8):
                for field in STATS_FIELDS:
                    stats[f"Match {i} {field}"] = pd.NA
        else:
            try:
                matches = fetch_last_matches(tid)
                for i in range(1, 8):
                    if i <= len(matches):
                        rs = extract_stats(matches[i-1], team)
                    else:
                        rs = {field: pd.NA for field in STATS_FIELDS}
                    for field, val in rs.items():
                        stats[f"Match {i} {field}"] = val
            except Exception as e:
                print(f"⚠️ Error fetching stats for '{team}': {e}")
                for i in range(1, 8):
                    for field in STATS_FIELDS:
                        stats[f"Match {i} {field}"] = pd.NA
        out_rows.append({**row.to_dict(), **stats})

    updated = pd.DataFrame(out_rows)
    if len(updated) != len(df):
        print("⚠️ Row count mismatch; CSV NOT overwritten.")
        return

    updated.to_csv(CSV_PATH, index=False)
    print(f"[{datetime.now():%Y-%m-%d %H:%M}] CSV updated for {len(out_rows)} teams.")


if __name__ == "__main__":
    update_csv()
