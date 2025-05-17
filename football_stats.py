import requests
import json
import pandas as pd


def load_mappings(path="id_mappings.json"):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_team_id(team, league, mappings):
    return mappings.get('leagues', {}).get(league, {}).get(team)


def fetch_last7(team_id):
    url = f"https://api.football-data.org/v4/teams/{team_id}/matches?limit=7"
    headers = {"X-Auth-Token": "YOUR_API_TOKEN"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json().get('matches', [])
    results = []
    for m in data:
        home = m['homeTeam']['name']
        away = m['awayTeam']['name']
        sh = m['score']['fullTime']['home']
        sa = m['score']['fullTime']['away']
        results.append((home, away, sh, sa))
    return results


def stats_last7(row, mappings):
    team = row['Team']
    league = row['League']  # adjust if your CSV uses a different column name
    team_id = get_team_id(team, league, mappings)
    if not team_id:
        return pd.Series([None, None, None, None, None],
                         index=['GF_last7','GA_last7','W_last7','D_last7','L_last7'])
    matches = fetch_last7(team_id)
    gf = ga = w = d = l = 0
    for home, away, sh, sa in matches:
        if team == home:
            gf += sh; ga += sa
            if sh > sa: w += 1
            elif sh < sa: l += 1
            else: d += 1
        elif team == away:
            gf += sa; ga += sh
            if sa > sh: w += 1
            elif sa < sh: l += 1
            else: d += 1
    return pd.Series([gf, ga, w, d, l],
                     index=['GF_last7','GA_last7','W_last7','D_last7','L_last7'])


def main(csv_path):
    mappings = load_mappings()
    df = pd.read_csv(csv_path)
    stats = df.apply(lambda row: stats_last7(row, mappings), axis=1)
    df = pd.concat([df, stats], axis=1)
    df.to_csv(csv_path, index=False)


if __name__ == '__main__':
    main('your_file.csv')