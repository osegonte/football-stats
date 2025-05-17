#!/usr/bin/env python3
"""
FBref Data Collector

This script fetches team statistics from FBref.com using a polite scraping approach.
It can work with your existing team names from CSV files and maps them to FBref IDs.

Features:
- Fetches recent match data for specified teams
- Handles rate limiting with polite delays
- Caches requests to minimize redundant downloads
- Maps team names to their FBref IDs
- Extracts common statistics (goals, xg, possession, etc.)
- Outputs to CSV for further analysis

Usage:
  python fbref_collector.py --input fixtures.csv --output stats.csv
  python fbref_collector.py --team "Liverpool" --lookback 10
  python fbref_collector.py --league "Premier League" --season 2023-2024
"""

import os
import sys
import time
import random
import logging
import json
import argparse
import re
from datetime import datetime
from io import StringIO
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import hashlib

# Configure logging
os.makedirs("logs", exist_ok=True)
log_file = f"logs/fbref_collector_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("fbref_collector")

# Constants
FBREF_BASE_URL = "https://fbref.com/en/"
CACHE_DIR = "data/cache"
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15'
]

# League ID mapping
LEAGUE_MAPPING = {
    "Premier League": "9",
    "La Liga": "12",
    "Bundesliga": "20",
    "Serie A": "11",
    "Ligue 1": "13",
    "Eredivisie": "23",
    "Primeira Liga": "32",
    "Championship": "10",
    "MLS": "22",
    "UEFA Champions League": "8",
    "UEFA Europa League": "19"
}

class RateLimitedRequester:
    """Handle web requests with rate limiting and caching"""
    
    def __init__(self, cache_dir=CACHE_DIR, min_delay=5, max_delay=10, cache_ttl=24*60*60):
        """
        Initialize the requester
        
        Args:
            cache_dir: Directory to store cached responses
            min_delay: Minimum delay between requests (seconds)
            max_delay: Maximum delay between requests (seconds)
            cache_ttl: Cache time-to-live in seconds (default: 24 hours)
        """
        self.cache_dir = cache_dir
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.cache_ttl = cache_ttl
        self.last_request_time = 0
        self.session = requests.Session()
        
        # Create cache directory if it doesn't exist
        os.makedirs(cache_dir, exist_ok=True)
    
    def _get_cache_path(self, url):
        """Get the cache file path for a URL"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{url_hash}.html")
    
    def get(self, url, use_cache=True):
        """
        Send a GET request with rate limiting and caching
        
        Args:
            url: The URL to request
            use_cache: Whether to use cached responses
            
        Returns:
            Response text
        """
        cache_path = self._get_cache_path(url)
        
        # Check cache first
        if use_cache and os.path.exists(cache_path):
            cache_age = time.time() - os.path.getmtime(cache_path)
            if cache_age < self.cache_ttl:
                logger.debug(f"Using cached response for {url}")
                with open(cache_path, 'r', encoding='utf-8') as f:
                    return f.read()
        
        # Rate limiting
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        delay = random.uniform(self.min_delay, self.max_delay)
        
        if elapsed < delay:
            sleep_time = delay - elapsed
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)
        
        # Send request
        logger.info(f"Requesting {url}")
        headers = {'User-Agent': random.choice(USER_AGENTS)}
        response = self.session.get(url, headers=headers)
        self.last_request_time = time.time()
        
        # Check for rate limiting response
        if response.status_code == 429:
            logger.warning("Rate limited! Waiting 60 seconds...")
            time.sleep(60)
            return self.get(url, use_cache)  # Retry
        
        response.raise_for_status()
        
        # Cache the response
        with open(cache_path, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        return response.text

class FBrefTeamMapper:
    """Map team names to FBref team IDs"""
    
    def __init__(self, requester):
        """
        Initialize the team mapper
        
        Args:
            requester: Rate-limited requester instance
        """
        self.requester = requester
        self.mapping_cache = {}
        self.mapping_file = os.path.join(CACHE_DIR, "team_mapping.json")
        self._load_mapping_cache()
    
    def _load_mapping_cache(self):
        """Load the team mapping cache from disk"""
        if os.path.exists(self.mapping_file):
            try:
                with open(self.mapping_file, 'r', encoding='utf-8') as f:
                    self.mapping_cache = json.load(f)
                logger.info(f"Loaded {len(self.mapping_cache)} team mappings from cache")
            except Exception as e:
                logger.error(f"Error loading team mapping cache: {e}")
    
    def _save_mapping_cache(self):
        """Save the team mapping cache to disk"""
        try:
            with open(self.mapping_file, 'w', encoding='utf-8') as f:
                json.dump(self.mapping_cache, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving team mapping cache: {e}")
    
    def search_team(self, team_name):
        """
        Search for a team by name
        
        Args:
            team_name: Team name to search for
            
        Returns:
            dict: Team information (id, name, url) or None if not found
        """
        # Check cache first
        cache_key = team_name.lower()
        if cache_key in self.mapping_cache:
            logger.debug(f"Found {team_name} in mapping cache")
            return self.mapping_cache[cache_key]
        
        # Search FBref
        search_url = f"{FBREF_BASE_URL}search/search.fcgi?search={team_name}"
        html = self.requester.get(search_url)
        soup = BeautifulSoup(html, 'html.parser')
        
        # Look for team links in search results
        team_results = []
        for section in soup.find_all('div', class_='search-section'):
            if 'Teams' in section.get_text():
                for a in section.find_all('a'):
                    if '/squads/' in a['href']:
                        # Extract team ID from URL
                        team_id_match = re.search(r'/squads/([a-f0-9]+)/', a['href'])
                        if team_id_match:
                            team_id = team_id_match.group(1)
                            team_info = {
                                'id': team_id,
                                'name': a.get_text(),
                                'url': urljoin(FBREF_BASE_URL, a['href'])
                            }
                            team_results.append(team_info)
        
        # If we found matches, use the first one
        if team_results:
            best_match = team_results[0]
            self.mapping_cache[cache_key] = best_match
            self._save_mapping_cache()
            return best_match
        
        logger.warning(f"No team matches found for '{team_name}'")
        return None
    
    def find_team_in_league(self, team_name, league_id):
        """
        Find a team in a specific league
        
        Args:
            team_name: Team name to search for
            league_id: FBref league ID
            
        Returns:
            dict: Team information or None if not found
        """
        cache_key = f"{team_name.lower()}:{league_id}"
        if cache_key in self.mapping_cache:
            return self.mapping_cache[cache_key]
            
        # Get the league teams page
        league_url = f"{FBREF_BASE_URL}comps/{league_id}/clubs/"
        html = self.requester.get(league_url)
        soup = BeautifulSoup(html, 'html.parser')
        
        # Look for team links
        teams_table = soup.find('table', id='clubs')
        if not teams_table:
            return self.search_team(team_name)
        
        for a in teams_table.find_all('a'):
            team_name_el = a.get_text().strip()
            if team_name.lower() in team_name_el.lower():
                # Extract team ID from URL
                team_id_match = re.search(r'/squads/([a-f0-9]+)/', a['href'])
                if team_id_match:
                    team_id = team_id_match.group(1)
                    team_info = {
                        'id': team_id,
                        'name': team_name_el,
                        'url': urljoin(FBREF_BASE_URL, a['href'])
                    }
                    self.mapping_cache[cache_key] = team_info
                    self._save_mapping_cache()
                    return team_info
        
        # If not found in this league, try general search
        return self.search_team(team_name)

class FBrefDataCollector:
    """Collect team data from FBref"""
    
    def __init__(self, requester=None, team_mapper=None):
        """
        Initialize the data collector
        
        Args:
            requester: Rate-limited requester instance (or None to create a new one)
            team_mapper: Team mapper instance (or None to create a new one)
        """
        self.requester = requester or RateLimitedRequester()
        self.team_mapper = team_mapper or FBrefTeamMapper(self.requester)
        
        # Create output directories
        os.makedirs("data/output", exist_ok=True)
    
    def get_team_matches(self, team_info, lookback=7):
        """
        Get the recent matches for a team
        
        Args:
            team_info: Team information dictionary
            lookback: Number of matches to look back
            
        Returns:
            pd.DataFrame: DataFrame of matches
        """
        # Get the team's matches page
        matches_url = team_info['url'].replace('/squads/', '/matchlogs/all_comps/schedule/')
        html = self.requester.get(matches_url)
        
        # Parse the HTML table
        try:
            dfs = pd.read_html(StringIO(html))
            matches_df = None
            
            # Find the correct table (should be the first one with the correct columns)
            for df in dfs:
                if 'Date' in df.columns and 'Comp' in df.columns:
                    matches_df = df
                    break
            
            if matches_df is None:
                logger.error(f"Could not find matches table for {team_info['name']}")
                return pd.DataFrame()
            
            # Clean up the DataFrame
            matches_df = matches_df.rename(columns={
                'Date': 'date',
                'Day': 'day',
                'Comp': 'competition',
                'Round': 'round',
                'Venue': 'venue',
                'Result': 'result',
                'GF': 'goals_for',
                'GA': 'goals_against',
                'Opponent': 'opponent',
                'xG': 'xg',
                'xGA': 'xga',
                'Poss': 'possession',
                'Attendance': 'attendance',
                'Captain': 'captain',
                'Formation': 'formation',
                'Referee': 'referee'
            })
            
            # Convert the date
            matches_df['date'] = pd.to_datetime(matches_df['date'])
            
            # Sort by date (most recent first) and take only lookback matches
            matches_df = matches_df.sort_values('date', ascending=False).head(lookback)
            
            # Create a match_id column
            matches_df['team_name'] = team_info['name']
            matches_df['team_id'] = team_info['id']
            matches_df['match_id'] = matches_df['date'].dt.strftime('%Y%m%d') + '_' + \
                                   matches_df['team_id'] + '_' + \
                                   matches_df['opponent'].str.replace(' ', '')
            
            # Process results
            if 'result' in matches_df.columns:
                matches_df['result'] = matches_df['result'].astype(str)
                matches_df['points'] = matches_df['result'].map({'W': 3, 'D': 1, 'L': 0})
            
            return matches_df
        
        except Exception as e:
            logger.error(f"Error parsing matches for {team_info['name']}: {e}")
            return pd.DataFrame()
    
    def get_team_stats(self, team_name, league_name=None, lookback=7):
        """
        Get statistics for a team
        
        Args:
            team_name: Name of the team
            league_name: Name of the league (optional)
            lookback: Number of matches to look back
            
        Returns:
            pd.DataFrame: DataFrame of match statistics
        """
        try:
            # Find the team
            if league_name and league_name in LEAGUE_MAPPING:
                league_id = LEAGUE_MAPPING[league_name]
                team_info = self.team_mapper.find_team_in_league(team_name, league_id)
            else:
                team_info = self.team_mapper.search_team(team_name)
            
            if not team_info:
                logger.error(f"Could not find team: {team_name}")
                return pd.DataFrame()
            
            logger.info(f"Found team: {team_info['name']} (ID: {team_info['id']})")
            
            # Get the team's matches
            matches_df = self.get_team_matches(team_info, lookback)
            
            if matches_df.empty:
                logger.warning(f"No matches found for {team_name}")
                return pd.DataFrame()
            
            # Add a processing timestamp
            matches_df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            return matches_df
            
        except Exception as e:
            logger.error(f"Error getting stats for {team_name}: {e}")
            return pd.DataFrame()
    
    def process_fixture_teams(self, fixtures_file, output_file=None, lookback=7):
        """
        Process all teams from a fixtures CSV file
        
        Args:
            fixtures_file: Path to the CSV file with fixtures
            output_file: Path to the output CSV file (optional)
            lookback: Number of matches to look back
            
        Returns:
            str: Path to the output file
        """
        try:
            # Read the fixtures file
            fixtures_df = pd.read_csv(fixtures_file)
            
            # Get unique teams
            if 'home_team' in fixtures_df.columns and 'away_team' in fixtures_df.columns:
                teams = pd.concat([fixtures_df['home_team'], fixtures_df['away_team']]).unique()
            else:
                # Try to find team columns with different names
                team_cols = [col for col in fixtures_df.columns if 'team' in col.lower()]
                if len(team_cols) >= 2:
                    teams = pd.concat([fixtures_df[team_cols[0]], fixtures_df[team_cols[1]]]).unique()
                else:
                    raise ValueError("Could not identify team columns in fixtures file")
            
            logger.info(f"Found {len(teams)} unique teams in fixtures file")
            
            # Process each team
            all_stats = []
            for team_name in teams:
                logger.info(f"Processing team: {team_name}")
                team_stats = self.get_team_stats(team_name, lookback=lookback)
                if not team_stats.empty:
                    all_stats.append(team_stats)
                    logger.info(f"Got {len(team_stats)} matches for {team_name}")
                else:
                    logger.warning(f"No statistics found for {team_name}")
                
                # Add a small delay
                time.sleep(random.uniform(1, 3))
            
            if not all_stats:
                logger.error("No statistics found for any team")
                return None
            
            # Combine all stats
            combined_stats = pd.concat(all_stats, ignore_index=True)
            
            # Save to CSV
            if output_file is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file = f"data/output/team_stats_{timestamp}.csv"
            
            combined_stats.to_csv(output_file, index=False)
            logger.info(f"Saved {len(combined_stats)} match statistics to {output_file}")
            
            return output_file
            
        except Exception as e:
            logger.error(f"Error processing fixtures file: {e}")
            return None
    
    def calculate_aggregate_stats(self, stats_df):
        """
        Calculate aggregate statistics for each team
        
        Args:
            stats_df: DataFrame of match statistics
            
        Returns:
            pd.DataFrame: DataFrame of aggregate statistics
        """
        if stats_df.empty:
            return pd.DataFrame()
        
        # Group by team
        team_groups = stats_df.groupby('team_name')
        
        # Calculate averages and totals
        agg_stats = []
        
        for team_name, team_df in team_groups:
            # Calculate basic stats
            row = {
                'team_name': team_name,
                'matches_played': len(team_df),
                'wins': (team_df['result'] == 'W').sum(),
                'draws': (team_df['result'] == 'D').sum(),
                'losses': (team_df['result'] == 'L').sum(),
                'points': team_df['points'].sum(),
                'goals_for_total': team_df['goals_for'].sum(),
                'goals_against_total': team_df['goals_against'].sum(),
                'goal_diff': team_df['goals_for'].sum() - team_df['goals_against'].sum(),
                'avg_goals_for': team_df['goals_for'].mean(),
                'avg_goals_against': team_df['goals_against'].mean(),
            }
            
            # Add additional stats if available
            if 'xg' in team_df.columns:
                row['avg_xg'] = team_df['xg'].mean()
            if 'xga' in team_df.columns:
                row['avg_xga'] = team_df['xga'].mean()
            if 'possession' in team_df.columns:
                row['avg_possession'] = team_df['possession'].mean()
            
            agg_stats.append(row)
        
        return pd.DataFrame(agg_stats)

def main():
    parser = argparse.ArgumentParser(description="FBref Data Collector")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--input', help='Input fixtures CSV file')
    group.add_argument('--team', help='Process a single team')
    group.add_argument('--league', help='Process all teams in a league')
    
    parser.add_argument('--output', help='Output CSV file (optional)')
    parser.add_argument('--lookback', type=int, default=7, help='Number of matches to look back (default: 7)')
    parser.add_argument('--league-name', help='League name for the team (optional)')
    parser.add_argument('--season', help='Season to process (e.g., 2023-2024)')
    
    args = parser.parse_args()
    
    collector = FBrefDataCollector()
    
    if args.input:
        # Process teams from fixtures file
        output_file = collector.process_fixture_teams(args.input, args.output, args.lookback)
        if output_file:
            print(f"Statistics saved to {output_file}")
            return 0
        else:
            print("Failed to process fixtures file")
            return 1
    
    elif args.team:
        # Process a single team
        team_stats = collector.get_team_stats(args.team, args.league_name, args.lookback)
        if team_stats.empty:
            print(f"No statistics found for {args.team}")
            return 1
        
        # Save to CSV
        if args.output:
            output_file = args.output
        else:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            team_slug = args.team.replace(' ', '_').lower()
            output_file = f"data/output/{team_slug}_stats_{timestamp}.csv"
        
        team_stats.to_csv(output_file, index=False)
        print(f"Statistics for {args.team} saved to {output_file}")
        
        # Show a summary
        print("\nSummary:")
        print(f"Matches found: {len(team_stats)}")
        if not team_stats.empty:
            print(f"Date range: {team_stats['date'].min()} to {team_stats['date'].max()}")
            print(f"Record: {(team_stats['result'] == 'W').sum()}W {(team_stats['result'] == 'D').sum()}D {(team_stats['result'] == 'L').sum()}L")
            print(f"Goals: {team_stats['goals_for'].sum()}-{team_stats['goals_against'].sum()}")
        
        return 0
    
    elif args.league:
        # Process a league
        if args.league not in LEAGUE_MAPPING:
            print(f"Unknown league: {args.league}")
            print(f"Available leagues: {', '.join(LEAGUE_MAPPING.keys())}")
            return 1
        
        print(f"League processing not yet implemented")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())