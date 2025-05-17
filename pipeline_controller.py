# Add this import at the top of your file
import subprocess
from fbref_collector import FBrefDataCollector

# Add this function to your pipeline controller
def collect_fbref_data(fixtures_csv, max_teams=0):
    """Collect team statistics from FBref"""
    try:
        logger.info("Initializing FBref stats collector")
        
        # Initialize collector
        collector = FBrefDataCollector()
        
        # Run collector
        output_file = collector.process_fixture_teams(
            fixtures_file=fixtures_csv,
            lookback=7  # Get 7 past matches per team
        )
        
        if output_file:
            logger.info(f"Successfully collected FBref stats, saved to: {output_file}")
            return {'success': True, 'output_file': output_file}
        else:
            logger.error("Failed to collect FBref stats")
            return {'success': False, 'error': 'Failed to collect FBref stats'}
            
    except Exception as e:
        logger.exception(f"Error collecting FBref stats: {e}")
        return {'success': False, 'error': str(e)}