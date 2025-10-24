# algolia_sync.py

import os
import logging
from typing import List, Dict, Any, Optional
from algoliasearch.search_client import SearchClient

logger = logging.getLogger("algolia")

# Algolia Configuration
ALGOLIA_APP_ID = os.getenv("ALGOLIA_APP_ID")
ALGOLIA_API_KEY = os.getenv("ALGOLIA_API_KEY") # Admin Key for Indexing
ALGOLIA_SEARCH_KEY = os.getenv("ALGOLIA_SEARCH_KEY") # Search Key for Querying
ALGOLIA_INDEX_NAME = "movie_index"

if not ALGOLIA_APP_ID or not ALGOLIA_API_KEY or not ALGOLIA_SEARCH_KEY:
    logger.critical("Missing Algolia credentials!")

class AlgoliaSearch:
    def __init__(self):
        self.search_client = SearchClient.create(ALGOLIA_APP_ID, ALGOLIA_SEARCH_KEY)
        self.admin_client = SearchClient.create(ALGOLIA_APP_ID, ALGOLIA_API_KEY)
        self.search_index = self.search_client.init_index(ALGOLIA_INDEX_NAME)
        self.admin_index = self.admin_client.init_index(ALGOLIA_INDEX_NAME)
        logger.info(f"Algolia client initialized. Index: {ALGOLIA_INDEX_NAME}")

    async def init_index(self):
        """Set up index settings for optimal fuzzy search."""
        try:
            # Setting searchable and display attributes
            self.admin_index.set_settings({
                'searchableAttributes': ['title', 'year', 'imdb_id'],
                'customRanking': ['desc(added_date)'],
                'attributesForFaceting': ['year'],
                'typoTolerance': 'strict', # Free-Tier CPU ko bachane ke liye
                'minWordSizefor1Typo': 4,
            }).wait()
            logger.info("Algolia index settings updated successfully.")
            return True
        except Exception as e:
            logger.error(f"Algolia setting init failed: {e}")
            return False

    async def add_movie_to_index(self, movie_data: Dict[str, Any]) -> bool:
        """Adds or updates a single movie record."""
        try:
            # Algolia uses objectID, we will use imdb_id for consistency
            record = {
                'objectID': movie_data['imdb_id'],
                'title': movie_data['title'],
                'year': movie_data['year'],
                'imdb_id': movie_data['imdb_id'],
                'added_date': movie_data.get('added_date', datetime.utcnow().isoformat()),
            }
            self.admin_index.save_object(record).wait()
            return True
        except Exception as e:
            logger.error(f"Algolia add/update failed for {movie_data.get('imdb_id')}: {e}")
            return False
            
    async def remove_movie_from_index(self, imdb_id: str) -> bool:
        """Removes a movie record from the index."""
        try:
            self.admin_index.delete_object(imdb_id).wait()
            return True
        except Exception as e:
            logger.error(f"Algolia delete failed for {imdb_id}: {e}")
            return False

    async def search_movies(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Performs search using the search-only key."""
        try:
            # 'hitsPerPage' is the limit in Algolia
            results = self.search_index.search(query, {
                'hitsPerPage': limit,
                # Simple query rules for better free-tier performance
                'queryLanguages': ['en'],
            })
            
            return [
                {'imdb_id': hit['imdb_id'], 'title': hit['title']}
                for hit in results['hits']
            ]
        except Exception as e:
            logger.error(f"Algolia search failed for query '{query}': {e}")
            return []

# Initialize client
algolia_client = AlgoliaSearch()
