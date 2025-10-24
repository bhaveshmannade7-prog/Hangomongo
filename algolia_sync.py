# algolia_sync.py

import os
import logging
import asyncio
from typing import List, Dict, Any, Optional
from algoliasearch.search_client import SearchClient
from datetime import datetime

logger = logging.getLogger("algolia")

# Algolia Configuration
ALGOLIA_APP_ID = os.getenv("ALGOLIA_APP_ID")
ALGOLIA_API_KEY = os.getenv("ALGOLIA_API_KEY") # Admin Key for Indexing
ALGOLIA_SEARCH_KEY = os.getenv("ALGOLIA_SEARCH_KEY") # Search Key for Querying
ALGOLIA_INDEX_NAME = "movie_index"

if not ALGOLIA_APP_ID or not ALGOLIA_API_KEY or not ALGOLIA_SEARCH_KEY:
    logger.critical("Missing Algolia credentials! Search will be disabled.")

class AlgoliaSearch:
    def __init__(self):
        # Client Initialization is safe even if keys are missing; operations will fail later.
        self.search_client = SearchClient.create(ALGOLIA_APP_ID or "DUMMY", ALGOLIA_SEARCH_KEY or "DUMMY")
        self.admin_client = SearchClient.create(ALGOLIA_APP_ID or "DUMMY", ALGOLIA_API_KEY or "DUMMY")
        self.search_index = self.search_client.init_index(ALGOLIA_INDEX_NAME)
        self.admin_index = self.admin_client.init_index(ALGOLIA_INDEX_NAME)
        logger.info(f"Algolia client initialized. Index: {ALGOLIA_INDEX_NAME}")

    async def init_index(self):
        """Set up index settings for optimal search."""
        if not ALGOLIA_APP_ID or not ALGOLIA_API_KEY: return False
        try:
            # Using asyncio.to_thread for potentially blocking Algolia sync operation
            await asyncio.to_thread(
                self.admin_index.set_settings,
                {
                    'searchableAttributes': ['title', 'year', 'imdb_id'],
                    'customRanking': ['desc(added_date)'],
                    'attributesForFaceting': ['year'],
                    'typoTolerance': 'min', # Relaxed typo tolerance for Free-Tier speed/relevance
                    'minWordSizefor1Typo': 4,
                }
            )
            logger.info("Algolia index settings updated successfully.")
            return True
        except Exception as e:
            logger.error(f"Algolia setting init failed: {e}")
            return False

    async def add_movie_to_index(self, movie_data: Dict[str, Any]) -> bool:
        """Adds or updates a single movie record."""
        if not ALGOLIA_APP_ID or not ALGOLIA_API_KEY: return False
        try:
            record = {
                'objectID': movie_data['imdb_id'],
                'title': movie_data['title'],
                'year': movie_data['year'],
                'imdb_id': movie_data['imdb_id'],
                'added_date': movie_data.get('added_date', datetime.utcnow().isoformat()),
            }
            # Use asyncio.to_thread for potentially blocking Algolia network call
            await asyncio.to_thread(self.admin_index.save_object, record)
            return True
        except Exception as e:
            logger.error(f"Algolia add/update failed for {movie_data.get('imdb_id')}: {e}")
            return False
            
    async def remove_movie_from_index(self, imdb_id: str) -> bool:
        """Removes a movie record from the index."""
        if not ALGOLIA_APP_ID or not ALGOLIA_API_KEY: return False
        try:
            await asyncio.to_thread(self.admin_index.delete_object, imdb_id)
            return True
        except Exception as e:
            logger.error(f"Algolia delete failed for {imdb_id}: {e}")
            return False

    async def search_movies(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Performs search using the search-only key."""
        if not ALGOLIA_APP_ID or not ALGOLIA_SEARCH_KEY: return []
        try:
            results = await asyncio.to_thread(
                self.search_index.search,
                query,
                {'hitsPerPage': limit}
            )
            
            return [
                {'imdb_id': hit['imdb_id'], 'title': hit['title']}
                for hit in results['hits']
            ]
        except Exception as e:
            logger.error(f"Algolia search failed for query '{query}': {e}")
            return []
            
    async def save_objects_batch(self, objects: List[Dict[str, Any]]) -> bool:
        """Saves a batch of objects (used for initial sync)."""
        if not ALGOLIA_APP_ID or not ALGOLIA_API_KEY: return False
        try:
            await asyncio.to_thread(self.admin_index.save_objects, objects)
            return True
        except Exception as e:
            logger.error(f"Algolia batch save failed: {e}")
            return False

# Initialize client
algolia_client = AlgoliaSearch()
