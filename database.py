import logging
import re
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, BigInteger, String, DateTime, Boolean, Integer, func
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from thefuzz import process

logger = logging.getLogger(__name__)
Base = declarative_base()

# ... (User aur Movie tables ka structure waisa hi rahega) ...
class User(Base):
    __tablename__ = 'users'
    #...
class Movie(Base):
    __tablename__ = 'movies'
    #...

def clean_text_for_search(text: str) -> str:
    text = text.lower()
    text = re.sub(r'\b(s|season|seson|sisan)\s*(\d{1,2})\b', r's\2', text)
    text = re.sub(r'complete season', '', text)
    text = re.sub(r'[\W_]+', ' ', text)
    return text.strip()

class Database:
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url)
        self.SessionLocal = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=self.engine))
        Base.metadata.create_all(bind=self.engine)
        logger.info("✅ Database initialized successfully.")
    
    def get_session(self): return self.SessionLocal()

    # ... (add_user, get_all_users, etc. sabhi functions waise hi rahenge) ...

    async def add_movie(self, imdb_id, title, year, file_id, message_id, channel_id, genre=None, rating=None):
        session = self.get_session()
        try:
            # Title ko saaf karke save karein taaki search aasan ho
            clean_title = clean_text_for_search(title)
            new_movie = Movie(
                imdb_id=imdb_id, title=title, clean_title=clean_title, year=year, 
                file_id=file_id, message_id=message_id, channel_id=channel_id,
                genre=genre, rating=rating
            )
            session.add(new_movie)
            session.commit()
            return True
        except Exception as e:
            logger.error(f"Movie add karne mein error: {e}")
            session.rollback()
            return False
        finally: session.close()
    
    # --- NEW SUPER-SMART SEARCH FUNCTION ---
    async def super_search_movies(self, query: str, limit: int = 20):
        session = self.get_session()
        try:
            # Step 1: Database se jald se jald milte-julte naam nikalein
            # Hum 'word1%word2%' jaisa pattern banayenge
            search_pattern = '%' + '%'.join(query.split()) + '%'
            
            # ILIKE case-insensitive hota hai
            potential_matches = session.query(Movie).filter(
                Movie.clean_title.ilike(search_pattern)
            ).limit(100).all() # 100 potential match nikalein

            if not potential_matches:
                return []

            # Step 2: Ab in 100 results par Python ki fuzzy search lagayein
            # Isse "avgar" ko "avengers" se milane mein madad milti hai
            choices = {movie.title: movie for movie in potential_matches}
            
            results = process.extract(query, choices.keys(), limit=limit)
            
            # Final, sorted list return karein
            final_list = []
            for title, score in results:
                if score > 50: # Sirf a_chhe match hi dikhayein
                    movie_obj = choices[title]
                    final_list.append({'imdb_id': movie_obj.imdb_id, 'title': movie_obj.title})
            
            return final_list

        except Exception as e:
            logger.error(f"Super search mein error: {e}")
            return []
        finally:
            session.close()

    # ... (baaki sabhi database functions) ...
