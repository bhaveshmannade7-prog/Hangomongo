import logging
import re
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, BigInteger, String, DateTime, Boolean, Integer, func
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from thefuzz import process

logger = logging.getLogger(__name__)
Base = declarative_base()

def clean_text_for_search(text: str) -> str:
    # Yeh function title aur search query, dono ko saaf karta hai
    text = text.lower()
    # "season 1", "s1", "complete season" jaise shabdon ko aasan banata hai
    text = re.sub(r'\b(s|season|seson|sisan)\s*(\d{1,2})\b', r's\2', text)
    text = re.sub(r'complete season', '', text)
    # Special characters hatata hai
    text = re.sub(r'[\W_]+', ' ', text)
    return text.strip()

class User(Base):
    __tablename__ = 'users'
    user_id = Column(BigInteger, primary_key=True) 
    username = Column(String, nullable=True)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    joined_date = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    last_active = Column(DateTime, default=datetime.utcnow)

class Movie(Base):
    __tablename__ = 'movies'
    id = Column(Integer, primary_key=True, autoincrement=True)
    imdb_id = Column(String(50), unique=True, nullable=False, index=True)
    title = Column(String, nullable=False)
    # NEW: Saaf kiya hua title, search ke liye
    clean_title = Column(String, nullable=False, index=True) 
    year = Column(String(10), nullable=True)
    file_id = Column(String, nullable=False)
    channel_id = Column(BigInteger, nullable=False)
    message_id = Column(BigInteger, nullable=False)
    added_date = Column(DateTime, default=datetime.utcnow)

class Database:
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url)
        self.SessionLocal = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=self.engine))
        Base.metadata.create_all(bind=self.engine)
        logger.info("✅ Database initialized successfully.")
    
    def get_session(self): return self.SessionLocal()

    async def add_user(self, user_id, username, first_name, last_name):
        session = self.get_session()
        try:
            user = session.query(User).filter(User.user_id == user_id).first()
            if user:
                user.last_active = datetime.utcnow()
                user.is_active = True
            else:
                session.add(User(user_id=user_id, username=username, first_name=first_name, last_name=last_name))
            session.commit()
        finally: session.close()

    async def get_all_users(self):
        session = self.get_session()
        try: return [user.user_id for user in session.query(User.user_id).filter(User.is_active == True).all()]
        finally: session.close()
    
    async def get_user_count(self):
        session = self.get_session()
        try: return session.query(User).count()
        finally: session.close()

    async def cleanup_inactive_users(self, days: int):
        session = self.get_session()
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            result = session.query(User).filter(User.last_active < cutoff_date, User.is_active == True).update({"is_active": False})
            session.commit()
            return result
        finally: session.close()

    async def add_movie(self, imdb_id, title, year, file_id, message_id, channel_id):
        session = self.get_session()
        try:
            clean_title = clean_text_for_search(title)
            new_movie = Movie(
                imdb_id=imdb_id, title=title, clean_title=clean_title, year=year, 
                file_id=file_id, message_id=message_id, channel_id=channel_id,
            )
            session.add(new_movie)
            session.commit()
            return True
        except Exception as e:
            logger.error(f"Movie add karne mein error: {e}")
            session.rollback()
            return False
        finally: session.close()
    
    async def get_movie_count(self):
        session = self.get_session()
        try: return session.query(Movie).count()
        finally: session.close()

    async def get_movie_by_imdb(self, imdb_id: str):
        session = self.get_session()
        try:
            movie = session.query(Movie).filter(Movie.imdb_id == imdb_id).first()
            return {c.name: getattr(movie, c.name) for c in movie.__table__.columns} if movie else None
        finally: session.close()

    # --- NEW SUPER-SMART SEARCH FUNCTION ---
    async def super_search_movies(self, query: str, limit: int = 20):
        session = self.get_session()
        try:
            # Step 1: Database se jald se jald milte-julte naam nikalein
            # Hum 'word1%word2%' jaisa pattern banayenge
            search_pattern = '%' + '%'.join(query.split()) + '%'
            
            potential_matches = session.query(Movie).filter(
                Movie.clean_title.ilike(search_pattern)
            ).limit(100).all()

            if not potential_matches:
                return []

            # Step 2: Ab in 100 results par Python ki fuzzy search lagayein
            choices = {movie.title: movie for movie in potential_matches}
            
            # process.extract zyada smart hai, typos ko a_chhe se handle karta hai
            results = process.extract(query, choices.keys(), limit=limit)
            
            final_list = []
            for title, score in results:
                if score > 45: # Thoda kam score bhi aane dein, taaki "avgar" jaisi query kaam kare
                    movie_obj = choices[title]
                    final_list.append({'imdb_id': movie_obj.imdb_id, 'title': movie_obj.title})
            
            return final_list

        except Exception as e:
            logger.error(f"Super search mein error: {e}")
            return []
        finally:
            session.close()
