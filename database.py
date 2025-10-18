import logging
import re
from datetime import datetime, timedelta
# Asynchronous SQLAlchemy imports
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_scoped_session
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, BigInteger, String, DateTime, Boolean, Integer, func, select

from thefuzz import process

logger = logging.getLogger(__name__)
Base = declarative_base()

def clean_text_for_search(text: str) -> str:
    """Text ko search ke liye saaf aur optimize karta hai (lowercase, special chars removed)."""
    text = text.lower()
    text = re.sub(r'\b(s|season|seson|sisan)\s*(\d{1,2})\b', r's\2', text)
    text = re.sub(r'complete season', '', text)
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
    last_active = Column(DateTime, default=datetime.utcnow) # Concurrency tracking ke liye upyog hoga

class Movie(Base):
    __tablename__ = 'movies'
    id = Column(Integer, primary_key=True, autoincrement=True)
    imdb_id = Column(String(50), unique=True, nullable=False, index=True)
    title = Column(String, nullable=False)
    clean_title = Column(String, nullable=False, index=True) 
    year = Column(String(10), nullable=True)
    file_id = Column(String, nullable=False)
    channel_id = Column(BigInteger, nullable=False)
    message_id = Column(BigInteger, nullable=False)
    added_date = Column(DateTime, default=datetime.utcnow)

class Database:
    def __init__(self, database_url: str):
        # FIX: Asynchronous engine ka upyog karein (eg. 'postgresql+asyncpg://')
        self.engine = create_async_engine(database_url, echo=False)
        self.SessionLocal = async_scoped_session(
            sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession),
            scopefunc=lambda: None
        )
        logger.info("✅ Database engine initialized for async operations.")
    
    async def init_db(self):
        """Database mein tables ko create karta hai agar woh maujood nahi hain."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("✅ Database tables checked/created successfully.")

    def get_session(self) -> AsyncSession: return self.SessionLocal()

    async def add_user(self, user_id, username, first_name, last_name):
        """User ko add ya last_active time update karta hai."""
        session = self.get_session()
        try:
            result = await session.execute(select(User).filter(User.user_id == user_id))
            user = result.scalar_one_or_none()
            
            if user:
                user.last_active = datetime.utcnow()
                user.is_active = True
            else:
                session.add(User(user_id=user_id, username=username, first_name=first_name, last_name=last_name))
            
            await session.commit()
        finally: await session.close()

    async def get_concurrent_user_count(self, minutes: int = 5):
        """Pichhle 'minutes' mein active users ki sankhaya count karta hai."""
        session = self.get_session()
        try:
            cutoff_time = datetime.utcnow() - timedelta(minutes=minutes)
            
            result = await session.execute(
                select(func.count(User.user_id))
                .filter(User.last_active >= cutoff_time)
                .filter(User.is_active == True)
            )
            return result.scalar_one()
        finally: await session.close()

    async def get_all_users(self):
        """Sabhi active users ki ID list return karta hai (Broadcast ke liye)."""
        session = self.get_session()
        try: 
            result = await session.execute(select(User.user_id).filter(User.is_active == True))
            return result.scalars().all()
        finally: await session.close()
    
    async def get_user_count(self):
        """Database mein total users ki sankhaya count karta hai."""
        session = self.get_session()
        try: 
            result = await session.execute(select(func.count(User.user_id)))
            return result.scalar_one()
        finally: await session.close()

    async def cleanup_inactive_users(self, days: int):
        """Nirdharit dinon se inactive users ko deactivate karta hai."""
        session = self.get_session()
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            result = await session.execute(
                select(User).filter(User.last_active < cutoff_date, User.is_active == True)
            )
            users_to_update = result.scalars().all()
            for user in users_to_update:
                user.is_active = False
            await session.commit()
            return len(users_to_update)
        finally: await session.close()

    async def add_movie(self, imdb_id, title, year, file_id, message_id, channel_id):
        """Nayi movie ko database mein jodta hai."""
        session = self.get_session()
        try:
            clean_title = clean_text_for_search(title)
            new_movie = Movie(
                imdb_id=imdb_id, title=title, clean_title=clean_title, year=year, 
                file_id=file_id, message_id=message_id, channel_id=channel_id,
            )
            session.add(new_movie)
            await session.commit()
            return True
        except Exception as e:
            logger.error(f"Movie add karne mein error: {e}")
            await session.rollback()
            return False
        finally: await session.close()
    
    async def get_movie_count(self):
        """Database mein total movies ki sankhaya count karta hai."""
        session = self.get_session()
        try: 
            result = await session.execute(select(func.count(Movie.id)))
            return result.scalar_one()
        finally: await session.close()

    async def get_movie_by_imdb(self, imdb_id: str):
        """IMDB ID se movie details laata hai."""
        session = self.get_session()
        try:
            result = await session.execute(
                select(Movie).filter(Movie.imdb_id == imdb_id)
            )
            movie = result.scalar_one_or_none()
            if movie:
                # Sirf zaroori attributes return karein
                return {
                    'imdb_id': movie.imdb_id,
                    'title': movie.title,
                    'year': movie.year,
                    'file_id': movie.file_id,
                    'channel_id': movie.channel_id,
                    'message_id': movie.message_id,
                }
            return None
        finally: await session.close()

    async def super_search_movies(self, query: str, limit: int = 20):
        """Fuzzy matching ke saath advanced movie search karta hai."""
        session = self.get_session()
        try:
            search_pattern = '%' + '%'.join(query.split()) + '%'
            
            # 30k movies ke liye, initial fetch ko 50 tak hi seemit rakhein (Crash prevention)
            result = await session.execute(
                select(Movie.imdb_id, Movie.title)
                .filter(Movie.clean_title.ilike(search_pattern))
                .limit(50) 
            )
            potential_matches = result.all()
            
            if not potential_matches:
                return []
            
            choices = {title: imdb_id for imdb_id, title in potential_matches}
            results = process.extract(query, choices.keys(), limit=limit)
            
            final_list = []
            for title, score in results:
                # Score 65 ya usse zyada (High quality match)
                if score > 65:  
                    imdb_id = choices[title] 
                    final_list.append({'imdb_id': imdb_id, 'title': title})
            return final_list
        except Exception as e:
            logger.error(f"Super search mein error: {e}")
            return []
        finally:
            await session.close()
