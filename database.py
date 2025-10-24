# database.py

import logging
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Any

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, BigInteger, String, DateTime, Boolean, Integer, func, select, or_, text, delete
from sqlalchemy.exc import OperationalError, DisconnectionError

logger = logging.getLogger(__name__)
Base = declarative_base()

AUTO_MESSAGE_ID_PLACEHOLDER = 9090909090 

# ============ DATABASE MODELS ============
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
    # clean_title field removed as Algolia handles search
    year = Column(String(10), nullable=True)
    file_id = Column(String, nullable=False)
    channel_id = Column(BigInteger, nullable=False)
    message_id = Column(BigInteger, nullable=False)
    added_date = Column(DateTime, default=datetime.utcnow)


class Database:
    def __init__(self, database_url: str):
        connect_args = {}
        
        # Connection string processing for asyncpg (Render/PostgreSQL)
        if database_url.startswith('postgres'):
            if database_url.startswith('postgres://'):
                database_url = database_url.replace('postgres://', 'postgresql+asyncpg://', 1)
            elif database_url.startswith('postgresql://'):
                database_url = database_url.replace('postgresql://', 'postgresql+asyncpg://', 1)
                
            if 'sslmode=require' in database_url or 'sslmode=required' in database_url:
                connect_args['ssl'] = 'require'
                database_url = database_url.split('?')[0] # Remove query string if using connect_args
        
        self.database_url = database_url 
        
        # TIGHT POOLING for FREE TIER STABILITY (Max 8 connections)
        self.engine = create_async_engine(
            database_url, 
            echo=False, 
            connect_args=connect_args,
            pool_size=3,          
            max_overflow=5,       
            pool_pre_ping=True,   
            pool_recycle=300,     
            pool_timeout=8,      
        )
        
        self.SessionLocal = sessionmaker(
            self.engine, 
            expire_on_commit=False, 
            class_=AsyncSession
        )
        logger.info("Database engine initialized with TIGHT pooling: pool_size=3, max_overflow=5.")
        
    async def _handle_db_error(self, e: Exception) -> bool:
        """Attempts to handle operational errors by disposing and recreating the engine."""
        if isinstance(e, (OperationalError, DisconnectionError)):
            logger.error(f"Critical DB error detected: {type(e).__name__}. Attempting engine re-initialization.", exc_info=True)
            try:
                await self.engine.dispose()
                # Re-initialize with tight pooling
                self.engine = create_async_engine(
                    self.database_url,
                    echo=False,
                    pool_size=3, max_overflow=5, pool_pre_ping=True, pool_recycle=300, pool_timeout=8,
                )
                self.SessionLocal = sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
                logger.info("DB engine successfully re-initialized.")
                return True
            except Exception as re_e:
                logger.critical(f"Failed to re-initialize DB engine: {re_e}", exc_info=True)
                return False
        return False 
        
    async def init_db(self):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with self.engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                logger.info("Database tables initialized successfully.")
                return
            except Exception as e:
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt) 
                    continue
                logger.critical(f"Failed to initialize DB after {attempt + 1} attempts.", exc_info=True)
                raise 

    async def add_user(self, user_id, username, first_name, last_name):
        max_retries = 2
        for attempt in range(max_retries):
            session = None
            try:
                async with self.SessionLocal() as session:
                    result = await session.execute(select(User).filter(User.user_id == user_id))
                    user = result.scalar_one_or_none()
                    if user:
                        user.last_active = datetime.utcnow()
                        user.is_active = True
                        user.username = username
                        user.first_name = first_name
                        user.last_name = last_name
                    else:
                        session.add(User(user_id=user_id, username=username, first_name=first_name, last_name=last_name))
                    await session.commit()
                    return
            except Exception as e:
                if session: await session.rollback()
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"add_user error: {e}", exc_info=True)
                return

    async def get_movie_by_imdb(self, imdb_id: str) -> Dict:
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.SessionLocal() as session:
                    result = await session.execute(select(Movie).filter(Movie.imdb_id == imdb_id))
                    movie = result.scalar_one_or_none()
                    if movie:
                        return {
                            'imdb_id': movie.imdb_id, 'title': movie.title, 'year': movie.year,
                            'file_id': movie.file_id, 'channel_id': movie.channel_id, 'message_id': movie.message_id,
                        }
                    return None
            except Exception as e:
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"get_movie_by_imdb error: {e}", exc_info=True)
                return None

    async def add_movie(self, imdb_id: str, title: str, year: str, file_id: str, message_id: int, channel_id: int):
        max_retries = 2
        for attempt in range(max_retries):
            session = None
            try:
                async with self.SessionLocal() as session:
                    # clean_title is removed
                    movie = Movie(
                        imdb_id=imdb_id, title=title, year=year,
                        file_id=file_id, message_id=message_id, channel_id=channel_id
                    )
                    session.add(movie)
                    await session.commit()
                    return True
            except Exception as e:
                if session: await session.rollback()
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"add_movie error: {e}", exc_info=True)
                return False

    async def remove_movie_by_imdb(self, imdb_id: str):
        max_retries = 2
        for attempt in range(max_retries):
            session = None
            try:
                async with self.SessionLocal() as session:
                    await session.execute(delete(Movie).where(Movie.imdb_id == imdb_id))
                    await session.commit()
                    return True
            except Exception as e:
                if session: await session.rollback()
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"remove_movie_by_imdb error: {e}", exc_info=True)
                return False
    
    async def get_all_movies_for_indexing(self) -> List[Dict]:
        """Fetch all movie data for initial Algolia indexing."""
        try:
            async with self.SessionLocal() as session:
                # Optimized to fetch only data needed for Algolia
                result = await session.execute(select(Movie.imdb_id, Movie.title, Movie.year, Movie.added_date))
                
                return [
                    {
                        'imdb_id': m[0],
                        'title': m[1],
                        'year': m[2],
                        'added_date': m[3].isoformat() if m[3] else None,
                        'objectID': m[0], # Algolia requires objectID
                    }
                    for m in result.all()
                ]
        except Exception as e:
            logger.error(f"get_all_movies_for_indexing error: {e}")
            return []

    # --- Other functions (get_user_count, get_movie_count, get_all_users, etc.) remain largely unchanged ---
    async def get_user_count(self) -> int:
        # ... (implementation remains) ...
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.SessionLocal() as session:
                    result = await session.execute(select(func.count(User.user_id)).where(User.is_active == True))
                    return result.scalar_one()
            except Exception as e:
                if await self._handle_db_error(e) and attempt < max_retries - 1: await asyncio.sleep(1); continue
                return 0

    async def get_movie_count(self) -> int:
        # ... (implementation remains) ...
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.SessionLocal() as session:
                    result = await session.execute(select(func.count(Movie.id)))
                    return result.scalar_one()
            except Exception as e:
                if await self._handle_db_error(e) and attempt < max_retries - 1: await asyncio.sleep(1); continue
                return 0

    async def get_concurrent_user_count(self, minutes: int) -> int:
        # ... (implementation remains) ...
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.SessionLocal() as session:
                    cutoff = datetime.utcnow() - timedelta(minutes=minutes)
                    result = await session.execute(
                        select(func.count(User.user_id)).where(User.last_active >= cutoff, User.is_active == True)
                    )
                    return result.scalar_one()
            except Exception as e:
                if await self._handle_db_error(e) and attempt < max_retries - 1: await asyncio.sleep(1); continue
                return 0

    async def cleanup_inactive_users(self, days: int = 30) -> int:
        # ... (implementation remains) ...
        max_retries = 2
        for attempt in range(max_retries):
            session = None
            try:
                async with self.SessionLocal() as session:
                    cutoff = datetime.utcnow() - timedelta(days=days)
                    result = await session.execute(
                        select(func.count(User.user_id)).where(User.last_active < cutoff, User.is_active == True)
                    )
                    count = result.scalar_one()
                    await session.execute(
                        text("UPDATE users SET is_active = FALSE WHERE last_active < :cutoff AND is_active = TRUE"),
                        {"cutoff": cutoff}
                    )
                    await session.commit()
                    return count
            except Exception as e:
                if session: await session.rollback()
                if await self._handle_db_error(e) and attempt < max_retries - 1: await asyncio.sleep(1); continue
                return 0

    async def get_all_users(self) -> List[int]:
        # ... (implementation remains) ...
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.SessionLocal() as session:
                    result = await session.execute(select(User.user_id).where(User.is_active == True))
                    return [row[0] for row in result.all()]
            except Exception as e:
                if await self._handle_db_error(e) and attempt < max_retries - 1: await asyncio.sleep(1); continue
                return []
