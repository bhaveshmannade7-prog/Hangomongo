# database.py (Only core functions for User/Movie Storage remain)

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

# Note: clean_text_for_search, fuzzy functions removed/unnecessary

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
        # ... [Unchanged DB initialization logic, but with TIGHTER POOLING] ...
        connect_args = {}
        if database_url.startswith('postgres'):
            if database_url.startswith('postgres://'):
                database_url = database_url.replace('postgres://', 'postgresql+asyncpg://', 1)
            elif database_url.startswith('postgresql://'):
                database_url = database_url.replace('postgresql://', 'postgresql+asyncpg://', 1)
                
            if 'sslmode=require' in database_url or 'sslmode=required' in database_url:
                connect_args['ssl'] = 'require'
                database_url = database_url.split('?')[0]

        self.database_url = database_url 
        
        # TIGHTER POOLING for FREE TIER STABILITY
        self.engine = create_async_engine(
            database_url, 
            echo=False, 
            connect_args=connect_args,
            pool_size=3,          # Reduced for stability
            max_overflow=5,       # Total max = 8 connections
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
        
    async def init_db(self):
        # ... [Unchanged init_db logic. Migration for clean_title is now unnecessary] ...
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with self.engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                logger.info("Database tables initialized successfully.")
                return
            except Exception as e:
                # ... [Handle DB Error logic] ...
                logger.critical(f"Failed to initialize DB after {attempt + 1} attempts.", exc_info=True)
                raise 
        
    # --- CRUD functions (add_user, get_concurrent_user_count, get_movie_by_imdb, etc.) remain largely UNCHANGED
    
    async def add_movie(self, imdb_id: str, title: str, year: str, file_id: str, message_id: int, channel_id: int):
        max_retries = 2
        for attempt in range(max_retries):
            session = None
            try:
                async with self.SessionLocal() as session:
                    # clean_title is no longer required here
                    movie = Movie(
                        imdb_id=imdb_id, title=title, year=year,
                        file_id=file_id, message_id=message_id, channel_id=channel_id
                    )
                    session.add(movie)
                    await session.commit()
                    return True
            except Exception as e:
                # ... [Handle Error] ...
                return False

    async def get_all_movies_for_indexing(self) -> List[Dict]:
        """Fetch all movie data for initial Algolia indexing."""
        try:
            async with self.SessionLocal() as session:
                result = await session.execute(select(Movie))
                movies = result.scalars().all()
                return [
                    {
                        'imdb_id': m.imdb_id,
                        'title': m.title,
                        'year': m.year,
                        'added_date': m.added_date.isoformat(),
                    }
                    for m in movies
                ]
        except Exception as e:
            logger.error(f"get_all_movies_for_indexing error: {e}")
            return []

    # --- Other functions (cleanup_inactive_users, get_user_count, etc.) remain UNCHANGED
