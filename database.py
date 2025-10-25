import logging
import re
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Any

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, BigInteger, String, DateTime, Boolean, Integer, func, select, or_, text, delete
from sqlalchemy.exc import OperationalError, DisconnectionError

from thefuzz import fuzz

logger = logging.getLogger(__name__)
Base = declarative_base()

AUTO_MESSAGE_ID_PLACEHOLDER = 9090909090 

def clean_text_for_search(text: str) -> str:
    """Removes special characters and common words for cleaner database search."""
    text = text.lower()
    # [FIXED] Regex ko punctuation hatane ke liye update kiya gaya hai
    # Purana code: [^a-z0-9s]+ (yeh 's' ko chhod kar sab non-alphanumeric remove karta tha)
    text = re.sub(r'[^a-z0-9]+', ' ', text)  # Punctuation ko space se replace karein
    
    # [FIXED] 's*' ko '\s*' (whitespace) se replace kiya gaya hai
    # [FIXED] 's|season' ke baad space compulsory nahi hai (\s* = zero or more space)
    text = re.sub(r'\b(s|season)\s*\d{1,2}\b', '', text) # 'season 1', 's01' etc. hatayein.
    
    # [FIXED] 's+' ko '\s+' (whitespace) se replace kiya gaya hai
    text = re.sub(r'\s+', ' ', text) # Ek se zyada space ko ek space banayein
    return text.strip()

def _normalize_for_fuzzy(text: str) -> str:
    """Normalizes text for better fuzzy matching (phonetic similarities)."""
    t = text.lower()
    # [FIXED] Regex ko punctuation hatane ke liye update kiya gaya hai
    # Purana code: [^a-z0-9s]
    t = re.sub(r'[^a-z0-9]', ' ', t) # Sirf alphanumeric rakhein
    
    # [FIXED] 's+' ko '\s+' (whitespace) se replace kiya gaya hai
    t = re.sub(r'\s+', ' ', t).strip() # Ek se zyada space ko ek space banayein
    
    t = t.replace('ph', 'f').replace('aa', 'a').replace('kh', 'k').replace('gh', 'g')
    t = t.replace('ck', 'k').replace('cq', 'k').replace('qu', 'k').replace('q', 'k')
    t = t.replace('x', 'ks').replace('c', 'k')
    return t

def _consonant_signature(text: str) -> str:
    """Extracts only consonants to detect missing vowels (ktra -> kntr)."""
    t = _normalize_for_fuzzy(text)
    t = re.sub(r'[aeiou]', '', t)
    
    # [FIXED] 's+' ko '\s+' (whitespace) se replace kiya gaya hai
    t = re.sub(r'\s+', '', t) # Saare space hatayein
    return t

def _process_fuzzy_candidates(candidates: List[Tuple[str, str, str]], query: str) -> List[Dict]:
    """
    OPTIMIZED: Advanced fuzzy matching with early exit and pre-filtering for Free Tier CPU efficiency.
    """
    # OPTIMIZATION: Limit candidate pool to prevent CPU exhaustion
    if len(candidates) > 100:
        candidates = candidates[:100]
    
    q_clean = clean_text_for_search(query)
    q_cons = _consonant_signature(query)
    tokens = q_clean.split()
    
    results = []
    for imdb_id, title, clean_title in candidates:
        # OPTIMIZATION: Fast pre-filter before expensive fuzzy operations
        if not any(t in clean_title for t in tokens if t):
            continue  # Skip if no tokens match
        
        s_w_ratio = fuzz.WRatio(clean_title, q_clean)
        
        # OPTIMIZATION: Early exit for low scores (saves ~40% CPU)
        if s_w_ratio < 40:
            continue
        
        s_token_set = fuzz.token_set_ratio(title, query)
        s_token_sort = fuzz.token_sort_ratio(title, query) 
        s_partial = fuzz.partial_ratio(clean_title, q_clean)
        s_consonant_partial = fuzz.partial_ratio(_consonant_signature(title), q_cons)
        score = max(s_w_ratio, s_token_set, s_token_sort, s_partial, s_consonant_partial)
        
        if all(t in clean_title for t in tokens if t):
            score = min(100, score + 3)
        
        results.append((score, imdb_id, title))

    results.sort(key=lambda x: (-x[0], x[2]))
    
    final = [{'imdb_id': imdb, 'title': t} for (sc, imdb, t) in results if sc >= 50][:20]
    return final

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
    clean_title = Column(String, nullable=False, index=True)
    year = Column(String(10), nullable=True)
    file_id = Column(String, nullable=False)
    channel_id = Column(BigInteger, nullable=False)
    message_id = Column(BigInteger, nullable=False)
    added_date = Column(DateTime, default=datetime.utcnow)


class Database:
    def __init__(self, database_url: str):
        connect_args = {}
        
        if database_url.startswith('postgres'):
            if database_url.startswith('postgres://'):
                database_url = database_url.replace('postgres://', 'postgresql+asyncpg://', 1)
            elif database_url.startswith('postgresql://'):
                database_url = database_url.replace('postgresql://', 'postgresql+asyncpg://', 1)
                
            # Yeh logic Neon aur Supabase dono ke liye kaam karega
            if 'sslmode=require' in database_url or 'sslmode=required' in database_url:
                connect_args['ssl'] = 'require'
                database_url = database_url.split('?')[0]
        
        # Agar Supabase URL mein sslmode nahi bhi hai, asyncpg default (prefer) use karega, jo Supabase ke liye sahi hai.
        self.database_url = database_url 
        
        # ============ OPTIMIZED FOR FREE TIER ============
        self.engine = create_async_engine(
            database_url, 
            echo=False, 
            connect_args=connect_args,
            pool_size=5,          # Reduced from 30 (Free Tier optimization)
            max_overflow=10,      # Reduced from 60 (Total max = 15 connections)
            pool_pre_ping=True,   # Validates connections before use
            pool_recycle=300,     # Increased from 60 (connections live 5 minutes)
            pool_timeout=8,       # Reduced from 10 (fail faster)
        )
        
        self.SessionLocal = sessionmaker(
            self.engine, 
            expire_on_commit=False, 
            class_=AsyncSession
        )
        logger.info("Database engine initialized with FREE-TIER-OPTIMIZED pooling: pool_size=5, max_overflow=10.")
        
    async def _handle_db_error(self, e: Exception) -> bool:
        """Attempts to handle operational errors by disposing and recreating the engine."""
        if isinstance(e, (OperationalError, DisconnectionError)):
            logger.error(f"Critical DB error detected: {type(e).__name__}. Attempting engine re-initialization.", exc_info=True)
            try:
                await self.engine.dispose()
                self.engine = create_async_engine(
                    self.database_url,
                    echo=False,
                    pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=300, pool_timeout=8,
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
                    
                    if self.engine.dialect.name == 'postgresql':
                        try:
                            # FIX: Added 'r' to make it a raw string
                            check_query = text(
                                r"""
                                SELECT 1
                                FROM information_schema.columns 
                                WHERE table_name='movies' AND column_name='clean_title';
                                """
                            )
                            result = await conn.execute(check_query)
                            column_exists = result.scalar_one_or_none()
                            
                            if not column_exists:
                                logger.warning("Applying manual migration: Adding 'clean_title' column.")
                                await conn.execute(text("ALTER TABLE movies ADD COLUMN clean_title VARCHAR"))
                                
                                # [FIXED] SQL Query ko naye clean_text_for_search logic se match kiya gaya hai
                                # FIX: Added 'r' to make it a raw string
                                update_query = text(r"""
                                    UPDATE movies 
                                    SET clean_title = trim(
                                        regexp_replace( -- Collapse spaces
                                            regexp_replace( -- Remove seasons
                                                regexp_replace( -- Punctuation to space
                                                    lower(title), 
                                                '[^a-z0-9]+', ' ', 'g'),
                                            '\y(s|season)\s*\d{1,2}\y', '', 'g'),
                                        '\s+', ' ', 'g')
                                    )
                                    WHERE clean_title IS NULL OR clean_title = ''
                                """)
                                await conn.execute(update_query)
                                await conn.execute(text("ALTER TABLE movies ALTER COLUMN clean_title SET NOT NULL"))
                                await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_movies_clean_title ON movies (clean_title)"))
                                await conn.commit()
                                logger.info("Manual migration completed.")
                        except Exception as e:
                            logger.error(f"Migration check failed: {e}")
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
                if session:
                    await session.rollback()
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"add_user error: {e}", exc_info=True)
                return

    async def get_concurrent_user_count(self, minutes: int) -> int:
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
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"get_concurrent_user_count error: {e}", exc_info=True)
                return 0

    async def get_user_count(self) -> int:
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.SessionLocal() as session:
                    result = await session.execute(select(func.count(User.user_id)).where(User.is_active == True))
                    return result.scalar_one()
            except Exception as e:
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"get_user_count error: {e}", exc_info=True)
                return 0

    async def get_movie_count(self) -> int:
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.SessionLocal() as session:
                    result = await session.execute(select(func.count(Movie.id)))
                    return result.scalar_one()
            except Exception as e:
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"get_movie_count error: {e}", exc_info=True)
                return 0

    async def get_movie_by_imdb(self, imdb_id: str) -> Dict:
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.SessionLocal() as session:
                    result = await session.execute(select(Movie).filter(Movie.imdb_id == imdb_id))
                    movie = result.scalar_one_or_none()
                    if movie:
                        return {
                            'imdb_id': movie.imdb_id,
                            'title': movie.title,
                            'year': movie.year,
                            'file_id': movie.file_id,
                            'channel_id': movie.channel_id,
                            'message_id': movie.message_id,
                        }
                    return None
            except Exception as e:
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"get_movie_by_imdb error: {e}", exc_info=True)
                return None

    async def super_search_movies_advanced(self, query: str, limit: int = 20) -> List[Dict]:
        """OPTIMIZED: Multi-stage search with early exit and candidate limiting."""
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.SessionLocal() as session:
                    q_clean = clean_text_for_search(query)
                    tokens = q_clean.split()
                    
                    # Stage 1: Exact match
                    exact_stmt = select(Movie).where(Movie.clean_title == q_clean).limit(5)
                    exact_result = await session.execute(exact_stmt)
                    exact_matches = exact_result.scalars().all()
                    if exact_matches:
                        return [{'imdb_id': m.imdb_id, 'title': m.title} for m in exact_matches[:limit]]
                    
                    # Stage 2: SQL partial match (OPTIMIZED: Limit to 150 candidates)
                    if tokens:
                        conditions = [Movie.clean_title.contains(token) for token in tokens if token]
                        partial_stmt = select(Movie.imdb_id, Movie.title, Movie.clean_title).where(or_(*conditions)).limit(150)
                        partial_result = await session.execute(partial_stmt)
                        candidates = partial_result.all()
                        
                        if candidates:
                            # Stage 3: Fuzzy matching (runs in ThreadPoolExecutor)
                            loop = asyncio.get_event_loop()
                            # _process_fuzzy_candidates ab fixed logic istemaal karega
                            fuzzy_results = await loop.run_in_executor(None, _process_fuzzy_candidates, candidates, query)
                            if fuzzy_results:
                                return fuzzy_results[:limit]
                    
                    return []
            except Exception as e:
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"super_search_movies_advanced error: {e}", exc_info=True)
                return []

    async def add_movie(self, imdb_id: str, title: str, year: str, file_id: str, message_id: int, channel_id: int):
        max_retries = 2
        for attempt in range(max_retries):
            session = None
            try:
                async with self.SessionLocal() as session:
                    # Yahan fixed 'clean_text_for_search' istemaal hoga
                    clean = clean_text_for_search(title)
                    movie = Movie(
                        imdb_id=imdb_id, title=title, clean_title=clean, year=year,
                        file_id=file_id, message_id=message_id, channel_id=channel_id
                    )
                    session.add(movie)
                    await session.commit()
                    return True
            except Exception as e:
                if session:
                    await session.rollback()
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
                if session:
                    await session.rollback()
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"remove_movie_by_imdb error: {e}", exc_info=True)
                return False

    async def cleanup_inactive_users(self, days: int = 30) -> int:
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
                if session:
                    await session.rollback()
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"cleanup_inactive_users error: {e}", exc_info=True)
                return 0

    async def rebuild_clean_titles(self) -> Tuple[int, int]:
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.SessionLocal() as session:
                    result = await session.execute(select(func.count(Movie.id)))
                    total = result.scalar_one()
                    
                    # [FIXED] SQL Query ko naye clean_text_for_search logic se match kiya gaya hai
                    # FIX: Added 'r' to make it a raw string
                    update_query = text(r"""
                        UPDATE movies 
                        SET clean_title = trim(
                            regexp_replace( -- Collapse spaces
                                regexp_replace( -- Remove seasons
                                    regexp_replace( -- Punctuation to space
                                        lower(title), 
                                    '[^a-z0-9]+', ' ', 'g'),
                                '\y(s|season)\s*\d{1,2}\y', '', 'g'),
                            '\s+', ' ', 'g')
                        )
                        WHERE clean_title IS NULL OR clean_title = '' OR clean_title != trim(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        lower(title), 
                                    '[^a-z0-9]+', ' ', 'g'),
                                '\y(s|season)\s*\d{1,2}\y', '', 'g'),
                            '\s+', ' ', 'g')
                        )
                    """)
                    update_result = await session.execute(update_query)
                    await session.commit()
                    return (update_result.rowcount, total)
            except Exception as e:
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"rebuild_clean_titles error: {e}", exc_info=True)
                return (0, 0)

    async def get_all_users(self) -> List[int]:
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.SessionLocal() as session:
                    result = await session.execute(select(User.user_id).where(User.is_active == True))
                    return [row[0] for row in result.all()]
            except Exception as e:
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"get_all_users error: {e}", exc_info=True)
                return []

    async def export_users(self, limit: int = 2000) -> List[Dict]:
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.SessionLocal() as session:
                    result = await session.execute(select(User).limit(limit))
                    users = result.scalars().all()
                    return [
                        {
                            'user_id': u.user_id,
                            'username': u.username,
                            'first_name': u.first_name,
                            'last_name': u.last_name,
                            'joined_date': u.joined_date.isoformat() if u.joined_date else '',
                            'last_active': u.last_active.isoformat() if u.last_active else '',
                            'is_active': u.is_active,
                        }
                        for u in users
                    ]
            except Exception as e:
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"export_users error: {e}", exc_info=True)
                return []

    async def export_movies(self, limit: int = 2000) -> List[Dict]:
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.SessionLocal() as session:
                    result = await session.execute(select(Movie).limit(limit))
                    movies = result.scalars().all()
                    return [
                        {
                            'imdb_id': m.imdb_id,
                            'title': m.title,
                            'year': m.year,
                            'channel_id': m.channel_id,
                            'message_id': m.message_id,
                            'added_date': m.added_date.isoformat() if m.added_date else '',
                        }
                        for m in movies
                    ]
            except Exception as e:
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"export_movies error: {e}", exc_info=True)
                return []
