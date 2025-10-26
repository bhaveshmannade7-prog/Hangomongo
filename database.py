import logging
import re
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Any, Set

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, BigInteger, String, DateTime, Boolean, Integer, func, select, or_, and_, text, delete, update, UniqueConstraint
from sqlalchemy.exc import OperationalError, DisconnectionError, IntegrityError

from thefuzz import fuzz

logger = logging.getLogger("database") # Logger ka naam file ke hisaab se rakha
Base = declarative_base()

# Yeh placeholder ID JSON se import ki gayi files ke liye hai
AUTO_MESSAGE_ID_PLACEHOLDER = 9090909090 

def clean_text_for_search(text: str) -> str:
    """Search ke liye text ko saaf karne wala function."""
    if not text:
        return ""
    text = text.lower()
    # Sirf alphanumeric characters rakhein
    text = re.sub(r'[^a-z0-9]+', ' ', text)
    # 'season 1', 's01' jaise shabdon ko hatayein
    text = re.sub(r'\b(s|season)\s*\d{1,2}\b', '', text)
    # Extra spaces hatayein
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def _normalize_for_fuzzy(text: str) -> str:
    """Fuzzy matching ke liye text ko normalize karein."""
    t = text.lower()
    t = re.sub(r'[^a-z0-9]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    # Aam spelling mistakes ko normalize karein
    t = t.replace('ph', 'f').replace('aa', 'a').replace('kh', 'k').replace('gh', 'g')
    t = t.replace('ck', 'k').replace('cq', 'k').replace('qu', 'k').replace('q', 'k')
    t = t.replace('x', 'ks').replace('c', 'k')
    return t

def _consonant_signature(text: str) -> str:
    """Sirf consonants (vyangan) ka signature banayein (e.g., 'kantara' -> 'kntr')."""
    t = _normalize_for_fuzzy(text)
    t = re.sub(r'[aeiou]', '', t)
    t = re.sub(r'\s+', '', t)
    return t

def _process_fuzzy_candidates(candidates: List[Tuple[str, str, str]], query: str) -> List[Dict]:
    """
    Search se mile candidates par fuzzy matching apply karein.
    Candidates (imdb_id, title, clean_title) ka tuple hai.
    """
    if not candidates:
        return []
        
    # Bahut zyada candidates hone par performance hit hota hai, limit karein
    if len(candidates) > 100:
        candidates = candidates[:100]
    
    q_clean = clean_text_for_search(query)
    q_cons = _consonant_signature(query)
    tokens = q_clean.split()
    
    results = []
    for imdb_id, title, clean_title in candidates:
        if not clean_title: # Agar clean_title empty hai toh skip karein
            continue
            
        # Token-based check (zaroori nahi, par score badha sakta hai)
        # if not any(t in clean_title for t in tokens if t):
        #     continue
        
        # Alag-alag fuzzy scoring techniques
        s_w_ratio = fuzz.WRatio(clean_title, q_clean)
        
        # Kam score waalon ko pehle hi hata dein
        if s_w_ratio < 40:
            continue
        
        s_token_set = fuzz.token_set_ratio(title, query)
        s_token_sort = fuzz.token_sort_ratio(title, query) 
        s_partial = fuzz.partial_ratio(clean_title, q_clean)
        s_consonant_partial = fuzz.partial_ratio(_consonant_signature(title), q_cons)
        
        # Sabse achha score chunein
        score = max(s_w_ratio, s_token_set, s_token_sort, s_partial, s_consonant_partial)
        
        # Agar saare query tokens title mein hain, toh score badhayein
        if all(t in clean_title for t in tokens if t):
            score = min(100, score + 3)
        
        results.append((score, imdb_id, title))

    # Score ke hisaab se sort karein (highest score pehle)
    results.sort(key=lambda x: (-x[0], x[2]))
    
    # Sirf 50 se zyada score waale results bhejein
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
    is_active = Column(Boolean, default=True, index=True) # Active users ko filter karne ke liye index
    last_active = Column(DateTime, default=datetime.utcnow, index=True) # Concurrent users ke liye index

class Movie(Base):
    __tablename__ = 'movies'
    id = Column(Integer, primary_key=True, autoincrement=True)
    imdb_id = Column(String(50), unique=True, nullable=False, index=True)
    title = Column(String, nullable=False)
    clean_title = Column(String, nullable=False, index=True) # Search ke liye B-Tree index
    year = Column(String(10), nullable=True)
    file_id = Column(String, nullable=False, index=True) 
    channel_id = Column(BigInteger, nullable=False)
    message_id = Column(BigInteger, nullable=False)
    added_date = Column(DateTime, default=datetime.utcnow)
    
    # file_id par UNIQUE constraint taaki duplicates na ban sakein
    __table_args__ = (UniqueConstraint('file_id', name='uq_file_id'),)


class Database:
    def __init__(self, database_url: str):
        connect_args = {}
        
        # Supabase/Render/Cloud DBs ke liye SSL settings ko force karein
        if '.com' in database_url or '.co' in database_url:
             connect_args['ssl'] = 'require'
             logger.info("External database URL (e.g., .com/.co) detected, setting ssl='require'.")
        else:
             logger.info("Internal database URL detected, using default SSL (none).")
        
        # SQLAlchemy 1.4+ asyncpg ke liye URL format
        if database_url.startswith('postgresql://'):
             database_url_mod = database_url.replace('postgresql://', 'postgresql+asyncpg://', 1)
        elif database_url.startswith('postgres://'):
            # Heroku/Render ka purana format
            database_url_mod = database_url.replace('postgres://', 'postgresql+asyncpg://', 1)
        else:
            database_url_mod = database_url 

        self.database_url = database_url_mod
        
        # Engine settings free tier ke liye optimized
        self.engine = create_async_engine(
            self.database_url, 
            echo=False, # Production mein False rakhein
            connect_args=connect_args,
            pool_size=5,            # Free tier par kam connections
            max_overflow=10,        # Thoda buffer
            pool_pre_ping=True,     # Connection use karne se pehle check karein (zaroori hai)
            pool_recycle=300,       # Har 5 min mein connection recycle karein
            pool_timeout=8,         # Connection ke liye 8 sec wait karein
        )
        
        self.SessionLocal = sessionmaker(
            self.engine, 
            expire_on_commit=False, 
            class_=AsyncSession
        )
        logger.info(f"Database engine initialized (SSL: {connect_args.get('ssl', 'default')}) with pooling: pool_size=5, max_overflow=10.")
        
    async def _handle_db_error(self, e: Exception) -> bool:
        """Connection errors ko handle karein (experimental)."""
        if isinstance(e, (OperationalError, DisconnectionError)):
            logger.error(f"Critical DB error detected: {type(e).__name__}. Attempting engine re-initialization.", exc_info=True)
            try:
                # Engine ko dispose karke naya banayein
                await self.engine.dispose()
                connect_args = {}
                if '.com' in self.database_url or '.co' in self.database_url:
                    connect_args['ssl'] = 'require'

                self.engine = create_async_engine(
                    self.database_url,
                    echo=False,
                    connect_args=connect_args,
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
        """Database table banayein."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with self.engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                logger.info("Database tables initialized successfully.")
                return
            except Exception as e:
                logger.critical(f"Failed to initialize DB (attempt {attempt+1}/{max_retries}).", exc_info=True)
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt) # Exponential backoff
                    continue
                if attempt == max_retries - 1:
                    raise # Aakhri try fail hone par raise karein
    
    async def add_user(self, user_id, username, first_name, last_name):
        """User ko add ya update karein."""
        max_retries = 2
        for attempt in range(max_retries):
            session = None
            try:
                async with self.SessionLocal() as session:
                    # UPSERT (Insert or Update) logic
                    # Pehle user ko select karein
                    result = await session.execute(select(User).filter(User.user_id == user_id))
                    user = result.scalar_one_or_none()
                    
                    if user:
                        # User hai toh update karein
                        user.last_active = datetime.utcnow()
                        user.is_active = True
                        user.username = username
                        user.first_name = first_name
                        user.last_name = last_name
                    else:
                        # User nahi hai toh add karein
                        session.add(User(user_id=user_id, username=username, first_name=first_name, last_name=last_name))
                    
                    await session.commit()
                    return
            except Exception as e:
                if session:
                    await session.rollback()
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"add_user error for {user_id}: {e}", exc_info=False)
                return

    async def deactivate_user(self, user_id: int):
        """Broadcast fail hone par user ko inactive karein."""
        max_retries = 2
        for attempt in range(max_retries):
            session = None
            try:
                async with self.SessionLocal() as session:
                    await session.execute(
                        update(User)
                        .where(User.user_id == user_id)
                        .values(is_active=False)
                    )
                    await session.commit()
                    logger.info(f"Deactivated user {user_id} (bot blocked).")
                    return
            except Exception as e:
                if session:
                    await session.rollback()
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"deactivate_user error for {user_id}: {e}", exc_info=False)
                return

    async def get_concurrent_user_count(self, minutes: int) -> int:
        """Active users ki ginti karein."""
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.SessionLocal() as session:
                    cutoff = datetime.utcnow() - timedelta(minutes=minutes)
                    # Index (last_active, is_active) ka istemaal karega
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
        """Total active users ki ginti karein."""
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
        """Total movies ki ginti karein."""
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
        """IMDB ID se movie search karein (bahut tez)."""
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.SessionLocal() as session:
                    # Index (imdb_id) ka istemaal karega
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

    # --- YEH FUNCTION AAPKI SLOWNESS KO FIX KAREGA ---
    async def super_search_movies_advanced(self, query: str, limit: int = 20) -> List[Dict]:
        """Optimized multi-stage search (Index-friendly)."""
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.SessionLocal() as session:
                    q_clean = clean_text_for_search(query)
                    tokens = q_clean.split()
                    
                    if not tokens:
                        return []

                    candidates: List[Tuple[str, str, str]] = []
                    found_ids: Set[str] = set()

                    # 1. Exact Match (Sabse tez)
                    exact_stmt = select(Movie.imdb_id, Movie.title, Movie.clean_title).where(Movie.clean_title == q_clean).limit(5)
                    exact_result = await session.execute(exact_stmt)
                    exact_matches = exact_result.all()
                    if exact_matches:
                        # Agar exact match mila, toh seedha return karein
                        return [{'imdb_id': m[0], 'title': m[1]} for m in exact_matches[:limit]]
                    
                    # 2. All Tokens Prefix Match (Tez, B-Tree index use karega)
                    # e.g., "dark knight 2008" -> ... WHERE clean_title LIKE 'dark%' AND clean_title LIKE 'knight%' AND clean_title LIKE '2008%'
                    prefix_conditions_all = [Movie.clean_title.like(f"{token}%") for token in tokens]
                    prefix_stmt_all = select(Movie.imdb_id, Movie.title, Movie.clean_title).where(and_(*prefix_conditions_all)).limit(limit)
                    
                    prefix_result_all = await session.execute(prefix_stmt_all)
                    for row in prefix_result_all.all():
                        if row[0] not in found_ids:
                            candidates.append(row)
                            found_ids.add(row[0])
                    
                    # 3. Any Token Prefix Match (Thoda slow, par index use karega)
                    # e.g., "knight dark" -> ... WHERE clean_title LIKE 'knight%' OR clean_title LIKE 'dark%'
                    if len(candidates) < limit:
                        prefix_conditions_any = [Movie.clean_title.like(f"{token}%") for token in tokens]
                        # Yahaan zyada results (100) layein taaki fuzzy matching ke liye candidates ho
                        prefix_stmt_any = select(Movie.imdb_id, Movie.title, Movie.clean_title).where(or_(*prefix_conditions_any)).limit(100)
                        
                        prefix_result_any = await session.execute(prefix_stmt_any)
                        
                        for row in prefix_result_any.all():
                            if row[0] not in found_ids:
                                candidates.append(row)
                                found_ids.add(row[0])

                    # 4. Fallback: Agar upar kuch nahi mila, toh pehle token se 'contains' (slow) try karein
                    # Yeh free tier par risky ho sakta hai agar DB bada hai
                    if not candidates and tokens:
                        first_token = tokens[0]
                        if len(first_token) > 2: # Chhote tokens (e.g., 'a', 'to') ko contains mein na dhoondein
                            logger.warning(f"Using slow 'contains' fallback for query: {query}")
                            fallback_stmt = select(Movie.imdb_id, Movie.title, Movie.clean_title).where(Movie.clean_title.contains(first_token)).limit(100)
                            fallback_result = await session.execute(fallback_stmt)
                            for row in fallback_result.all():
                                if row[0] not in found_ids:
                                    candidates.append(row)
                                    found_ids.add(row[0])

                    # 5. Fuzzy Match (Ab yeh sirf chote, filtered list par chalega)
                    if candidates:
                        loop = asyncio.get_event_loop()
                        fuzzy_results = await loop.run_in_executor(None, _process_fuzzy_candidates, candidates, query)
                        if fuzzy_results:
                            return fuzzy_results[:limit]
                    
                    return [] # Kuch nahi mila
            except Exception as e:
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"super_search_movies_advanced error: {e}", exc_info=True)
                return []
        return [] # Max retries fail

    async def add_movie(self, imdb_id: str, title: str, year: str, file_id: str, message_id: int, channel_id: int):
        """Movie add karein, duplicates ko skip karein."""
        max_retries = 2
        for attempt in range(max_retries):
            session = None
            try:
                async with self.SessionLocal() as session:
                    clean = clean_text_for_search(title)
                    movie = Movie(
                        imdb_id=imdb_id, title=title, clean_title=clean, year=year,
                        file_id=file_id, message_id=message_id, channel_id=channel_id
                    )
                    session.add(movie)
                    await session.commit()
                    return True 
            except IntegrityError as e:
                # Yeh error tab aata hai jab imdb_id ya file_id (unique constraints) duplicate ho
                if session:
                    await session.rollback()
                logger.warning(f"Duplicate entry skipped: {title} (IMDB: {imdb_id} or FileID: {file_id}). Error: {e.orig}")
                return "duplicate" 
            except Exception as e:
                if session:
                    await session.rollback()
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"add_movie error: {e}", exc_info=True)
                return False
        return False

    async def remove_movie_by_imdb(self, imdb_id: str):
        """Dead file ko remove karein."""
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
        """Inactive users ko 'is_active = False' set karein."""
        max_retries = 2
        for attempt in range(max_retries):
            session = None
            try:
                async with self.SessionLocal() as session:
                    cutoff = datetime.utcnow() - timedelta(days=days)
                    
                    # Pehle count karein kitne users update honge
                    result = await session.execute(
                        select(func.count(User.user_id)).where(User.last_active < cutoff, User.is_active == True)
                    )
                    count = result.scalar_one()
                    
                    if count > 0:
                        # Ab update karein
                        await session.execute(
                            update(User)
                            .where(User.last_active < cutoff, User.is_active == True)
                            .values(is_active=False)
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
        """Sabhi movies ke liye 'clean_title' ko dobara banayein."""
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.SessionLocal() as session:
                    # Total movies count
                    result = await session.execute(select(func.count(Movie.id)))
                    total = result.scalar_one()
                    
                    # Un-cleaned ya galat-cleaned titles ko select karein
                    update_stmt = (
                        update(Movie)
                        .where(
                            or_(
                                Movie.clean_title == None,
                                Movie.clean_title == "",
                                # Agar logic badal gaya hai toh purane ko bhi update karein
                                Movie.clean_title != func.trim(
                                    func.regexp_replace(
                                        func.regexp_replace(
                                            func.regexp_replace(func.lower(Movie.title), r'[^a-z0-9]+', ' ', 'g'),
                                            r'\m(s|season)\s*\d{1,2}\b', '', 'g'
                                        ),
                                        r'\s+', ' ', 'g'
                                    )
                                )
                            )
                        )
                        .values(
                            clean_title=func.trim(
                                func.regexp_replace(
                                    func.regexp_replace(
                                        func.regexp_replace(func.lower(Movie.title), r'[^a-z0-9]+', ' ', 'g'),
                                        r'\m(s|season)\s*\d{1,2}\b', '', 'g'
                                    ),
                                    r'\s+', ' ', 'g'
                                )
                            )
                        )
                    )
                    
                    update_result = await session.execute(update_stmt)
                    await session.commit()
                    return (update_result.rowcount, total)
            except Exception as e:
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                logger.error(f"rebuild_clean_titles error: {e}", exc_info=True)
                return (0, 0)

    async def get_all_users(self) -> List[int]:
        """Broadcast ke liye sabhi active users ki ID lein."""
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
        """Users ko export karein."""
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
        """Movies ko export karein."""
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
