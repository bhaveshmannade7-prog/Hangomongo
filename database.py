import logging
import re
import asyncio
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, BigInteger, String, DateTime, Boolean, Integer, func, select, or_, text 

from thefuzz import fuzz

logger = logging.getLogger(__name__)
Base = declarative_base()

# FIX: JSON Import kiye gaye movies ke liye chota aur safe placeholder message ID constant.
AUTO_MESSAGE_ID_PLACEHOLDER = 9090909090 

def clean_text_for_search(text: str) -> str:
    """Removes special characters and common words for cleaner database search."""
    text = text.lower()
    text = re.sub(r'[^a-z0-9s]+', ' ', text)
    text = re.sub(r's+', ' ', text)
    # Season/Sxx jaise shabdon ko hatao taaki movie titles saaf rahein
    text = re.sub(r'\b(s|season)s*d{1,2}\b', '', text) 
    return text.strip()

def _normalize_for_fuzzy(text: str) -> str:
    """Normalizes text for better fuzzy matching (e.g., phonetic similarities)."""
    t = text.lower()
    t = re.sub(r'[^a-z0-9s]', ' ', t)
    t = re.sub(r's+', ' ', t).strip()
    # Hinglish phonetic normalization
    t = t.replace('ph', 'f').replace('aa', 'a').replace('kh', 'k').replace('gh', 'g')
    t = t.replace('ck', 'k').replace('cq', 'k').replace('qu', 'k').replace('q', 'k')
    t = t.replace('x', 'ks').replace('c', 'k')
    return t

def _consonant_signature(text: str) -> str:
    """Extracts only consonants to detect missing vowels (ktra -> kntr)."""
    t = _normalize_for_fuzzy(text)
    # Sirf consonants rakho
    t = re.sub(r'[aeiou]', '', t)
    t = re.sub(r's+', '', t)
    return t

# --- HELPER FUNCTION TO TRANSFORM DATA (FOR JSON IMPORT) ---
def generate_auto_info(movie_data: Dict, channel_id: int) -> Dict | None:
    """
    Generates required missing fields and maps input data to the Movie model structure.
    Uses placeholder message_id for JSON imports.
    """
    
    # 1. Map Title (title ya name/movie_name me se koi ek hona chahiye)
    title = movie_data.get("title") or movie_data.get("name") or movie_data.get("movie_name")

    # 2. Map File ID (file_id ya file_ref/media_id me se koi ek hona chahiye)
    file_id = movie_data.get("file_id") or movie_data.get("file_ref") or movie_data.get("media_id")

    # CRITICAL CHECK: Sirf title aur file_id chahiye
    if not title or not file_id:
        logger.warning(f"Skipping import: Title or File ID missing after mapping attempts: {movie_data}")
        return None

    # 3. IMDB ID (Agar original IMDB ID nahi hai to hash use karo)
    imdb_id = movie_data.get("imdb_id") 
    if not imdb_id:
        hash_object = hashlib.sha1(f"{title}{file_id}".encode('utf-8'))
        imdb_id = f"auto_{hash_object.hexdigest()[:15]}" 

    # 4. Year (Attempt to extract year from title, ya JSON se lo)
    year = movie_data.get("year")
    if not year:
        year_match = re.search(r'\b(19|20)\d{2}\b', title)
        year = year_match.group(0) if year_match else None

    # 5. Message ID (Hamesha placeholder use karo JSON imported files ke liye)
    auto_message_id = AUTO_MESSAGE_ID_PLACEHOLDER  

    return {
        "imdb_id": imdb_id,
        "title": title,
        "year": year,
        "file_id": file_id,
        "message_id": auto_message_id,
        "channel_id": channel_id,
    }

# --- Synchronous Helper Function for CPU-Bound Logic ---
def _process_fuzzy_candidates(candidates: List[Tuple[str, str, str]], query: str) -> List[Dict]:
    """
    Advanced fuzzy matching logic to handle spelling mistakes, typos, and word order issues aggressively.
    Runs in a separate thread for performance.
    """
    q_clean = clean_text_for_search(query)
    q_cons = _consonant_signature(query)
    tokens = q_clean.split()
    
    results = []
    for imdb_id, title, clean_title in candidates:
        # 1. Standard Ratios
        s_w_ratio = fuzz.WRatio(clean_title, q_clean)
        s_token_set = fuzz.token_set_ratio(title, query)
        s_token_sort = fuzz.token_sort_ratio(title, query) 
        
        # 2. Aggressive Partial/Substring match (crucial for short queries like 'ktra' or 'mirz')
        s_partial = fuzz.partial_ratio(clean_title, q_clean)
        
        # 3. Phonetic match 
        s_consonant_partial = fuzz.partial_ratio(_consonant_signature(title), q_cons)

        # Final Score: Maximum of all robust matching methods is taken to ensure the best possible match is used.
        score = max(
            s_w_ratio, 
            s_token_set, 
            s_token_sort,
            s_partial,
            s_consonant_partial
        )
        
        # Small boost if all query tokens are present 
        if all(t in clean_title for t in tokens if t):
            score = min(100, score + 3)
        
        results.append((score, imdb_id, title))

    results.sort(key=lambda x: (-x[0], x[2]))
    
    # Filtering score 
    final = [{'imdb_id': imdb, 'title': t} for (sc, imdb, t) in results if sc >= 50][:20]
    return final

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
        
        # CRITICAL FIX 1: Connection Stability for Render/Neon PostgreSQL
        if database_url.startswith('postgres'):
            if database_url.startswith('postgres://'):
                database_url = database_url.replace('postgres://', 'postgresql+asyncpg://', 1)
            elif database_url.startswith('postgresql://'):
                database_url = database_url.replace('postgresql://', 'postgresql+asyncpg://', 1)
                
            # Neon requires SSL set as 'require'
            if 'sslmode=require' in database_url or 'sslmode=required' in database_url:
                connect_args['ssl'] = 'require'
                database_url = database_url.split('?')[0] # Remove query params

        self.engine = create_async_engine(
            database_url, 
            echo=False, 
            connect_args=connect_args,
            # HIGH-RESILIENCE settings for Render/Neon Free Tier (Increased pool size)
            pool_size=20, # FIX: Increased from 10
            max_overflow=40, # FIX: Increased from 20
            pool_pre_ping=True, 
            pool_recycle=180,  # CRITICAL FIX: Reduced from 300s to 180s for better Free Tier resilience
            pool_timeout=15, 
        )
        
        self.SessionLocal = sessionmaker(
            self.engine, 
            expire_on_commit=False, 
            class_=AsyncSession
        )
        logger.info("Database engine initialized with HIGH-RESILIENCE pooling settings.")

    async def init_db(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            
            # Manual migration check for clean_title column (if running on existing DB)
            if self.engine.dialect.name == 'postgresql':
                try:
                    check_query = text(
                        """
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
                        update_query = text("""
                            UPDATE movies 
                            SET clean_title = lower(regexp_replace(title, '[^a-z0-9\s]+', ' ', 'g'))
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

    async def add_user(self, user_id, username, first_name, last_name):
        async with self.SessionLocal() as session:
            try:
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
            except Exception as e:
                await session.rollback()
                logger.error(f"add_user error: {e}", exc_info=True)

    async def get_concurrent_user_count(self, minutes: int = 5):
        async with self.SessionLocal() as session:
            try:
                cutoff_time = datetime.utcnow() - timedelta(minutes=minutes)
                result = await session.execute(
                    select(func.count(User.user_id)).filter(User.last_active >= cutoff_time, User.is_active == True)
                )
                return result.scalar_one()
            except Exception as e:
                logger.error(f"get_concurrent_user_count error: {e}", exc_info=True)
                return 0

    async def get_all_users(self):
        async with self.SessionLocal() as session:
            try:
                result = await session.execute(select(User.user_id).filter(User.is_active == True))
                return result.scalars().all()
            except Exception as e:
                logger.error(f"get_all_users error: {e}", exc_info=True)
                return []

    async def get_user_count(self):
        async with self.SessionLocal() as session:
            try:
                result = await session.execute(select(func.count(User.user_id))) 
                return result.scalar_one()
            except Exception as e:
                logger.error(f"get_user_count error: {e}", exc_info=True)
                return 0

    async def cleanup_inactive_users(self, days: int):
        async with self.SessionLocal() as session:
            try:
                cutoff_date = datetime.utcnow() - timedelta(days=days)
                result = await session.execute(select(User).filter(User.last_active < cutoff_date, User.is_active == True))
                users_to_update = result.scalars().all()
                for u in users_to_update:
                    u.is_active = False
                await session.commit()
                return len(users_to_update)
            except Exception as e:
                await session.rollback()
                logger.error(f"cleanup_inactive_users error: {e}", exc_info=True)
                return 0

    async def add_movie(self, imdb_id, title, year, file_id, message_id, channel_id):
        async with self.SessionLocal() as session:
            try:
                clean_title = clean_text_for_search(title)
                new_movie = Movie(
                    imdb_id=imdb_id, title=title, clean_title=clean_title, year=year,
                    file_id=file_id, message_id=message_id, channel_id=channel_id
                )
                session.add(new_movie)
                await session.commit()
                return True
            except Exception as e:
                logger.error(f"Movie add error: {e}", exc_info=True)
                await session.rollback()
                return False

    async def bulk_add_new_movies(self, movies_data: List[Dict], channel_id: int):
        added_count = 0
        skipped_count = 0
        
        async with self.SessionLocal() as session:
            # FIX: Loop ko efficiency aur robustness ke liye update kiya gaya hai
            for new_movie in movies_data:
                try:
                    transformed_data = generate_auto_info(new_movie, channel_id)
                    
                    if not transformed_data:
                        skipped_count += 1
                        continue 
                        
                    # Existing check (imdb_id already present)
                    result = await session.execute(
                        select(Movie.id).filter(Movie.imdb_id == transformed_data['imdb_id'])
                    )
                    if result.scalar_one_or_none():
                        skipped_count += 1
                        continue 
                        
                    clean_title = clean_text_for_search(transformed_data['title'])
                    
                    new_movie_entry = Movie(
                        imdb_id=transformed_data['imdb_id'], 
                        title=transformed_data['title'], 
                        clean_title=clean_title, 
                        year=transformed_data['year'],
                        file_id=transformed_data['file_id'], 
                        message_id=transformed_data['message_id'], 
                        channel_id=transformed_data['channel_id']
                    )
                    
                    session.add(new_movie_entry)
                    added_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing new movie data: {new_movie}. Error: {e}", exc_info=True)
                    
            if added_count > 0:
                try:
                    await session.commit()
                except Exception as e:
                    await session.rollback()
                    logger.error(f"Bulk commit failed: {e}", exc_info=True)
            
        return added_count, skipped_count


    async def get_movie_count(self):
        async with self.SessionLocal() as session:
            try:
                result = await session.execute(select(func.count(Movie.id)))
                return result.scalar_one()
            except Exception as e:
                logger.error(f"get_movie_count error: {e}", exc_info=True)
                return 0

    async def get_movie_by_imdb(self, imdb_id: str):
        async with self.SessionLocal() as session:
            try:
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
                logger.error(f"get_movie_by_imdb error: {e}", exc_info=True)
                return None

    async def export_users(self, limit: int = 2000) -> List[Dict]:
        async with self.SessionLocal() as session:
            try:
                result = await session.execute(
                    select(User.user_id, User.username, User.first_name, User.last_name, User.joined_date, User.last_active, User.is_active)
                    .limit(limit)
                )
                rows = result.all()
                return [
                    dict(
                        user_id=r[0],
                        username=r[1],
                        first_name=r[2],
                        last_name=r[3],
                        joined_date=r[4],
                        last_active=r[5],
                        is_active=r[6],
                    )
                    for r in rows
                ]
            except Exception as e:
                logger.error(f"export_users error: {e}", exc_info=True)
                return []

    async def export_movies(self, limit: int = 2000) -> List[Dict]:
        async with self.SessionLocal() as session:
            try:
                result = await session.execute(
                    select(Movie.imdb_id, Movie.title, Movie.year, Movie.channel_id, Movie.message_id, Movie.added_date)
                    .limit(limit)
                )
                rows = result.all()
                return [
                    dict(
                        imdb_id=r[0],
                        title=r[1],
                        year=r[2],
                        channel_id=r[3],
                        message_id=r[4],
                        added_date=r[5],
                    )
                    for r in rows
                ]
            except Exception as e:
                logger.error(f"export_movies error: {e}", exc_info=True)
                return []

    async def rebuild_clean_titles(self) -> Tuple[int, int]:
        async with self.SessionLocal() as session:
            updated = 0
            total = 0
            try:
                result = await session.execute(select(func.count(Movie.id)))
                total = result.scalar_one()
                batch = 1000
                offset = 0
                while True:
                    res = await session.execute(select(Movie).limit(batch).offset(offset))
                    rows = res.scalars().all()
                    if not rows:
                        break
                    for m in rows:
                        new_clean = clean_text_for_search(m.title)
                        if m.clean_title != new_clean:
                            m.clean_title = new_clean
                            updated += 1
                    await session.commit()
                    offset += batch
                return updated, total
            except Exception as e:
                logger.error(f"Rebuild index failed: {e}", exc_info=True)
                await session.rollback()
                return 0, total

    async def super_search_movies_advanced(self, query: str, limit: int = 20) -> List[Dict]:
        async with self.SessionLocal() as session:
            try:
                q_clean = clean_text_for_search(query)
                
                # --- Optimized DB Query Filters (FIXED FOR AGGRESSIVE SEARCH) ---
                # Hum DB se zyaada se zyaada titles laayेंगे.
                
                # Query ko individual characters mein todkar wildcard pattern banana (Trigram-like search)
                # 'ktra' -> '%k%t%r%a%' (Most aggressive for missing vowels/letters)
                char_wildcard_pattern = '%' + '%'.join(list(q_clean)) + '%'
                
                db_filters = [
                    # 1. Exact Match and Starts With
                    Movie.clean_title == q_clean,
                    Movie.clean_title.ilike(f"{q_clean}%"),

                    # 2. Contains any substring (essential for 2+ word queries)
                    Movie.clean_title.ilike(f"%{q_clean}%"),
                    
                    # 3. Aggressive Character Search (FIX for 'ktra' to match 'kantara')
                    Movie.clean_title.ilike(char_wildcard_pattern),
                ]

                # 4. Multi-word match
                if len(q_clean.split()) > 1:
                    db_filters.append(
                        Movie.clean_title.ilike('%' + '%'.join(q_clean.split()) + '%')
                    )

                # 5. Phonetic/Vowel Skip Filter (e.g., k_nt_r_)
                if len(q_clean) > 3:
                     # Vowels ko underscore se replace karo aur search karo
                    vowel_skip_pattern = q_clean.replace('a', '_').replace('e', '_').replace('i', '_').replace('o', '_').replace('u', '_')
                    if '_' in vowel_skip_pattern:
                        db_filters.append(
                            Movie.clean_title.ilike(f"%{vowel_skip_pattern}%")
                        )

                
                filt = or_(*db_filters)
                
                # Candidate LIMIT ko 400 tak badha diya hai taaki fuzzy logic ko full chance mile
                res = await session.execute(
                    select(Movie.imdb_id, Movie.title, Movie.clean_title).filter(filt).limit(400)
                )
                candidates = res.all()
                
                if not candidates:
                    return []
                
                # Heavy fuzzy logic ko separate thread mein chalao taaki bot fast rahe
                # FIX: Ab yeh search logic main thread ko block nahi karega.
                final_results = await asyncio.to_thread(
                    _process_fuzzy_candidates, 
                    candidates, 
                    query
                )
                
                return final_results[:limit]
            
            except Exception as e:
                logger.error(f"Advanced search failed: {e}", exc_info=True)
                return []
