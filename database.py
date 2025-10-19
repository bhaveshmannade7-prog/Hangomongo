import logging
import re
import asyncio
import hashlib # <--- NEW IMPORT
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, BigInteger, String, DateTime, Boolean, Integer, func, select, or_, text 

from thefuzz import fuzz

logger = logging.getLogger(__name__)
Base = declarative_base()

def clean_text_for_search(text: str) -> str:
    text = text.lower()
    text = re.sub(r'[^a-z0-9s]+', ' ', text)
    text = re.sub(r's+', ' ', text)
    text = re.sub(r'\b(s|season)s*d{1,2}\b', '', text) 
    return text.strip()

def _normalize_for_fuzzy(text: str) -> str:
    t = text.lower()
    t = re.sub(r'[^a-z0-9s]', ' ', t)
    t = re.sub(r's+', ' ', t).strip()
    t = t.replace('ph', 'f').replace('aa', 'a').replace('kh', 'k').replace('gh', 'g')
    t = t.replace('ck', 'k').replace('cq', 'k').replace('qu', 'k').replace('q', 'k')
    t = t.replace('x', 'ks').replace('c', 'k')
    return t

def _consonant_signature(text: str) -> str:
    t = _normalize_for_fuzzy(text)
    t = re.sub(r'[aeiou]', '', t)
    t = re.sub(r's+', '', t)
    return t

# --- NEW HELPER FUNCTION TO TRANSFORM DATA ---
def generate_auto_info(movie_data: Dict, channel_id: int) -> Dict:
    """
    Generates required missing fields (imdb_id, year, message_id) for the new structure.
    """
    title = movie_data.get("title", "Unknown Title")
    file_id = movie_data.get("file_id", "NO_FILE_ID")
    
    # 1. IMDB ID (Using a hash of title+file_id to ensure near-uniqueness)
    hash_object = hashlib.sha1(f"{title}{file_id}".encode('utf-8'))
    auto_imdb_id = f"auto_{hash_object.hexdigest()[:15]}" 

    # 2. Year (Attempt to extract year from title)
    year_match = re.search(r'\b(19|20)\d{2}\b', title)
    year = year_match.group(0) if year_match else None

    # 3. Message ID (Using a large placeholder)
    auto_message_id = 9999999999999  

    return {
        "imdb_id": auto_imdb_id,
        "title": title,
        "year": year,
        "file_id": file_id,
        "message_id": auto_message_id,
        "channel_id": channel_id,
    }

# --- New Synchronous Helper Function for CPU-Bound Logic ---
def _process_fuzzy_candidates(candidates: List[Tuple[str, str, str]], query: str) -> List[Dict]:
    """Runs the CPU-intensive fuzzy matching in a separate thread."""
    q_clean = clean_text_for_search(query)
    q_cons = _consonant_signature(query)
    tokens = q_clean.split()
    
    results = []
    for imdb_id, title, clean_title in candidates:
        s1 = fuzz.WRatio(clean_title, q_clean)
        s2 = fuzz.token_set_ratio(title, query)
        s3 = fuzz.partial_ratio(clean_title, q_clean)
        s4 = fuzz.ratio(_consonant_signature(title), q_cons)
        score = max(s1, s2, s3, s4)
        
        # Boost score if all query tokens are present in the clean title
        if all(t in clean_title for t in tokens if t):
            score = min(100, score + 5)
        
        results.append((score, imdb_id, title))

    results.sort(key=lambda x: (-x[0], x[2]))
    # Filter by score and limit
    final = [{'imdb_id': imdb, 'title': t} for (sc, imdb, t) in results if sc >= 55][:20]
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
        # FIX: Added proper SSL handling for Render's PostgreSQL connection
        if 'sslmode=require' in database_url or database_url.startswith('postgres'):
            connect_args['ssl'] = 'require'
            database_url = database_url.replace('?sslmode=require', '').replace('?sslmode=required', '')

        if database_url.startswith('postgresql://'):
            database_url = database_url.replace('postgresql://', 'postgresql+asyncpg://', 1)
        elif database_url.startswith('postgres://'):
            database_url = database_url.replace('postgres://', 'postgresql+asyncpg://', 1)

        self.engine = create_async_engine(
            database_url, 
            echo=False, 
            connect_args=connect_args,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_timeout=30,
        )
        
        self.SessionLocal = sessionmaker(
            self.engine, 
            expire_on_commit=False, 
            class_=AsyncSession
        )
        logger.info("Database engine initialized with proper pooling.")

    async def init_db(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            
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
                        await conn.execute(text("UPDATE movies SET clean_title = lower(regexp_replace(title, '[^a-z0-9\\s]+', ' ', 'g'))"))
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
                else:
                    session.add(User(user_id=user_id, username=username, first_name=first_name, last_name=last_name))
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"add_user error: {e}")

    async def get_concurrent_user_count(self, minutes: int = 5):
        async with self.SessionLocal() as session:
            try:
                cutoff_time = datetime.utcnow() - timedelta(minutes=minutes)
                result = await session.execute(
                    select(func.count(User.user_id)).filter(User.last_active >= cutoff_time, User.is_active == True)
                )
                return result.scalar_one()
            except Exception as e:
                logger.error(f"get_concurrent_user_count error: {e}")
                return 0

    async def get_all_users(self):
        async with self.SessionLocal() as session:
            try:
                result = await session.execute(select(User.user_id).filter(User.is_active == True))
                return result.scalars().all()
            except Exception as e:
                logger.error(f"get_all_users error: {e}")
                return []

    async def get_user_count(self):
        async with self.SessionLocal() as session:
            try:
                result = await session.execute(select(func.count(User.id)))
                return result.scalar_one()
            except Exception as e:
                logger.error(f"get_user_count error: {e}")
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
                logger.error(f"cleanup_inactive_users error: {e}")
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
                logger.error(f"Movie add error: {e}")
                await session.rollback()
                return False

    # --- NEW FUNCTION TO HANDLE NEW JSON STRUCTURE ---
    async def bulk_add_new_movies(self, movies_data: List[Dict], channel_id: int):
        """
        Processes and adds a list of movies from the simple (title, file_id) structure.
        Generates auto fields (imdb_id, message_id, year).
        """
        added_count = 0
        skipped_count = 0
        
        async with self.SessionLocal() as session:
            for new_movie in movies_data:
                try:
                    # Transform the simple data to the required complex structure
                    transformed_data = generate_auto_info(new_movie, channel_id)
                    
                    # Check if an entry with this auto-generated imdb_id already exists
                    result = await session.execute(
                        select(Movie.id).filter(Movie.imdb_id == transformed_data['imdb_id'])
                    )
                    if result.scalar_one_or_none():
                        skipped_count += 1
                        continue # Skip if already exists
                        
                    # Prepare the data for insertion
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
                    logger.error(f"Error processing new movie data: {new_movie}. Error: {e}")
                    # Continue to the next entry even if one fails
            
            # Commit all processed entries at the end of the batch
            if added_count > 0:
                try:
                    await session.commit()
                except Exception as e:
                    await session.rollback()
                    logger.error(f"Bulk commit failed: {e}")
            
        return added_count, skipped_count


    async def get_movie_count(self):
        async with self.SessionLocal() as session:
            try:
                result = await session.execute(select(func.count(Movie.id)))
                return result.scalar_one()
            except Exception as e:
                logger.error(f"get_movie_count error: {e}")
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
                logger.error(f"get_movie_by_imdb error: {e}")
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
                logger.error(f"export_users error: {e}")
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
                logger.error(f"export_movies error: {e}")
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
                logger.error(f"Rebuild index failed: {e}")
                await session.rollback()
                return 0, total

    async def super_search_movies_advanced(self, query: str, limit: int = 20) -> List[Dict]:
        async with self.SessionLocal() as session:
            try:
                q_clean = clean_text_for_search(query)
                q_norm = _normalize_for_fuzzy(query)
                
                # --- Optimized DB Query Filters ---
                db_filters = [
                    # 1. Exact clean title match (fastest)
                    Movie.clean_title == q_clean,
                    # 2. Starts with clean query (index-friendly if database supports it)
                    Movie.clean_title.ilike(f"{q_clean}%"),
                    # 3. Full match on the clean query (only for a single phrase)
                    Movie.clean_title.ilike(f"%{q_clean}%"),
                ]

                # If query has multiple words, try matching them separated by wildcards (less aggressive than before)
                if len(q_clean.split()) > 1:
                    db_filters.append(
                        Movie.clean_title.ilike('%' + '%'.join(q_clean.split()) + '%')
                    )

                # Combine filters using OR
                filt = or_(*db_filters)
                
                # Fetch maximum 100 candidates from the database
                res = await session.execute(
                    select(Movie.imdb_id, Movie.title, Movie.clean_title).filter(filt).limit(100)
                )
                candidates = res.all()
                if not candidates:
                    return []
                
                # CPU-Bound Fuzzy Matching (Still offloaded to a separate thread)
                final_results = await asyncio.to_thread(
                    _process_fuzzy_candidates, 
                    candidates, 
                    query
                )
                
                # Limit the final results to the requested limit
                return final_results[:limit]
            
            except Exception as e:
                logger.error(f"Advanced search failed: {e}")
                return []
