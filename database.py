import logging
import re
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_scoped_session
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, BigInteger, String, DateTime, Boolean, Integer, func, select, or_, text 

from thefuzz import fuzz

logger = logging.getLogger(__name__)
Base = declarative_base()

def clean_text_for_search(text: str) -> str:
    text = text.lower()
    # Failsafe: Replace non-alphanumeric characters (except space) with a single space
    text = re.sub(r'[^a-z0-9\s]+', ' ', text)
    # Condense multiple spaces
    text = re.sub(r'\s+', ' ', text)
    # Remove common season/episode notation that can interfere with movie search
    text = re.sub(r'\b(s|season)\s*\d{1,2}\b', '', text) 
    return text.strip()

def _normalize_for_fuzzy(text: str) -> str:
    t = text.lower()
    t = re.sub(r'[^a-z0-9s]', ' ', t)
    t = re.sub(r's+', ' ', t).strip()
    t = re.sub(r'(.)\1+', r'\1', t)
    t = t.replace('ph', 'f').replace('aa', 'a').replace('kh', 'k').replace('gh', 'g')
    t = t.replace('ck', 'k').replace('cq', 'k').replace('qu', 'k').replace('q', 'k')
    t = t.replace('x', 'ks').replace('c', 'k')
    return t

def _consonant_signature(text: str) -> str:
    t = _normalize_for_fuzzy(text)
    t = re.sub(r'[aeiou]', '', t)
    t = re.sub(r's+', '', t)
    return t

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
        if '?sslmode=require' in database_url:
            database_url = database_url.replace('?sslmode=require', '')
            connect_args['ssl'] = True

        if database_url.startswith('postgresql://'):
            database_url = database_url.replace('postgresql://', 'postgresql+asyncpg://', 1)
        elif database_url.startswith('postgres://'):
            database_url = database_url.replace('postgres://', 'postgresql+asyncpg://', 1)

        self.engine = create_async_engine(database_url, echo=False, connect_args=connect_args)
        self.SessionLocal = async_scoped_session(
            sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession),
            scopefunc=asyncio.current_task,
        )
        logger.info("Database engine initialized for async operations.")

    async def init_db(self):
        async with self.engine.begin() as conn:
            # 1. Ensure tables exist (Base.metadata.create_all handles initial creation)
            await conn.run_sync(Base.metadata.create_all)
            
            # 2. FIX: Crash-proof Manual Migration Check for 'clean_title' column
            if self.engine.dialect.name == 'postgresql':
                
                # Check PostgreSQL information schema to see if the column exists
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
                    logger.warning("Applying manual migration: Adding 'clean_title' column to 'movies'.")
                    
                    # Add column
                    await conn.execute(text("ALTER TABLE movies ADD COLUMN clean_title VARCHAR"))
                    
                    # Populate the column
                    await conn.execute(text("UPDATE movies SET clean_title = lower(regexp_replace(title, '[^a-z0-9\\s]+', ' ', 'g'))"))
                    
                    # Set NOT NULL constraint
                    await conn.execute(text("ALTER TABLE movies ALTER COLUMN clean_title SET NOT NULL"))
                    
                    # Create index on the new column (optional, but good for search speed)
                    await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_movies_clean_title ON movies (clean_title)"))

                    await conn.commit()
                    logger.info("Manual migration completed and clean_title initialized.")
            
        logger.info("Database tables checked/created successfully.")

    def get_session(self) -> AsyncSession:
        return self.SessionLocal()

    # Users
    async def add_user(self, user_id, username, first_name, last_name):
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
        finally:
            await session.close()

    async def get_concurrent_user_count(self, minutes: int = 5):
        session = self.get_session()
        try:
            cutoff_time = datetime.utcnow() - timedelta(minutes=minutes)
            result = await session.execute(
                select(func.count(User.user_id)).filter(User.last_active >= cutoff_time, User.is_active == True)
            )
            return result.scalar_one()
        finally:
            await session.close()

    async def get_all_users(self):
        session = self.get_session()
        try:
            result = await session.execute(select(User.user_id).filter(User.is_active == True))
            return result.scalars().all()
        finally:
            await session.close()

    async def get_user_count(self):
        session = self.get_session()
        try:
            result = await session.execute(select(func.count(User.user_id)))
            return result.scalar_one()
        finally:
            await session.close()

    async def cleanup_inactive_users(self, days: int):
        session = self.get_session()
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            result = await session.execute(select(User).filter(User.last_active < cutoff_date, User.is_active == True))
            users_to_update = result.scalars().all()
            for u in users_to_update:
                u.is_active = False
            await session.commit()
            return len(users_to_update)
        finally:
            await session.close()

    # Movies
    async def add_movie(self, imdb_id, title, year, file_id, message_id, channel_id):
        session = self.get_session()
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
        finally:
            await session.close()

    async def get_movie_count(self):
        session = self.get_session()
        try:
            result = await session.execute(select(func.count(Movie.id)))
            return result.scalar_one()
        finally:
            await session.close()

    async def get_movie_by_imdb(self, imdb_id: str):
        session = self.get_session()
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
        finally:
            await session.close()

    async def export_users(self, limit: int = 2000) -> List[Dict]:
        session = self.get_session()
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
        finally:
            await session.close()

    async def export_movies(self, limit: int = 2000) -> List[Dict]:
        session = self.get_session()
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
        finally:
            await session.close()

    async def rebuild_clean_titles(self) -> Tuple[int, int]:
        session = self.get_session()
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
        finally:
            await session.close()

    # Fuzzy search (improved)
    async def super_search_movies_advanced(self, query: str, limit: int = 20) -> List[Dict]:
        session = self.get_session()
        try:
            q_clean = clean_text_for_search(query)
            q_norm = _normalize_for_fuzzy(query)
            q_cons = _consonant_signature(query)

            tokens = q_clean.split()
            ilike_patterns = [
                # Full token match (most precise)
                '%' + '%'.join(tokens) + '%' if tokens else '%',
                # Clean query match
                f"%{q_clean}%",
                # Normalized query match
                f"%{q_norm}%",
            ]

            cons_chunks = [q_cons[i:i+2] for i in range(0, len(q_cons), 2)] if q_cons else []
            if cons_chunks:
                # Consonant signature chunk match
                ilike_patterns.append('%' + '%'.join(cons_chunks) + '%')

            # Use OR condition for broader search
            filt = or_(*[Movie.clean_title.ilike(p) for p in ilike_patterns])

            res = await session.execute(
                select(Movie.imdb_id, Movie.title, Movie.clean_title).filter(filt).limit(300)
            )
            candidates = res.all()
            if not candidates:
                return []

            results = []
            for imdb_id, title, clean_title in candidates:
                # Scoring based on different matching techniques
                s1 = fuzz.WRatio(clean_title, q_clean)
                s2 = fuzz.token_set_ratio(title, query)
                s3 = fuzz.partial_ratio(clean_title, q_clean)
                s4 = fuzz.ratio(_consonant_signature(title), q_cons)
                score = max(s1, s2, s3, s4)
                
                # Boost score if all query tokens are present in the title
                if all(t in clean_title for t in tokens if t):
                    score = min(100, score + 5)
                
                results.append((score, imdb_id, title))

            # Sort by score (descending) then title (ascending)
            results.sort(key=lambda x: (-x[0], x[2]))
            
            # Filter results with a decent match score (>= 55)
            final = [{'imdb_id': imdb, 'title': t} for (sc, imdb, t) in results if sc >= 55][:limit]
            return final
        except Exception as e:
            logger.error(f"Advanced search failed: {e}")
            return []
        finally:
            await session.close()
