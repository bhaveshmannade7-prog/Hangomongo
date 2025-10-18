import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, BigInteger, String, DateTime, Boolean, Integer, func
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base

logger = logging.getLogger(__name__)
Base = declarative_base()

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
    year = Column(String(10), nullable=True)
    genre = Column(String(200), nullable=True)
    rating = Column(String(10), nullable=True)
    file_id = Column(String, nullable=False)
    channel_id = Column(BigInteger, nullable=False)
    message_id = Column(BigInteger, nullable=False)
    added_by = Column(BigInteger)
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

    async def add_movie(self, imdb_id, title, year, genre, rating, file_id, channel_id, message_id, added_by):
        session = self.get_session()
        try:
            new_movie = Movie(imdb_id=imdb_id, title=title, year=year, genre=genre, rating=rating, file_id=file_id, channel_id=channel_id, message_id=message_id, added_by=added_by)
            session.add(new_movie)
            session.commit()
            return True
        except:
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

    async def search_movies_fuzzy(self, query: str, limit: int = 20):
        session = self.get_session()
        try:
            results = session.query(Movie).filter(func.similarity(Movie.title, query) > 0.15).order_by(func.similarity(Movie.title, query).desc()).limit(limit).all()
            return [{'imdb_id': m.imdb_id, 'title': m.title} for m in results]
        except Exception as e:
            logger.error(f"Fuzzy search error: {e}. 'pg_trgm' extension enable karein.")
            return []
        finally: session.close()
