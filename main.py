import os
import json
import time
import asyncio
import sys 
import traceback
import logging
from typing import Dict, List, Optional
from collections import defaultdict
from flask import Flask, jsonify
from threading import Thread

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.enums import ParseMode

from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from algoliasearch.search_client import SearchClient 
from rapidfuzz import fuzz 

# ====================================================================
# CONFIGURATION
# ====================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEB_SERVER_PORT = int(os.environ.get("PORT", 8080))
ADMIN_IDS = [7263519581] 

DATABASE_URL = os.getenv("DATABASE_URL")

ALGOLIA_APP_ID = os.getenv("ALGOLIA_APPLICATION_ID")
ALGOLIA_SEARCH_KEY = os.getenv("ALGOLIA_SEARCH_KEY") 
# 🚨 FIX: Added ALGOLIA_WRITE_KEY environment variable (MUST be set in Render)
ALGOLIA_WRITE_KEY = os.getenv("ALGOLIA_WRITE_KEY") 
ALGOLIA_INDEX_NAME = os.getenv("ALGOLIA_INDEX_NAME", "Media_index")

CORRECT_LIBRARY_CHANNEL_ID = -1003138949015 

LIBRARY_CHANNEL_USERNAME = os.getenv("LIBRARY_CHANNEL_USERNAME", "MOVIEMAZA19")
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", CORRECT_LIBRARY_CHANNEL_ID))
JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "MOVIEMAZASU")
JOIN_GROUP_USERNAME = os.getenv("JOIN_GROUP_USERNAME", "THEGREATMOVIESL9")

# FIX: Added ALGOLIA_WRITE_KEY check
if not BOT_TOKEN or not ALGOLIA_APP_ID or not ALGOLIA_SEARCH_KEY or not ALGOLIA_WRITE_KEY or not DATABASE_URL:
    logger.warning("⚠️  WARNING: Missing essential environment variables (DB/Token/Algolia Keys)")
    logger.warning("⚠️  Running in DEMO MODE - bot functionality will be limited")
    logger.warning("⚠️  For production, set: BOT_TOKEN, DATABASE_URL, ALGOLIA_APPLICATION_ID, ALGOLIA_SEARCH_KEY, ALGOLIA_WRITE_KEY")
    
    if not BOT_TOKEN:
        BOT_TOKEN = "demo_token_placeholder"
    if not DATABASE_URL:
        DATABASE_URL = "postgresql://demo:demo@localhost/demo"
    if not ALGOLIA_APP_ID:
        ALGOLIA_APP_ID = "demo_app_id"
    if not ALGOLIA_SEARCH_KEY:
        ALGOLIA_SEARCH_KEY = "demo_search_key"
    if not ALGOLIA_WRITE_KEY:
        ALGOLIA_WRITE_KEY = "demo_write_key" # Added dummy write key

Base = declarative_base()
engine = None
SessionLocal = None

class Movie(Base):
    __tablename__ = "movies"
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, index=True, nullable=False)
    post_id = Column(Integer, unique=True, nullable=False)
    
bot = None
algolia_index = None
DEMO_MODE = False

try:
    bot = Bot(token=BOT_TOKEN)
except Exception as e:
    logger.error(f"⚠️  Could not initialize bot (likely demo mode): {e}")
    logger.error("⚠️  Bot will run as health-check server only")
    DEMO_MODE = True

dp = Dispatcher()

# ====================================================================
# INITIALIZATION & DB/ALGOLIA SETUP
# ====================================================================

def initialize_db_and_algolia_with_retry(max_retries: int = 5, base_delay: float = 2.0) -> bool:
    """Initialize DB and Algolia with exponential backoff retry logic."""
    global engine, SessionLocal, algolia_index
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to initialize PostgreSQL and Algolia... (Attempt {attempt + 1}/{max_retries})")
            
            db_url = DATABASE_URL
            if db_url.startswith("postgresql://"):
                db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
            
            engine = create_engine(
                db_url,
                pool_pre_ping=True,
                pool_size=10,
                max_overflow=20,
                pool_timeout=30,
                pool_recycle=3600,
                connect_args={"connect_timeout": 10}
            )
            
            Base.metadata.create_all(bind=engine)
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            
            test_session = SessionLocal()
            try:
                test_session.execute(text("SELECT 1"))
                test_session.close()
                logger.info("✅ PostgreSQL connection verified.")
            except Exception as e:
                test_session.close()
                raise Exception(f"DB health check failed: {e}")
            
            # FIX: Use the Write Key for Algolia Client (required for indexing)
            algolia_client = SearchClient(ALGOLIA_APP_ID, ALGOLIA_WRITE_KEY) 
            
            algolia_index = algolia_client.init_index(ALGOLIA_INDEX_NAME) 
            
            try:
                # Test connection using Search Key (passed in the request_options for read operations)
                algolia_index.search(
                    query="test",
                    request_options={
                        "hitsPerPage": 1, 
                        "apiKey": ALGOLIA_SEARCH_KEY # Use search key for read test
                    }
                )
                logger.info("✅ Algolia connection verified.")
            except Exception as e:
                logger.warning(f"⚠️ Algolia health check warning: {e}")
            
            logger.info("✅ PostgreSQL & Algolia Clients Initialized Successfully.")
            return True

        except Exception as e:
            logger.error(f"❌ Initialization attempt {attempt + 1} failed: {e}")
            
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.info(f"⏳ Retrying in {delay:.1f} seconds...")
                time.sleep(delay)
            else:
                logger.error("❌ CRITICAL: All initialization attempts failed.")
                logger.warning("⚠️ Bot will continue running with degraded functionality.")
                return False
    
    return False

if not initialize_db_and_algolia_with_retry():
    logger.warning("⚠️ WARNING: Database/Search initialization failed after all retries.")
    logger.warning("⚠️ Bot will run with limited functionality. Admin commands may not work.")

def get_db():
    """Database session dependency with error handling."""
    if not SessionLocal:
        logger.warning("⚠️ Database not initialized. Cannot provide session.")
        return
    
    db_session = SessionLocal()
    try:
        yield db_session
    finally:
        db_session.close()

user_sessions: Dict[int, Dict] = defaultdict(dict)
verified_users: set = set() 
users_database: Dict[int, Dict] = {} 
bot_stats = {"start_time": time.time(), "total_searches": 0, "algolia_searches": 0}
RATE_LIMIT_SECONDS = 1 

def check_rate_limit(user_id: int) -> bool:
    current_time = time.time()
    if user_id in user_sessions and current_time - user_sessions[user_id].get('last_action', 0) < RATE_LIMIT_SECONDS:
        return False
    user_sessions[user_id]['last_action'] = current_time
    return True

def add_user(user_id: int, username: Optional[str] = None, first_name: Optional[str] = None):
    if user_id not in users_database:
        users_database[user_id] = {"user_id": user_id}

# ====================================================================
# SYNCHRONOUS Algolia/DB Operations 
# ====================================================================

def sync_algolia_fuzzy_search(query: str, limit: int = 20) -> List[Dict]:
    global algolia_index, bot_stats
    if not algolia_index: 
        logger.warning("⚠️ Algolia not initialized. Cannot perform search.")
        return []
    
    bot_stats["total_searches"] += 1
    
    try:
        # Use Search Key for read operations
        search_results = algolia_index.search(
            query=query,
            request_options={
                "attributesToRetrieve": ['title', 'post_id'],
                "hitsPerPage": limit,
                "typoTolerance": True,
                "apiKey": ALGOLIA_SEARCH_KEY # Explicitly use Search Key
            }
        )
        bot_stats["algolia_searches"] += 1
        
        results = []
        for hit in search_results.get('hits', []):
            post_id = hit.get('post_id')
            if post_id:
                results.append({"title": hit.get('title', 'Unknown Movie'), "post_id": post_id})
        return results
        
    except Exception as e:
        logger.error(f"Error searching with Algolia: {e}")
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        return []

def sync_add_movie_to_db_and_algolia(title: str, post_id: int):
    """Handles automatic indexing of new channel posts."""
    global algolia_index
    if not algolia_index or not SessionLocal: 
        logger.warning("⚠️ Indexing failed: DB/Algolia not initialized.")
        return False
        
    db_session = SessionLocal()
    try:
        # 1. DB Check for Duplicates (post_id)
        existing_movie = db_session.query(Movie).filter(Movie.post_id == post_id).first()
        if existing_movie: 
            logger.info(f"Movie already indexed in DB: {title}")
            return False

        # 2. Add to DB
        new_movie = Movie(title=title.strip(), post_id=post_id)
        db_session.add(new_movie)
        db_session.commit()
        db_session.refresh(new_movie)

        # 3. Add to Algolia (FIXED: Removed 'body=' keyword to fix TypeError)
        algolia_index.save_object(
            { # object is passed directly
                "objectID": str(new_movie.id),
                "title": title.strip(),
                "post_id": post_id,
            }
        )
        
        logger.info(f"✅ Auto-Indexed: {title} (Post ID: {post_id})")
        return True
        
    except Exception as e:
        db_session.rollback()
        logger.error(f"❌ Error adding movie to DB/Algolia: {e}")
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        return False
    finally:
        db_session.close()

# ASYNCHRONOUS wrappers for the main bot
def algolia_fuzzy_search(query: str, limit: int = 20):
    return asyncio.to_thread(sync_algolia_fuzzy_search, query, limit)

async def add_movie_to_db_and_algolia(title: str, post_id: int):
    return await asyncio.to_thread(sync_add_movie_to_db_and_algolia, title, post_id)


# ====================================================================
# TELEGRAM HANDLERS 
# ====================================================================

@dp.message(Command("start"))
async def cmd_start(message: Message):
    logger.info(f"📨 Received /start from user {message.from_user.id if message.from_user else 'Unknown'}")
    
    if message.from_user:
        user_id = message.from_user.id
        add_user(user_id=user_id, username=message.from_user.username, first_name=message.from_user.first_name)
    else: 
        return
    
    if user_id in ADMIN_IDS:
        uptime_seconds = int(time.time() - bot_stats["start_time"])
        hours = uptime_seconds // 3600
        minutes = (uptime_seconds % 3600) // 60
        
        # FIX: Professional English and correct MarkdownV2 escaping
        admin_welcome_text = (
            f"👑 *Admin Dashboard \\- Status Report*\n"
            f"────────────────────────\n"
            f"🟢 *Status:* Operational\n"
            f"⏱ *Uptime:* {hours}h {minutes}m\n"
            f"👥 *Active Users:* {len(users_database)}\n"
            f"🔍 *Total Searches:* {bot_stats['total_searches']}\n"
            f"────────────────────────\n"
            f"*Quick Commands:*\n"
            f"• /total\\_movies \\(DB Index Count\\)\n"
            f"• /stats \\(Performance Metrics\\)\n"
            f"• /broadcast \\[message\\] \\(Send message to all users\\)\n"
            f"• /cleanup\\_users \\(Clear in\\-memory user list\\)\n"
            f"• /help \\(List of all commands\\)"
        )
        # FIX: Ensure ParseMode is used correctly for the Admin Welcome message
        await message.answer(admin_welcome_text, parse_mode=ParseMode.MARKDOWN_V2) 
        logger.info(f"✅ Sent admin welcome to user {user_id}")
        return 

    # FIX: New User-friendly message for joining channels
    if user_id not in verified_users:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🔗 Channel Join Karein", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}")],
            [InlineKeyboardButton(text=f"👥 Group Join Karein", url=f"https://t.me/{JOIN_GROUP_USERNAME}")],
            [InlineKeyboardButton(text="✅ Mene Join Kar Liya", callback_data="joined")]
        ])
        
        welcome_msg = (
            "👋 **नमस्ते\! Aapka Swagat Hai**\n\n"
            "Bot ka upyog karne ke liye, kripya neeche diye gaye "
            "channel aur group ko **join karein** aur phir "
            "**'Mene Join Kar Liya'** button dabayein: 👇\n\n"
            "➡️ _Access Sirf Joined Users ke liye hai\!_"
        )
        await message.answer(welcome_msg, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN_V2)
        logger.info(f"✅ Sent join prompt to user {user_id}")
    else:
        # FIX: User-friendly message after verification
        search_msg = (
            "🎬 **Ready to Search?**\n\n"
            "🔎 **Search:** Film ka poora ya thoda sa naam type karein\\.\n"
            "✨ *Accuracy:* Spelling galat hone par bhi aapko **20 behtareen options** milenge\\.\n"
            "🛡️ *Safe Access:* Button dabate hi aapko seedha **download link** mil jaayega\\."
        )
        await message.answer(search_msg, parse_mode=ParseMode.MARKDOWN_V2)
        logger.info(f"✅ Sent welcome message to verified user {user_id}")

@dp.callback_query(F.data == "joined")
async def process_joined(callback: types.CallbackQuery):
    logger.info(f"📨 Received 'joined' callback from user {callback.from_user.id if callback.from_user else 'Unknown'}")
    
    if callback.from_user: 
        verified_users.add(callback.from_user.id)
        logger.info(f"✅ User {callback.from_user.id} verified")
        
    search_msg = (
        "✅ **Access Granted\!** 🎉\n\n"
        "Ab aap nischint hokar search kar sakte hain\\.\n"
        "🔎 Film ka naam type karein aur turant results dekhein\\."
    )
    if callback.message and isinstance(callback.message, Message):
        await callback.message.edit_text(search_msg, reply_markup=None, parse_mode=ParseMode.MARKDOWN_V2) 
    await callback.answer("✅ Access granted! You can now start searching.")

@dp.message(F.text)
async def handle_search(message: Message):
    try:
        if not message.text or message.text.startswith('/'): 
            return
        
        logger.info(f"📨 Received search query from user {message.from_user.id}: '{message.text}'")
        
        query = message.text.strip()
        user_id = message.from_user.id
        
        if user_id not in ADMIN_IDS and user_id not in verified_users: 
            logger.info(f"⚠️ User {user_id} not verified, showing join prompt")
            await cmd_start(message)
            return
        if not check_rate_limit(user_id): 
            logger.info(f"⚠️ Rate limit hit for user {user_id}")
            return
        
        logger.info(f"🔍 Searching Algolia for: '{query}'")
        results = await algolia_fuzzy_search(query, limit=20) 
        
        if not results:
            await message.answer(f"❌ Koi Movie Nahin Mili: **{query}**")
            logger.info(f"❌ No results found for: '{query}'")
            return
        
        logger.info(f"✅ Found {len(results)} results for: '{query}'")
        
        keyboard_buttons = []
        for result in results:
            button_text = f"🎬 {result['title']}"
            callback_data = f"post_{result['post_id']}"
            keyboard_buttons.append([InlineKeyboardButton(text=button_text, callback_data=callback_data)])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
        sent_msg = await message.answer(
            f"🎯 **{len(keyboard_buttons)}** Sateek Parinaam Milein: **{query}**",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN_V2
        )
        user_sessions[user_id]['last_search_msg'] = sent_msg.message_id
        logger.info(f"✅ Sent {len(keyboard_buttons)} results to user {user_id}")
    
    except Exception as e:
        logger.error(f"❌ ERROR in handle_search: {e}")
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        await message.answer("❌ Search mein koi aantarik samasya hui.")

@dp.callback_query(F.data.startswith("post_"))
async def send_movie_link(callback: types.CallbackQuery):
    try:
        logger.info(f"📨 Received post callback from user {callback.from_user.id}: {callback.data}")
        
        user_id = callback.from_user.id
        if user_id not in ADMIN_IDS and user_id not in verified_users: 
            await callback.answer("🛑 Pahunch Varjit (Access Denied)。")
            logger.info(f"⚠️ Unverified user {user_id} tried to access movie")
            return

        try: 
            post_id = int(callback.data.split('_')[1])
        except (ValueError, IndexError): 
            await callback.answer("❌ Galat chunav.")
            return
        
        channel_id_clean = str(LIBRARY_CHANNEL_ID).replace("-100", "") 
        post_url = f"https://t.me/c/{channel_id_clean}/{post_id}"
        
        if 'last_search_msg' in user_sessions.get(user_id, {}):
            try: 
                await bot.delete_message(chat_id=user_id, message_id=user_sessions[user_id]['last_search_msg'])
            except: 
                pass
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬇️ Movie Download Link", url=post_url)]
        ])
        
        await bot.send_message(
            chat_id=user_id,
            text="🔗 **Aapka Link Taiyar Hai!**\n\nIsko dabate hi aapko seedha movie post par le jaaya jaayega.",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN_V2
        )
        await callback.answer("✅ Link bhej diya gaya hai.")
        logger.info(f"✅ Sent movie link (post {post_id}) to user {user_id}")
        
    except Exception as e:
        logger.error(f"❌ ERROR in send_movie_link: {e}")
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        await callback.answer("❌ Link banane mein samasya hui.")

@dp.channel_post()
async def handle_channel_post(message: Message):
    try:
        if not message.chat or message.chat.id != LIBRARY_CHANNEL_ID: 
            return
        if message.document or message.video:
            caption = message.caption or ""
            title = caption.split('\n')[0].strip() if caption else "Unknown Movie"
            post_id = message.message_id 
            
            if title and title != "Unknown Movie" and post_id:
                await add_movie_to_db_and_algolia(title, post_id) 
    except Exception as e:
        logger.error(f"Error in handle_channel_post: {e}")

# ====================================================================
# ADMIN HANDLERS 
# ====================================================================

@dp.message(Command("refresh"))
async def cmd_refresh(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: 
        return
    # FIX: Added ParseMode.MARKDOWN_V2
    await message.answer("✅ Cloud services are active\\. Auto\\-indexing is on\\.", parse_mode=ParseMode.MARKDOWN_V2) 

@dp.message(Command("cleanup_users"))
async def cmd_cleanup_users(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: 
        return
    
    old_count = len(users_database)
    users_database.clear()
    # FIX: Added ParseMode.MARKDOWN_V2
    await message.answer(f"🧹 Cleaned up in\\-memory user list\\. Cleared **{old_count}** entries\\.", parse_mode=ParseMode.MARKDOWN_V2)

@dp.message(Command("reload_config"))
async def cmd_reload_config(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: 
        return
    # FIX: Added ParseMode.MARKDOWN_V2
    await message.answer("🔄 Config status: Environment variables are static in Render\\. To apply changes, please manually redeploy the service\\.", parse_mode=ParseMode.MARKDOWN_V2)

@dp.message(Command("total_movies"))
async def cmd_total_movies(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: 
        return
    if not engine: 
        await message.answer("❌ Database connection failed\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return
    try:
        db_gen = get_db()
        db_session = next(db_gen, None)
        if db_session:
            count = db_session.query(Movie).count()
            db_session.close()
            # FIX: Added ParseMode.MARKDOWN_V2
            await message.answer(f"📊 Live Indexed Movies in DB: **{count}**", parse_mode=ParseMode.MARKDOWN_V2)
        else:
            await message.answer("❌ Database session unavailable\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        await message.answer(f"❌ Error fetching movie count: {e}")

@dp.message(Command("help"))
async def cmd_help(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: 
        await message.answer("नमस्ते! फिल्म का नाम टाइप करें और 20 सबसे सटीक परिणाम पाएँगे।")
        return
        
    help_text = (
        "🎬 Admin Panel Commands:\n\n"
        "1. /stats - Bot के प्रदर्शन (performance) के आँकडे देखें।\n"
        "2. /broadcast [Message/Photo/Video] - सभी यूज़र्स को संदेश भेजें।\n"
        "3. /total_movies - Database में Indexed Movies की लाइव संख्या देखें।\n"
        "4. /refresh - Cloud service status चेक करें।\n"
        "5. /cleanup_users - Inactive users को हटाएँ।\n"
        "6. /reload_config - Environment variables की स्थिति देखें।\n\n"
        "ℹ️ User Logic: Search Algolia द्वारा 20 परिणामों के साथ चलता है। Link Generation Render-Safe है।"
    )
    await message.answer(help_text)

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: 
        return
    uptime_seconds = int(time.time() - bot_stats["start_time"])
    hours = uptime_seconds // 3600
    minutes = (uptime_seconds % 3600) // 60
    
    stats_text = (
        "📊 Bot Statistics (Live):\n\n"
        f"🔍 Total Searches: {bot_stats['total_searches']}\n"
        f"⚡ Algolia Searches: {bot_stats['algolia_searches']}\n"
        f"👥 Total Unique Users: {len(users_database)}\n"
        f"⏱ Uptime: {hours}h {minutes}m"
    )
    await message.answer(stats_text)

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: 
        return
    broadcast_text = message.text.replace("/broadcast", "").strip()
    broadcast_photo, broadcast_video = None, None
    if message.reply_to_message:
        if message.reply_to_message.photo: 
            broadcast_photo = message.reply_to_message.photo[-1].file_id
        elif message.reply_to_message.video: 
            broadcast_video = message.reply_to_message.video.file_id
        if message.reply_to_message.caption: 
            broadcast_text = broadcast_text or message.reply_to_message.caption
    
    if not broadcast_text and not broadcast_photo and not broadcast_video:
        await message.answer("⚠️ Broadcast Usage: Reply to a photo/video with /broadcast or type /broadcast [Your message here].")
        return
    
    if not users_database: 
        await message.answer("⚠️ No users in database yet.")
        return
    
    sent_count, blocked_count = 0, 0
    media_type = "📸 photo" if broadcast_photo else ("🎥 video" if broadcast_video else "📝 text")
    status_msg = await message.answer(f"📡 Broadcasting {media_type} to {len(users_database)} users...")
    
    for user_id_key, user_data in users_database.items():
        try:
            target_user_id = int(user_id_key)
            if broadcast_photo: 
                await bot.send_photo(chat_id=target_user_id, photo=broadcast_photo, caption=f"📢 Broadcast:\n\n{broadcast_text}")
            elif broadcast_video: 
                await bot.send_video(chat_id=target_user_id, video=broadcast_video, caption=f"📢 Broadcast:\n\n{broadcast_text}")
            else: 
                await bot.send_message(chat_id=target_user_id, text=f"📢 Broadcast:\n\n{broadcast_text}")
            sent_count += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            if "blocked" in str(e).lower() or "deactivated" in str(e).lower(): 
                blocked_count += 1
            logger.error(f"Failed to send broadcast to user {user_id_key}: {e}")
    
    summary = (
        "✅ Broadcast Complete!\n\n" 
        f"✅ Sent: {sent_count}\n" 
        f"🚫 Blocked/Failed: {blocked_count + (len(users_database) - sent_count - blocked_count)}\n" 
        f"👥 Total Users: {len(users_database)}"
    )
    await status_msg.edit_text(summary)

app_flask = Flask(__name__)

@app_flask.route('/', methods=['GET', 'POST'])
def health_check():
    global bot_stats
    uptime_seconds = int(time.time() - bot_stats["start_time"])
    db_status = "connected" if engine and SessionLocal else "disconnected"
    algolia_status = "connected" if algolia_index else "disconnected"
    
    return jsonify({
        "status": "ok", 
        "service": "telegram_bot_poller", 
        "searches_total": bot_stats['total_searches'], 
        "uptime_seconds": uptime_seconds,
        "database": db_status,
        "algolia": algolia_status
    })

@app_flask.route('/health', methods=['GET'])
def health_endpoint():
    """Health check endpoint for monitoring."""
    return jsonify({
        "status": "healthy",
        "timestamp": time.time(),
        "uptime": int(time.time() - bot_stats["start_time"])
    })

def start_flask_server():
    logger.info(f"🌐 Starting Flask server on port {WEB_SERVER_PORT} for health checks...")
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    app_flask.run(host='0.0.0.0', port=WEB_SERVER_PORT, debug=False, use_reloader=False)

async def start_bot():
    """Start the bot with improved webhook deletion and comprehensive logging."""
    logger.info("=" * 70)
    logger.info("🤖 STARTING TELEGRAM BOT")
    logger.info("=" * 70)

    if DEMO_MODE or not bot:
        logger.warning("⚠️  DEMO MODE: Telegram bot polling disabled")
        logger.warning("⚠️  Only Flask health check server is running")
        logger.warning("⚠️  Set BOT_TOKEN, DATABASE_URL, ALGOLIA_APPLICATION_ID, ALGOLIA_SEARCH_KEY, ALGOLIA_WRITE_KEY to activate bot")
        while True:
            await asyncio.sleep(3600)
        return
    
    try:
        logger.info("🔐 Verifying bot token...")
        bot_info = await bot.get_me()
        logger.info(f"✅ Bot authenticated successfully!")
        logger.info(f"   📛 Bot name: @{bot_info.username}")
        logger.info(f"   🆔 Bot ID: {bot_info.id}")
    except Exception as token_error:
        logger.error(f"❌ CRITICAL: Bot token verification failed!")
        logger.error(f"❌ Error: {token_error}")
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        logger.info("🔄 Flask server will continue running for health checks only")
        while True:
            await asyncio.sleep(3600)
        return
    
    logger.info("🔄 Checking for existing webhooks...")
    try:
        webhook_info = await bot.get_webhook_info()
        logger.info(f"   📡 Current webhook URL: {webhook_info.url if webhook_info.url else 'None'}")
        
        if webhook_info.url:
            logger.info("   🗑️  Webhook detected! Deleting to enable polling...")
            delete_result = await bot.delete_webhook(drop_pending_updates=True)
            logger.info(f"   ✅ Webhook deletion result: {delete_result}")
            await asyncio.sleep(2)
            logger.info("   ✅ Webhook successfully deleted!")
        else:
            logger.info("   ✅ No webhook configured. Ready for polling.")
    except Exception as webhook_error:
        logger.warning(f"   ⚠️  Warning during webhook check: {webhook_error}")
        logger.error(f"   ❌ Traceback: {traceback.format_exc()}")
    
    logger.info("📝 Message handlers registered successfully")
    
    logger.info("🔄 Starting Long Polling mode...")
    logger.info("=" * 70)
    
    try:
        logger.info("✅ BOT IS NOW LISTENING FOR MESSAGES!")
        logger.info("=" * 70)
        logger.info("📨 Waiting for incoming messages...")
        logger.info("=" * 70)
        
        await dp.start_polling(
            bot, 
            allowed_updates=dp.resolve_used_update_types(),
            drop_pending_updates=True,
            timeout=60, 
            request_timeout=60.0 
        )
    except Exception as polling_error:
        logger.error("=" * 70)
        logger.error(f"❌ CRITICAL ERROR in polling: {polling_error}")
        logger.error(f"❌ Error type: {type(polling_error).__name__}")
        logger.error(f"❌ Traceback:\n{traceback.format_exc()}")
        logger.error("=" * 70)
        logger.info("🔄 Bot will attempt to keep Flask server alive for health checks...")
        while True:
            await asyncio.sleep(3600)

async def main():
    """Main entry point with Flask in thread and bot in main asyncio loop."""
    logger.info("=" * 70)
    logger.info("🚀 TELEGRAM MOVIE BOT - DEPLOYMENT")
    logger.info("=" * 70)
    
    flask_thread = Thread(target=start_flask_server, daemon=True)
    flask_thread.start()
    logger.info("✅ Flask health check server started in background thread")
    
    await asyncio.sleep(2)
    
    await start_bot()

if __name__ == "__main__":
    try:
        # Start the main async entry point
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n⚠️ Bot stopped by user.")
    except Exception as main_error:
        logger.error(f"❌ FATAL ERROR in main: {main_error}")
        logger.error(f"❌ Traceback:\n{traceback.format_exc()}")
