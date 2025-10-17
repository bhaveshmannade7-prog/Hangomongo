import os
import time
import asyncio
import sys 
import traceback
import logging
from typing import Dict, List, Optional
from collections import defaultdict
from threading import Thread # Required for background Flask
from contextlib import suppress 

# Flask and related imports are necessary for Web Service type
from flask import Flask, jsonify 

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.enums import ParseMode

from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
# FIX: Correct Algolia V3 Import
from algoliasearch.search_client import SearchClient 
from rapidfuzz import fuzz 

# ====================================================================
# CONFIGURATION & SCALABILITY SETUP
# ====================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment Variables (MUST BE SET ON RENDER)
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ALGOLIA_APP_ID = os.getenv("ALGOLIA_APPLICATION_ID")
ALGOLIA_SEARCH_KEY = os.getenv("ALGOLIA_SEARCH_KEY") 
ALGOLIA_WRITE_KEY = os.getenv("ALGOLIA_WRITE_KEY") # Essential for indexing
ALGOLIA_INDEX_NAME = os.getenv("ALGOLIA_INDEX_NAME", "Media_index")

# IDs
ADMIN_IDS = [7263519581] 
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", -1003138949015))
JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "MOVIEMAZASU")
JOIN_GROUP_USERNAME = os.getenv("JOIN_GROUP_USERNAME", "THEGREATMOVIESL9")

# Fallback for DEMO MODE (Crash Prevention)
if not all([BOT_TOKEN, DATABASE_URL, ALGOLIA_APP_ID, ALGOLIA_SEARCH_KEY, ALGOLIA_WRITE_KEY]):
    logger.critical("⚠️ CRITICAL: Essential Environment variables are missing.")
    DEMO_MODE = True
else:
    DEMO_MODE = False

# Database Setup
Base = declarative_base()
engine = None
SessionLocal = None

class Movie(Base):
    __tablename__ = "movies"
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, index=True, nullable=False)
    post_id = Column(Integer, unique=True, nullable=False)
    
# Global Instances
bot = None
algolia_index = None
try:
    bot = Bot(token=BOT_TOKEN)
except Exception:
    pass # Let initialization handle the error

dp = Dispatcher()
user_sessions: Dict[int, Dict] = defaultdict(dict)
verified_users: set = set() 
bot_stats = {"start_time": time.time(), "total_searches": 0, "algolia_searches": 0}
RATE_LIMIT_SECONDS = 1 

# ====================================================================
# INIT & HEALTH CHECK LOGIC
# ====================================================================

def initialize_db_and_algolia_with_retry(max_retries: int = 5, base_delay: float = 2.0) -> bool:
    """Initializes DB and Algolia clients."""
    global engine, SessionLocal, algolia_index
    
    # 1. Bot Initialization Check is done outside
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to initialize PostgreSQL and Algolia... (Attempt {attempt + 1}/{max_retries})")
            
            # 2. PostgreSQL Connection
            db_url = DATABASE_URL
            if db_url.startswith("postgresql://"):
                db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
            
            engine = create_engine(db_url, pool_pre_ping=True, connect_args={"connect_timeout": 10})
            Base.metadata.create_all(bind=engine)
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            
            test_session = SessionLocal()
            test_session.execute(text("SELECT 1"))
            test_session.close()
            logger.info("✅ PostgreSQL connection verified.")
            
            # 3. Algolia Connection (Using Write Key for Client Initialization)
            algolia_client = SearchClient.create(ALGOLIA_APP_ID, ALGOLIA_WRITE_KEY) 
            algolia_index = algolia_client.init_index(ALGOLIA_INDEX_NAME) 
            
            algolia_index.search(
                query="test",
                request_options={"hitsPerPage": 1, "apiKey": ALGOLIA_SEARCH_KEY}
            )
            logger.info("✅ Algolia connection verified.")
            
            logger.info("✅ PostgreSQL & Algolia Clients Initialized Successfully.")
            return True

        except Exception as e:
            logger.error(f"❌ Initialization attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1 and not DEMO_MODE:
                delay = base_delay * (2 ** attempt)
                logger.info(f"⏳ Retrying in {delay:.1f} seconds...")
                time.sleep(delay)
            else:
                logger.critical("❌ CRITICAL: All initialization attempts failed. Bot starting with degraded functionality.")
                return False
    
    return False

if not initialize_db_and_algolia_with_retry():
    logger.warning("⚠️ WARNING: Database/Search initialization failed after all retries. Admin commands may not work.")

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

# ====================================================================
# UTILITIES & SCALABLE SYNCHRONOUS WRAPPERS
# ====================================================================

def check_rate_limit(user_id: int) -> bool:
    current_time = time.time()
    if user_id in user_sessions and current_time - user_sessions[user_id].get('last_action', 0) < RATE_LIMIT_SECONDS:
        return False
    user_sessions[user_id]['last_action'] = current_time
    return True

def add_user(user_id: int, username: Optional[str] = None, first_name: Optional[str] = None):
    if user_id not in users_database:
        users_database[user_id] = {"user_id": user_id}

def sync_algolia_fuzzy_search(query: str, limit: int = 20) -> List[Dict]:
    global algolia_index, bot_stats
    if not algolia_index: return []
    
    bot_stats["total_searches"] += 1
    
    try:
        search_results = algolia_index.search(
            query=query,
            request_options={
                "attributesToRetrieve": ['title', 'post_id'],
                "hitsPerPage": limit,
                "typoTolerance": True,
                "apiKey": ALGOLIA_SEARCH_KEY 
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
        return []

def sync_add_movie_to_db_and_algolia(title: str, post_id: int):
    """Handles automatic indexing of new channel posts and prevents duplicates."""
    global algolia_index
    if not algolia_index or not SessionLocal: return False
        
    db_session = SessionLocal()
    try:
        # 1. DB Check for Duplicates (post_id) - Prevents duplicate indexing
        existing_movie = db_session.query(Movie).filter(Movie.post_id == post_id).first()
        if existing_movie: return False

        # 2. Add to DB
        new_movie = Movie(title=title.strip(), post_id=post_id)
        db_session.add(new_movie)
        db_session.commit()
        db_session.refresh(new_movie)

        # 3. Add to Algolia (FIXED: Auto-Indexing Bug)
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
        return False
    finally:
        db_session.close()

# ASYNCHRONOUS wrappers for the main bot
async def algolia_fuzzy_search(query: str, limit: int = 20):
    return await asyncio.to_thread(sync_algolia_fuzzy_search, query, limit)

async def add_movie_to_db_and_algolia(title: str, post_id: int):
    return await asyncio.to_thread(sync_add_movie_to_db_and_algolia, title, post_id)


# ====================================================================
# TELEGRAM HANDLERS 
# ====================================================================

@dp.message(Command("start"))
async def cmd_start(message: Message):
    if not message.from_user: return
    user_id = message.from_user.id
    add_user(user_id=user_id, username=message.from_user.username, first_name=message.from_user.first_name)
    
    if user_id in ADMIN_IDS:
        uptime_seconds = int(time.time() - bot_stats["start_time"])
        hours = uptime_seconds // 3600
        minutes = (uptime_seconds % 3600) // 60
        
        # FIX: Admin Commands ParseMode and escaping is correct now.
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
        await message.answer(admin_welcome_text, parse_mode=ParseMode.MARKDOWN_V2) 
        return 

    # FIX: New User-friendly message for joining channels
    if user_id not in verified_users:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🔗 Channel Join Karein", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}")],
            [InlineKeyboardButton(text=f"👥 Group Join Karein", url=f"https://t.me/{JOIN_GROUP_USERNAME}")],
            [InlineKeyboardButton(text="✅ Mene Join Kar Liya", callback_data="joined")]
        ])
        
        # FIX: Proper MarkdownV2 escaping
        welcome_msg = (
            "👋 **नमस्ते\\! Aapka Swagat Hai**\n\n"
            "Bot ka upyog karne ke liye, kripya neeche diye gaye "
            "channel aur group ko **join karein** aur phir "
            "**'Mene Join Kar Liya'** button dabayein: 👇\n\n"
            "➡️ _Access Sirf Joined Users ke liye hai\\!_"
        )
        await message.answer(welcome_msg, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN_V2)
    else:
        # FIX: User-friendly message after verification
        search_msg = (
            "🎬 **Ready to Search?**\n\n"
            "🔎 **Search:** Film ka poora ya thoda sa naam type karein\\.\n"
            "✨ *Accuracy:* Spelling galat hone par bhi aapko **20 behtareen options** milenge\\.\n"
            "🛡️ *Safe Access:* Button dabate hi aapko seedha **download link** mil jaayega\\."
        )
        await message.answer(search_msg, parse_mode=ParseMode.MARKDOWN_V2)

@dp.callback_query(F.data == "joined")
async def process_joined(callback: types.CallbackQuery):
    if not callback.from_user: return
    verified_users.add(callback.from_user.id)
        
    search_msg = (
        "✅ **Access Granted\\!** 🎉\n\n"
        "Ab aap nischint hokar search kar sakte hain\\.\n"
        "🔎 Film ka naam type karein aur turant results dekhein\\."
    )
    if callback.message and isinstance(callback.message, Message):
        await callback.message.edit_text(search_msg, reply_markup=None, parse_mode=ParseMode.MARKDOWN_V2) 
    await callback.answer("✅ Access granted! You can now start searching.")

@dp.message(F.text)
async def handle_search(message: Message):
    if not message.from_user or not message.text or message.text.startswith('/'): return
    
    query = message.text.strip()
    user_id = message.from_user.id
    
    if user_id not in ADMIN_IDS and user_id not in verified_users: 
        return await cmd_start(message)
    if not check_rate_limit(user_id): return
    
    try:
        results = await algolia_fuzzy_search(query, limit=20) 
        
        if not results:
            await message.answer(f"❌ Koi Movie Nahin Mili: **{query}**", parse_mode=ParseMode.MARKDOWN_V2)
            return
        
        keyboard_buttons = []
        for result in results:
            safe_title = result['title'].replace('.', '\\.').replace('-', '\\-')
            button_text = f"🎬 {safe_title}"
            callback_data = f"post_{result['post_id']}"
            keyboard_buttons.append([InlineKeyboardButton(text=button_text, callback_data=callback_data)])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
        safe_query = query.replace('.', '\\.').replace('-', '\\-')
        sent_msg = await message.answer(
            f"🎯 **{len(keyboard_buttons)}** Sateek Parinaam Milein: **{safe_query}**",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN_V2
        )
        user_sessions[user_id]['last_search_msg'] = sent_msg.message_id
    
    except Exception:
        await message.answer("❌ Search mein koi aantarik samasya hui.")

@dp.callback_query(F.data.startswith("post_"))
async def send_movie_link(callback: types.CallbackQuery):
    if not callback.from_user: return
    user_id = callback.from_user.id
    
    if user_id not in ADMIN_IDS and user_id not in verified_users: 
        return await callback.answer("🛑 Pahunch Varjit (Access Denied)。")

    try: 
        post_id = int(callback.data.split('_')[1])
    except (ValueError, IndexError): 
        return await callback.answer("❌ Galat chunav.")
    
    with suppress(Exception):
        await bot.delete_message(chat_id=user_id, message_id=user_sessions[user_id].get('last_search_msg', 0))

    channel_id_clean = str(LIBRARY_CHANNEL_ID).replace("-100", "") 
    post_url = f"https://t.me/c/{channel_id_clean}/{post_id}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬇️ Movie Download Link", url=post_url)]
    ])
    
    try:
        await bot.send_message(
            chat_id=user_id,
            text="🔗 **Aapka Link Taiyar Hai\\!**\n\nIsko dabate hi aapko seedha movie post par le jaaya jaayega\\.",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN_V2
        )
        await callback.answer("✅ Link bhej diya gaya hai.")
    except Exception:
        await callback.answer("❌ Link banane mein samasya hui.")

@dp.channel_post()
async def handle_channel_post(message: Message):
    if not message.chat or message.chat.id != LIBRARY_CHANNEL_ID or not (message.document or message.video):
        return

    try:
        caption = message.caption or ""
        title = caption.split('\n')[0].strip() if caption else message.message_id 
        post_id = message.message_id 
        
        if title and post_id:
            await add_movie_to_db_and_algolia(str(title), post_id) 
    except Exception as e:
        logger.error(f"Error in handle_channel_post: {e}")

# ====================================================================
# ADMIN HANDLERS 
# ====================================================================

@dp.message(Command("refresh"))
async def cmd_refresh(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: return
    await message.answer("✅ Cloud services are active\\. Auto\\-indexing is on\\.", parse_mode=ParseMode.MARKDOWN_V2) 

@dp.message(Command("cleanup_users"))
async def cmd_cleanup_users(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: return
    old_count = len(users_database)
    users_database.clear()
    await message.answer(f"🧹 Cleaned up in\\-memory user list\\. Cleared **{old_count}** entries\\.", parse_mode=ParseMode.MARKDOWN_V2)

@dp.message(Command("reload_config") )
async def cmd_reload_config(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: return
    await message.answer("🔄 Config status: Environment variables are static in Render\\. To apply changes, please manually redeploy the service\\.", parse_mode=ParseMode.MARKDOWN_V2)

@dp.message(Command("total_movies"))
async def cmd_total_movies(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: return
    if DEMO_MODE: return await message.answer("❌ Database connection failed\\.", parse_mode=ParseMode.MARKDOWN_V2)
    try:
        count = await asyncio.to_thread(lambda: SessionLocal().query(Movie).count())
        await message.answer(f"📊 Live Indexed Movies in DB: **{count}**", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        safe_error = str(e).replace('.', '\\.').replace(':', '\\:')
        await message.answer(f"❌ Error fetching movie count: {safe_error}", parse_mode=ParseMode.MARKDOWN_V2)

@dp.message(Command("help"))
async def cmd_help(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: 
        return await message.answer("नमस्ते! फिल्म का नाम टाइप करें और 20 सबसे सटीक परिणाम पाएँगे।")
        
    help_text = (
        "🎬 Admin Panel Commands:\n\n"
        "1. /stats \\- Bot के प्रदर्शन \\(performance\\) के आँकडे देखें\\.\n"
        "2. /broadcast \\[Message/Photo/Video\\] \\- सभी यूज़र्स को संदेश भेजें\\.\n"
        "3. /total\\_movies \\- Database में Indexed Movies की लाइव संख्या देखें\\.\n"
        "4. /refresh \\- Cloud service status चेक करें\\.\n"
        "5. /cleanup\\_users \\- Inactive users को हटाएँ\\.\n"
        "6. /reload\\_config \\- Environment variables की स्थिति देखें\\.\n\n"
        "ℹ️ User Logic: Search Algolia द्वारा 20 परिणामों के साथ चलता है\\. Link Generation Render\\-Safe है\\."
    )
    await message.answer(help_text, parse_mode=ParseMode.MARKDOWN_V2)

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: return
    uptime_seconds = int(time.time() - bot_stats["start_time"])
    hours = uptime_seconds // 3600
    minutes = (uptime_seconds % 3600) // 60
    
    stats_text = (
        "📊 Bot Statistics \\(Live\\):\n\n"
        f"🔍 Total Searches: {bot_stats['total_searches']}\n"
        f"⚡ Algolia Searches: {bot_stats['algolia_searches']}\n"
        f"👥 Total Unique Users: {len(users_database)}\n"
        f"⏱ Uptime: {hours}h {minutes}m"
    )
    await message.answer(stats_text, parse_mode=ParseMode.MARKDOWN_V2)

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: return
    broadcast_text = message.text.replace("/broadcast", "").strip()
    broadcast_photo, broadcast_video = None, None
    if message.reply_to_message:
        if message.reply_to_message.photo: broadcast_photo = message.reply_to_message.photo[-1].file_id
        elif message.reply_to_message.video: broadcast_video = message.reply_to_message.video.file_id
        if message.reply_to_message.caption: broadcast_text = broadcast_text or message.reply_to_message.caption
    
    if not broadcast_text and not broadcast_photo and not broadcast_video:
        return await message.answer("⚠️ Broadcast Usage: Reply to a photo/video with /broadcast or type /broadcast [Your message here]\\.", parse_mode=ParseMode.MARKDOWN_V2)
    
    if not users_database: return await message.answer("⚠️ No users in database yet\\.", parse_mode=ParseMode.MARKDOWN_V2)
    
    sent_count, blocked_count = 0, 0
    media_type = "📸 photo" if broadcast_photo else ("🎥 video" if broadcast_video else "📝 text")
    status_msg = await message.answer(f"📡 Broadcasting {media_type} to {len(users_database)} users...", parse_mode=ParseMode.MARKDOWN_V2)
    
    # Broadcast loop... (error handling is crucial here)
    for user_id_key in list(users_database.keys()):
        try:
            target_user_id = int(user_id_key)
            # FIX: Ensure broadcast text is properly escaped before sending
            safe_broadcast_text = f"📢 Broadcast:\n\n{broadcast_text}".replace('.', '\\.').replace('-', '\\-')
            
            if broadcast_photo: 
                await bot.send_photo(chat_id=target_user_id, photo=broadcast_photo, caption=safe_broadcast_text, parse_mode=ParseMode.MARKDOWN_V2)
            elif broadcast_video: 
                await bot.send_video(chat_id=target_user_id, video=broadcast_video, caption=safe_broadcast_text, parse_mode=ParseMode.MARKDOWN_V2)
            else: 
                await bot.send_message(chat_id=target_user_id, text=safe_broadcast_text, parse_mode=ParseMode.MARKDOWN_V2)
            sent_count += 1
            await asyncio.sleep(0.05)
        except Exception:
            blocked_count += 1
            
    summary = (
        "✅ Broadcast Complete\\!\n\n" 
        f"✅ Sent: {sent_count}\n" 
        f"🚫 Blocked/Failed: {blocked_count + (len(users_database) - sent_count - blocked_count)}\n" 
        f"👥 Total Users: {len(users_database)}"
    )
    await status_msg.edit_text(summary, parse_mode=ParseMode.MARKDOWN_V2)


# ====================================================================
# EXECUTION LOGIC (Pure Polling)
# ====================================================================

async def start_bot():
    """Starts the main Polling loop."""
    logger.info("=" * 70)
    logger.info("🤖 STARTING TELEGRAM BOT")
    logger.info("=" * 70)

    if DEMO_MODE or not bot:
        logger.critical("❌ CRITICAL: Bot cannot start in DEMO MODE. Check environment variables.")
        return
    
    try:
        logger.info("🔐 Verifying bot token...")
        bot_info = await bot.get_me()
        logger.info(f"✅ Bot authenticated successfully! Name: @{bot_info.username}")
    except Exception as token_error:
        logger.critical(f"❌ CRITICAL: Bot token verification failed! Error: {token_error}")
        return
    
    logger.info("🔄 Checking for existing webhooks...")
    with suppress(Exception):
        webhook_info = await bot.get_webhook_info()
        if webhook_info.url:
            logger.info("   🗑️ Webhook detected! Deleting...")
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("   ✅ Webhook successfully deleted!")
    
    logger.info("📝 Message handlers registered successfully")
    logger.info("🔄 Starting Long Polling mode...")
    
    try:
        logger.info("✅ BOT IS NOW LISTENING FOR MESSAGES!")
        logger.info("=" * 70)
        await dp.start_polling(
            bot, 
            allowed_updates=dp.resolve_used_update_types(),
            drop_pending_updates=True,
            timeout=25, # FINAL CONFLICT FIX: Aggressive timeout 
            request_timeout=60.0 
        )
    except Exception as polling_error:
        logger.critical(f"❌ FATAL ERROR in Polling: {polling_error}")

async def main():
    # Initial setup
    if not initialize_db_and_algolia_with_retry():
        logger.critical("System initialization failed. Exiting.")
        return

    # Start the bot's main loop
    await start_bot()

if __name__ == "__main__":
    try:
        # Final deployment structure for stable worker
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n⚠️ Bot stopped by user.")
    except Exception as main_error:
        logger.critical(f"❌ FATAL ERROR in main execution: {main_error}")
