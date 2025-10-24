# -*- coding: utf-8 -*-
import os
import asyncio
import logging
# ... [Other imports remain] ...
from aiogram import Bot, Dispatcher, types, F
# ... [Other aiogram imports remain] ...
from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks, Request, HTTPException

from database import Database, AUTO_MESSAGE_ID_PLACEHOLDER 
from algolia_sync import algolia_client # NEW IMPORT

# ... [Load dotenv, logging setup remains] ...

# ============ CONFIGURATION ============
# ... [Configuration variables remain] ...

# ============ OPTIMIZED TIMEOUTS FOR FREE TIER ============
HANDLER_TIMEOUT = 10  # Reduced from 15 for stability
DB_OP_TIMEOUT = 5     
TG_OP_TIMEOUT = 2     # Reduced from 3
ALGOLIA_OP_TIMEOUT = 5 # New Timeout for Algolia calls

# ============ SEMAPHORE FOR DB OPERATIONS ============
DB_SEMAPHORE = asyncio.Semaphore(5)  # Max 5 concurrent DB calls

# ... [BOT_TOKEN and DATABASE_URL check remains] ...

# ... [build_webhook_url, bot, dp, db, start_time initialization remains] ...

# ... [Graceful Shutdown Signal Handlers remains] ...

# ... [handler_timeout decorator remains] ...

# ============ SAFE WRAPPERS WITH SEMAPHORE ============
# ... [safe_db_call remains] ...

async def safe_algolia_call(coro, timeout=ALGOLIA_OP_TIMEOUT, default=None):
    """Safely execute Algolia call with timeout."""
    try:
        # Algolia calls are fast, no semaphore needed, but a timeout is good.
        return await asyncio.wait_for(coro, timeout=timeout) 
    except asyncio.TimeoutError:
        logger.error(f"Algolia operation timed out after {timeout}s")
        return default
    except Exception as e:
        logger.error(f"Algolia operation error: {e}") 
        return default

# ... [safe_tg_call remains] ...

# ... [FILTERS & HELPERS (AdminFilter, get_uptime, check_user_membership, get_join_keyboard, etc.) remain] ...

# ============ LIFESPAN MANAGEMENT (OPTIMIZATION: Initial Algolia Setup) ============
@asynccontextmanager
async def lifespan(app: FastAPI):
    # OPTIMIZED: Reduced executor size for Free Tier (0.1 CPU)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=5) # Reduced from 10 
    loop = asyncio.get_event_loop()
    loop.set_default_executor(executor)
    logger.info("ThreadPoolExecutor initialized with max_workers=5 (Free Tier optimized).")
    
    await db.init_db() 
    
    # NEW: Initialize Algolia settings
    if not await algolia_client.init_index():
        logger.error("Algolia index setup failed! Search will be unavailable.")

    # NEW: Initial sync to Algolia (Run only once on first deploy or restart)
    if not (await safe_algolia_call(algolia_client.search_movies('test', limit=1))):
        logger.info("Algolia index is empty or test failed. Starting full sync...")
        movies_to_index = await safe_db_call(db.get_all_movies_for_indexing(), timeout=600, default=[])
        if movies_to_index:
             # Using ThreadPoolExecutor for a potentially long sync operation
            await loop.run_in_executor(None, algolia_client.admin_index.save_objects, movies_to_index)
            logger.info(f"Initial {len(movies_to_index)} movies synced to Algolia.")

    # ... [Start event loop monitor, Webhook setup, etc. remains] ...

    yield
    # ... [Cleanup remains] ...

app = FastAPI(lifespan=lifespan)

# ... [WEBHOOK ENDPOINT, HEALTH CHECK, CAPACITY MANAGEMENT remains] ...

# ============ BOT HANDLERS ============
# ... [start_command, help_command, check_join_callback remains] ...

@dp.message(F.text & ~F.text.startswith("/") & (F.chat.type == "private"))
@handler_timeout(10) # Reduced timeout
async def search_movie_handler(message: types.Message):
    user_id = message.from_user.id
    # ... [Membership check and capacity check remains] ...

    original_query = message.text.strip()
    if len(original_query) < 2:
        await safe_tg_call(message.answer("🤔 Kripya kam se kam 2 characters ka query bhejein."))
        return

    searching_msg = await safe_tg_call(message.answer(f"🔍 <b>{original_query}</b> ki khoj jaari hai… (Algolia)"))
    if not searching_msg:
        return
    
    # NEW: Algolia Search Call - Offloading CPU heavy work!
    top = await safe_algolia_call(algolia_client.search_movies(original_query, limit=20), timeout=5, default=[])
    
    if not top:
        await safe_tg_call(searching_msg.edit_text(
            f"🥲 Maaf kijiye, <b>{original_query}</b> ke liye match nahi mila; spelling/variant try karein."
        ))
        return

    buttons = [[InlineKeyboardButton(text=movie["title"], callback_data=f"get_{movie['imdb_id']}")] for movie in top]
    await safe_tg_call(searching_msg.edit_text(
        f"🎬 <b>{original_query}</b> ke liye {len(top)} results mile — file paane ke liye chunein:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    ))

@dp.callback_query(F.data.startswith("get_"))
@handler_timeout(10) # Reduced timeout
async def get_movie_callback(callback: types.CallbackQuery):
    # ... [Logic remains UNCHANGED] ...

# ============ ADMIN COMMANDS ============
# ... [stats_command, broadcast_command, cleanup_users_command, etc. remains] ...

@dp.message(Command("add_movie"), AdminFilter())
@handler_timeout(15)
async def add_movie_command(message: types.Message):
    # ... [Input validation and parsing remains] ...
        
    existing = await safe_db_call(db.get_movie_by_imdb(imdb_id))
    if existing:
        await safe_tg_call(message.answer("⚠️ Is IMDB ID se movie pehle se maujood hai."))
        return
        
    file_id = message.reply_to_message.video.file_id if message.reply_to_message.video else message.reply_to_message.document.file_id
    
    # 1. Add to DB
    success = await safe_db_call(db.add_movie(
        imdb_id=imdb_id, title=title, year=year,
        file_id=file_id, message_id=message.reply_to_message.message_id, channel_id=message.reply_to_message.chat.id
    ), default=False)
    
    if success:
        # 2. Add to Algolia Index
        algolia_success = await safe_algolia_call(algolia_client.add_movie_to_index({
             'imdb_id': imdb_id, 'title': title, 'year': year,
        }))
        if not algolia_success:
             await safe_tg_call(message.answer("⚠️ Movie DB me add ho gayi, par Algolia search index update nahi ho paya."))
             return

        await safe_tg_call(message.answer(f"✅ Movie '<b>{title}</b>' successfully add ho gayi hai."))
    else:
        await safe_tg_call(message.answer("❌ Movie add karne me error aaya (DB connection issue)."))

@dp.message(Command("remove_dead_movie"), AdminFilter())
@handler_timeout(15)
async def remove_dead_movie_command(message: types.Message):
    # ... [Validation remains] ...
    
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id))
    if not movie:
        await safe_tg_call(message.answer(f"❌ Movie with IMDB ID <code>{imdb_id}</code> not found in database."))
        return
    
    # 1. Remove from DB
    success = await safe_db_call(db.remove_movie_by_imdb(imdb_id), default=False)
    
    if success:
        # 2. Remove from Algolia Index
        await safe_algolia_call(algolia_client.remove_movie_from_index(imdb_id))
        
        await safe_tg_call(message.answer(f"✅ Successfully removed movie: <b>{movie['title']}</b> (IMDB: {imdb_id})"))
    else:
        await safe_tg_call(message.answer(f"❌ Failed to remove movie (database error)."))

@dp.message(Command("rebuild_index"), AdminFilter())
@handler_timeout(300)
async def rebuild_index_command(message: types.Message):
    await safe_tg_call(message.answer("🔧 Full Indexing shuru ho raha hai (Algolia)…"))
    
    movies_to_index = await safe_db_call(db.get_all_movies_for_indexing(), timeout=180, default=[])
    if not movies_to_index:
        await safe_tg_call(message.answer("❌ DB se movies data nahi mil paya."))
        return

    # Use ThreadPoolExecutor for the potentially long network I/O
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, algolia_client.admin_index.save_objects, movies_to_index)

    await safe_tg_call(message.answer(f"✅ Full Reindex complete: <b>{len(movies_to_index)}</b> titles Algolia ko send kiye gaye."))


# ============ AUTO-INDEX FROM CHANNEL ============
@dp.channel_post()
@handler_timeout(15)
async def auto_index_handler(message: types.Message):
    # ... [Initial checks and info parsing remains] ...
    
    # ... [DB entry success logic remains] ...
    
    if success:
        # NEW: Sync to Algolia after successful DB entry
        algolia_success = await safe_algolia_call(algolia_client.add_movie_to_index({
             'imdb_id': imdb_id, 'title': movie_info.get("title"), 'year': movie_info.get("year"),
        }))
        if algolia_success:
            logger.info(f"Auto-indexed and Synced to Algolia: {movie_info.get('title')}")
        else:
             logger.error(f"Auto-index failed to sync to Algolia: {movie_info.get('title')}")
    else:
        logger.error(f"Auto-index failed: {movie_info.get('title')} (DB connection issue).")
