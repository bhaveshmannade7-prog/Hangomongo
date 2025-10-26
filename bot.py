# -*- coding: utf-8 -*-
import os
import asyncio
import logging
import re
import io
import signal
import json
import hashlib
from datetime import datetime
from contextlib import asynccontextmanager
from typing import List, Dict
from functools import wraps
import concurrent.futures

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart, BaseFilter
from aiogram.types import Update, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest 
from aiogram.client.default import DefaultBotProperties

from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks, Request, HTTPException

# Yahan 'database.py' file se import kar rahe hain
from database import Database, clean_text_for_search, AUTO_MESSAGE_ID_PLACEHOLDER 

load_dotenv()
# Logging ko behtar format kiya gaya hai
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)-12s %(message)s")
logger = logging.getLogger("bot")
logging.getLogger("aiogram").setLevel(logging.WARNING) # Aiogram ke verbose logs ko kam kiya
logging.getLogger("sqlalchemy").setLevel(logging.WARNING) # SQLAlchemy ke logs ko kam kiya

# ============ CONFIGURATION ============
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "123456789")) 
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", "-1003138949015"))
JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "MOVIEMAZASU")
USER_GROUP_USERNAME = os.getenv("USER_GROUP_USERNAME", "THEGREATMOVIESL9")

DATABASE_URL = os.getenv("DATABASE_URL")

RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
PUBLIC_URL = os.getenv("PUBLIC_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# Render free tier ke liye concurrent limit (default 35)
DEFAULT_CONCURRENT_LIMIT = int(os.getenv("CONCURRENT_LIMIT", "35"))
ACTIVE_WINDOW_MINUTES = int(os.getenv("ACTIVE_WINDOW_MINUTES", "5"))
CURRENT_CONC_LIMIT = DEFAULT_CONCURRENT_LIMIT

ALTERNATE_BOTS = ["Moviemaza91bot", "Moviemaza92bot", "Mazamovie9bot"]

# ============ OPTIMIZED TIMEOUTS FOR FREE TIER ============
HANDLER_TIMEOUT = 15  # Default handler timeout
DB_OP_TIMEOUT = 8     # DB operations ke liye thoda extra time
TG_OP_TIMEOUT = 4     # Telegram operations ke liye thoda extra time

# ============ SEMAPHORE FOR DB OPERATIONS ============
# DB par ek saath zyada load na pade, iske liye semaphore
DB_SEMAPHORE = asyncio.Semaphore(10)

if not BOT_TOKEN or not DATABASE_URL:
    logger.critical("Missing BOT_TOKEN or DATABASE_URL! Exiting.")
    raise SystemExit(1)

def build_webhook_url() -> str:
    base = None
    if RENDER_EXTERNAL_URL:
        base = RENDER_EXTERNAL_URL.rstrip("/")
    elif PUBLIC_URL:
        base = PUBLIC_URL.rstrip("/")
    else:
        logger.warning("No external URL found; set RENDER_EXTERNAL_URL or PUBLIC_URL.")
        base = ""
    return f"{base}/bot/{BOT_TOKEN}" if base else ""

WEBHOOK_URL = build_webhook_url()

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
db = Database(DATABASE_URL)
start_time = datetime.utcnow()

# ============ GRACEFUL SHUTDOWN SIGNAL HANDLERS ============
def handle_shutdown_signal(signum, frame):
    logger.info(f"Received shutdown signal {signum}, cleaning up...")
    raise KeyboardInterrupt

signal.signal(signal.SIGTERM, handle_shutdown_signal)
signal.signal(signal.SIGINT, handle_shutdown_signal)

# ============ TIMEOUT DECORATOR ============
def handler_timeout(timeout: int = HANDLER_TIMEOUT):
    """Decorator to add timeout to handlers to prevent hanging"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
            except asyncio.TimeoutError:
                logger.error(f"Handler {func.__name__} timed out after {timeout}s")
                try:
                    # User ko batayein ki timeout ho gaya
                    if args and isinstance(args[0], (types.Message, types.CallbackQuery)):
                        user_id = args[0].from_user.id
                        await bot.send_message(user_id, "⚠️ Request timeout - kripya dobara try karein. Server busy ho sakta hai.", parse_mode=ParseMode.HTML)
                except Exception:
                    pass # Agar user ko message nahi bhej paaye toh koi baat nahi
            except Exception as e:
                logger.error(f"Handler {func.__name__} error: {e}", exc_info=True)
        return wrapper
    return decorator

# ============ SAFE WRAPPERS WITH SEMAPHORE ============
async def safe_db_call(coro, timeout=DB_OP_TIMEOUT, default=None):
    """Safely execute database call with semaphore + timeout."""
    try:
        async with DB_SEMAPHORE:
            return await asyncio.wait_for(coro, timeout=timeout) 
    except asyncio.TimeoutError:
        logger.error(f"DB operation timed out after {timeout}s")
        return default
    except Exception as e:
        logger.debug(f"DB operation error (handled internally): {e}") 
        return default

async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT):
    """Safely execute Telegram API call with timeout."""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(f"Telegram API call timed out after {timeout}s")
        return None
    except Exception as e:
        if isinstance(e, TelegramAPIError):
            logger.warning(f"Telegram API Error: {e}")
            # Agar bot block ho gaya hai toh False return karein (broadcast ke liye)
            if "bot was blocked by the user" in str(e).lower():
                return False
            raise e
        logger.error(f"Unexpected error in Telegram call: {e}")
        return None

# ============ FILTERS & HELPERS ============
class AdminFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user and (message.from_user.id == ADMIN_USER_ID)

def get_uptime() -> str:
    delta = datetime.utcnow() - start_time
    hours, remainder = divmod(int(delta.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    days, hours = divmod(hours, 24)
    if days > 0: return f"{days}d {hours}h {minutes}m"
    if hours > 0: return f"{hours}h {minutes}m"
    return f"{minutes}m {seconds}s"

async def check_user_membership(user_id: int) -> bool:
    # TODO: Implement actual check
    # Abhi ke liye, sabko joined maan rahe hain
    return True 

def get_join_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Channel Join Karein", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}")],
        [InlineKeyboardButton(text="👥 Group Join Karein", url=f"https://t.me/{USER_GROUP_USERNAME}")],
        [InlineKeyboardButton(text="✅ I Have Joined Both", callback_data="check_join")]
    ])

def get_full_limit_keyboard():
    buttons = [[InlineKeyboardButton(text=f"🚀 @{b} (Alternate Bot)", url=f"https://t.me/{b}")] for b in ALTERNATE_BOTS]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def extract_movie_info(caption: str):
    if not caption:
        return None
    info = {}
    lines = caption.splitlines()
    if lines:
        title = lines[0].strip()
        # Agar agli line mein Season (S01, S1) hai, toh use title mein jodein
        if len(lines) > 1 and re.search(r"S\d{1,2}", lines[1], re.IGNORECASE):
            title += " " + lines[1].strip()
        info["title"] = title
    
    imdb_match = re.search(r"(tt\d{7,})", caption)
    if imdb_match:
        info["imdb_id"] = imdb_match.group(1)
        
    year_match = re.search(r"\b(19|20)\d{2}\b", caption)
    if year_match:
        info["year"] = year_match.group(0)
    return info if "title" in info else None

def parse_filename(filename: str) -> Dict[str, str]:
    """JSON se title aur year nikalne ke liye helper function."""
    year = None
    
    # Pehle (YYYY) format dhoondein
    match = re.search(r"\(((19|20)\d{2})\)", filename)
    if match:
        year = match.group(1)
    else:
        # Agar woh nahi mila, toh koi bhi 4-digit number (19XX ya 20XX) dhoondein
        matches = re.findall(r"\b((19|20)\d{2})\b", filename)
        if matches:
            # Aakhri match ko year maanein (e.g., "Title 1999 2020 Edition" mein 2020 lega)
            year = matches[-1][0] 
    
    # .json extension hatayein
    title = os.path.splitext(filename)[0]
    
    return {"title": title, "year": year}


def overflow_message(active_users: int) -> str:
    msg = f"""⚠️ <b>Server Capacity Reached</b>

Hamari free-tier service is waqt <b>{CURRENT_CONC_LIMIT}</b> concurrent users par chal rahi hai 
aur abhi <b>{active_users}</b> active hain. Nayi requests temporarily hold par hain.

Be-rukavat access ke liye alternate bots use karein; neeche se choose karke turant dekhna shuru karein."""
    return msg

# ============ EVENT LOOP MONITOR ============
async def monitor_event_loop():
    """Monitors event loop for blocking operations."""
    while True:
        try:
            start = asyncio.get_event_loop().time()
            await asyncio.sleep(0.1) # Thoda lamba sleep taaki normal operations ko block na kare
            lag = asyncio.get_event_loop().time() - start
            if lag > 0.5: # 500ms se zyaada lag (Free tier par thoda margin rakhein)
                logger.warning(f"⚠️ Event loop lag detected: {lag:.3f}s")
            await asyncio.sleep(60) # Har minute check karein
        except asyncio.CancelledError:
            logger.info("Event loop monitor stopping.")
            break
        except Exception as e:
            logger.error(f"Event loop monitor error: {e}")
            await asyncio.sleep(60)

# ============ LIFESPAN MANAGEMENT ============
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Free tier par CPU kam hote hain, 10 workers aam taur par theek hain
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    loop = asyncio.get_event_loop()
    loop.set_default_executor(executor)
    logger.info("ThreadPoolExecutor initialized with max_workers=10 (Free Tier optimized).")
    
    # DB connection setup
    await db.init_db() 
    
    # Background task monitor
    monitor_task = asyncio.create_task(monitor_event_loop())
    logger.info("Event loop monitor started.")

    # Webhook setup
    if WEBHOOK_URL:
        try:
            current_webhook = await bot.get_webhook_info()
            if current_webhook.url != WEBHOOK_URL:
                await bot.set_webhook(
                    url=WEBHOOK_URL,
                    allowed_updates=dp.resolve_used_update_types(),
                    secret_token=(WEBHOOK_SECRET or None),
                    drop_pending_updates=True,
                )
                logger.info(f"Webhook set to {WEBHOOK_URL}")
            else:
                logger.info(f"Webhook already set to {WEBHOOK_URL}")
        except Exception as e:
            logger.error(f"Webhook setup error: {e}", exc_info=True)
    else:
        logger.warning("WEBHOOK_URL is empty; public URL (RENDER_EXTERNAL_URL) set karna zaroori hai.")

    yield

    # Cleanup
    logger.info("Shutting down...")
    monitor_task.cancel()
    try:
        await asyncio.sleep(2) # Monitor ko cancel hone ka time dein
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Webhook deleted.")
    except Exception as e:
        logger.error(f"Webhook delete error: {e}", exc_info=True)
        
    executor.shutdown(wait=False)
    logger.info("ThreadPoolExecutor shut down.")

app = FastAPI(lifespan=lifespan)

# ============ WEBHOOK ENDPOINT WITH TIMEOUT WRAPPER ============
async def _process_update(u: Update):
    try:
        await dp.feed_update(bot=bot, update=u)
    except Exception as e:
        logger.exception(f"feed_update failed: {e}")
        
@app.post(f"/bot/{BOT_TOKEN}")
async def bot_webhook(update: dict, background_tasks: BackgroundTasks, request: Request):
    try:
        # Webhook secret check
        if WEBHOOK_SECRET:
            if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
                logger.warning("Invalid webhook secret token")
                raise HTTPException(status_code=403, detail="Forbidden")
        
        telegram_update = Update(**update)
        
        # --- FIX: Lamba chalne waale commands ke liye dynamic timeout ---
        timeout_duration = HANDLER_TIMEOUT # Default 15 second timeout
        
        msg_text = None
        if telegram_update.message and telegram_update.message.text:
            msg_text = telegram_update.message.text
        
        if msg_text:
            # JSON import aur broadcast ke liye 30 min
            if msg_text.startswith("/import_json") or msg_text.startswith("/broadcast"):
                timeout_duration = 1810  # 30 minute + 10s buffer
                logger.info(f"Long-running command '{msg_text.split()[0]}' detected, setting timeout to {timeout_duration}s")
            # Index rebuild ke liye 5 min
            elif msg_text.startswith("/rebuild_index"):
                timeout_duration = 310  # 5 minute + 10s buffer
                logger.info(f"Medium-running command '/rebuild_index' detected, setting timeout to {timeout_duration}s")
        # --- End of Fix ---

        async def _process_with_timeout():
            try:
                # Dynamic timeout ka istemaal
                await asyncio.wait_for(_process_update(telegram_update), timeout=timeout_duration)
            except asyncio.TimeoutError:
                logger.error(f"Update processing timed out after {timeout_duration}s: {telegram_update.update_id}")
        
        # Request ko background mein process karein taaki Telegram ko turant 200 OK mil jaaye
        background_tasks.add_task(_process_with_timeout)
        return {"ok": True}
    except Exception as e:
        logger.error(f"Webhook processing error: {e}", exc_info=True)
        return {"ok": False, "error": str(e)}

# ============ HEALTH CHECK ENDPOINT ============
@app.get("/")
async def ping():
    logger.info("Ping/Root endpoint hit (keep-alive).")
    return {"status": "ok", "service": "Movie Bot is Live", "uptime": get_uptime()}

@app.get("/health")
async def health_check():
    logger.info("Health check endpoint hit (keep-alive).")
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat(), "uptime": get_uptime()}

# ============ CAPACITY MANAGEMENT ============
async def ensure_capacity_or_inform(message: types.Message) -> bool:
    """Checks capacity, updates user's last_active time, and enforces limit."""
    user_id = message.from_user.id
    
    # Har message par user ka last_active time update karein
    await safe_db_call(
        db.add_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name),
        timeout=DB_OP_TIMEOUT
    )

    # Admin ko limit nahi lagti
    if user_id == ADMIN_USER_ID:
        return True
    
    # Concurrent users check karein
    active = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), timeout=DB_OP_TIMEOUT, default=0)
    if active > CURRENT_CONC_LIMIT: 
        logger.warning(f"Capacity reached: {active}/{CURRENT_CONC_LIMIT}. User {user_id} request held.")
        try:
            # User ko batayein ki limit poori ho gayi hai
            await asyncio.wait_for(message.answer(overflow_message(active), reply_markup=get_full_limit_keyboard()), timeout=TG_OP_TIMEOUT)
        except:
            pass # Agar message nahi bhej paaye (e.g., timeout)
        return False
        
    return True

# ============ BOT HANDLERS ============
@dp.message(CommandStart())
@handler_timeout(15) # Start command ko thoda extra time dein
async def start_command(message: types.Message):
    user_id = message.from_user.id
    try:
        bot_info = await asyncio.wait_for(bot.get_me(), timeout=5)
    except (asyncio.TimeoutError, TelegramAPIError):
        await safe_tg_call(message.answer("⚠️ Bot mein technical error hai. Kripya thodi der baad /start karein."))
        return

    if user_id == ADMIN_USER_ID:
        await safe_db_call(db.add_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))
        user_count = await safe_db_call(db.get_user_count(), default=0)
        movie_count = await safe_db_call(db.get_movie_count(), default=0)
        concurrent_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        
        # --- Professional Admin Welcome Message ---
        admin_message = f"""👑 <b>Admin Console: @{bot_info.username}</b>
Access Level: Full Management

<b>System Status (Render Free Tier)</b>
• Bot Status: 🟢 Online
• Active Users (5m): {concurrent_users:,}/<b>{CURRENT_CONC_LIMIT}</b>
• Total Users: {user_count:,}
• Indexed Movies: {movie_count:,}
• Uptime: {get_uptime()}
• Service Note: Render free tier 15 min inactivity ke baad spin down hota hai. Pehla request slow hoga.

<b>Management Commands</b>
• /stats — Real-time stats
• /broadcast — Reply to message to send
• /import_json — Reply to a .json file to import
• /rebuild_index — Re-index clean_titles (use if search is poor)
• /cleanup_users — Deactivate inactive users
• /add_movie — Reply: /add_movie imdb_id | title | year
• /remove_dead_movie IMDB_ID — Remove invalid movie
• /export_csv users|movies [limit]
• /set_limit N — Change concurrency cap"""
        
        await safe_tg_call(message.answer(admin_message))
        return

    # Check capacity before welcoming user
    if not await ensure_capacity_or_inform(message):
        return

    # --- User-friendly Welcome Message ---
    welcome_text = f"""🎬 Namaskar <b>{message.from_user.first_name}</b>!

Movie Search Bot me swagat hai.

Bas movie ka naam bhejein. Behtar results ke liye saal (year) bhi likh sakte hain (jaise <b>Kantara 2022</b>).

⚠️ <b>Free Service Note:</b>
Yeh bot Render ke free server par chalta hai. Agar 15 minute tak use nahi hota, toh yeh 'so' jaata hai.
Agar bot /start par 10-15 second lagaye, toh chinta na karein, yeh bas server ko jaga raha hai.

Hamare Channel aur Group join karke "I Have Joined Both" dabayen aur access paayen."""
    
    await safe_tg_call(message.answer(welcome_text, reply_markup=get_join_keyboard()))

@dp.message(Command("help"))
@handler_timeout(15)
async def help_command(message: types.Message):
    await safe_db_call(db.add_user(message.from_user.id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))
    
    # --- Updated Help Text ---
    help_text = """❓ <b>Bot Ka Upyog Kaise Karein</b>

1.  <b>Search Karein:</b> Movie/Show ka naam seedha message mein bhejein. (Example: <code>Jawan</code> ya <code>Mirzapur Season 1</code>)
2.  <b>Behtar Results:</b> Naam ke saath saal (year) zaroor jodein. (Example: <code>Pushpa 2021</code>)

⚠️ <b>Bot Slow Kyon Hai?</b>
Yeh bot free server par hai.
• <b>Start Hone Mein Deri:</b> Agar bot 15 minute use na ho, toh server "so" jaata hai. Dobara /start karne par use "jagne" mein 10-15 second lagte hain.
• <b>Search Mein Deri:</b> Search tez kar di gayi hai, lekin free database par 2-5 second lag sakte hain. Kripya dhairya rakhein.

Agar Bot bilkul na chale, toh alternate bots (jo /start par dikhte hain) use karein."""
    
    await safe_tg_call(message.answer(help_text))

@dp.callback_query(F.data == "check_join")
@handler_timeout(15)
async def check_join_callback(callback: types.CallbackQuery):
    await safe_tg_call(callback.answer("Verifying..."))
    
    # Check capacity again, in case user waited long
    active_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    if active_users > CURRENT_CONC_LIMIT and callback.from_user.id != ADMIN_USER_ID:
        await safe_tg_call(callback.message.edit_text(overflow_message(active_users)))
        await safe_tg_call(bot.send_message(callback.from_user.id, "Alternate bots ka upyog karein:", reply_markup=get_full_limit_keyboard()))
        return
            
    # TODO: Yahaan actual membership check logic daalein
    is_member = await check_user_membership(callback.from_user.id)
    
    if is_member:
        success_text = f"""✅ Verification successful, <b>{callback.from_user.first_name}</b>!

Ab aap library access kar sakte hain — apni pasand ki title ka naam bhejein.

(Free tier capacity: {CURRENT_CONC_LIMIT}, abhi active: {active_users}.)"""
            
        result = await safe_tg_call(callback.message.edit_text(success_text, reply_markup=None))
        if not result:
            # Agar edit fail ho (e.g., message purana hai), toh naya message bhej do
            await safe_tg_call(bot.send_message(callback.from_user.id, success_text, reply_markup=None))
    else:
        await safe_tg_call(callback.message.answer("❌ Aapne abhi tak Channel/Group join nahi kiya hai. Kripya join karke dobara try karein.", show_alert=True))


@dp.message(F.text & ~F.text.startswith("/") & (F.chat.type == "private"))
@handler_timeout(20) # Search ke liye 20 sec timeout
async def search_movie_handler(message: types.Message):
    user_id = message.from_user.id

    if not await check_user_membership(user_id):
        await safe_tg_call(message.answer("⚠️ Kripya pehle Channel aur Group join karein, phir se /start dabayen.", reply_markup=get_join_keyboard()))
        return

    if not await ensure_capacity_or_inform(message):
        return

    original_query = message.text.strip()
    if len(original_query) < 2:
        await safe_tg_call(message.answer("🤔 Kripya kam se kam 2 characters ka query bhejein."))
        return

    searching_msg = await safe_tg_call(message.answer(f"🔍 <b>{original_query}</b> ki khoj jaari hai…"))
    if not searching_msg:
        logger.warning(f"Could not send 'searching' message to user {user_id}")
        return
    
    # --- Optimized Search Call ---
    # DB operation ko 15 sec ka time dein, kyunki yeh complex ho sakti hai
    top = await safe_db_call(db.super_search_movies_advanced(original_query, limit=20), timeout=15, default=[])
    
    if not top:
        await safe_tg_call(searching_msg.edit_text(
            f"🥲 Maaf kijiye, <b>{original_query}</b> ke liye koi match nahi mila.\nKripya spelling check karein ya sirf movie ka naam (bina saal) try karein."
        ))
        return

    buttons = [[InlineKeyboardButton(text=movie["title"], callback_data=f"get_{movie['imdb_id']}")] for movie in top]
    await safe_tg_call(searching_msg.edit_text(
        f"🎬 <b>{original_query}</b> ke liye {len(top)} results mile — file paane ke liye chunein:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    ))

@dp.callback_query(F.data.startswith("get_"))
@handler_timeout(15)
async def get_movie_callback(callback: types.CallbackQuery):
    await safe_tg_call(callback.answer("File forward ki ja rahi hai…"))
    imdb_id = callback.data.split("_", 1)[1]
    
    if not await ensure_capacity_or_inform(callback.message):
        return
        
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id), timeout=DB_OP_TIMEOUT)
    if not movie:
        await safe_tg_call(callback.message.edit_text("❌ Yeh movie ab database me uplabdh nahi hai."))
        return
        
    success = False
    
    await safe_tg_call(callback.message.edit_text(f"✅ <b>{movie['title']}</b> — file forward ki ja rahi hai, kripya chat check karein."))
    
    try:
        # Pehle forward karne ki koshish karein
        await asyncio.wait_for(
            bot.forward_message(
                chat_id=callback.from_user.id,
                from_chat_id=int(movie["channel_id"]),
                message_id=movie["message_id"],
            ),
            timeout=TG_OP_TIMEOUT 
        )
        success = True
        
    except (asyncio.TimeoutError, TelegramAPIError) as e:
        forward_failed_msg = str(e).lower()
        logger.error(f"Forward failed for {imdb_id}: {e}")
        
        # Agar forward fail ho (ya message_id placeholder ho), toh file_id se bhejne ki koshish karein
        # Yeh JSON se import ki gayi files ke liye zaroori hai
        if movie["message_id"] == AUTO_MESSAGE_ID_PLACEHOLDER or 'message to forward not found' in forward_failed_msg or 'bad request: message to forward not found' in forward_failed_msg:
            logger.info(f"Forward failed, falling back to send_document for {imdb_id} (FileID: {movie['file_id']})")
            try:
                # Document/Video ko file_id se bhej
                # (Note: Yeh video/document, dono ho sakta hai, send_document dono handle karta hai)
                await asyncio.wait_for(
                    bot.send_document(
                        chat_id=callback.from_user.id,
                        document=movie["file_id"], 
                        caption=f"🎬 <b>{movie['title']}</b> ({movie['year'] or 'Year not specified'})" 
                    ),
                    timeout=TG_OP_TIMEOUT * 2 # File send karne mein zyada time lag sakta hai
                )
                success = True
                
            except (asyncio.TimeoutError, TelegramAPIError) as e2:
                logger.error(f"❌ DEAD FILE: Movie '{movie['title']}' (IMDB: {imdb_id}) failed both forward and send_document. Error: {type(e2).__name__}. Use: /remove_dead_movie {imdb_id}")
                
            except Exception as e3:
                logger.error(f"Unexpected error during send_document fallback for {imdb_id}: {e3}")
                
    if not success:
        admin_hint = f"\n\n(Admin Hint: File dead hai. Use: /remove_dead_movie {imdb_id})" if callback.from_user.id == ADMIN_USER_ID else ""
        
        await safe_tg_call(bot.send_message(
            callback.from_user.id, 
            f"❗️ Takneeki samasya: <b>{movie['title']}</b> ki file uplabdh nahi hai. File channel se delete ho chuki hai ya **File ID** invalid hai." + admin_hint
        ))
        
        await safe_tg_call(callback.message.edit_text(f"❌ <b>{movie['title']}</b> ki file send nahi ho payi. Upar chat check karein."))

# ============ ADMIN COMMANDS ============
@dp.message(Command("stats"), AdminFilter())
@handler_timeout(15)
async def stats_command(message: types.Message):
    await safe_db_call(db.add_user(message.from_user.id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))
    user_count = await safe_db_call(db.get_user_count(), default=0)
    movie_count = await safe_db_call(db.get_movie_count(), default=0)
    concurrent_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    
    stats_msg = f"""📊 <b>Live System Statistics</b>

🟢 Active Users (5m): {concurrent_users:,}/{CURRENT_CONC_LIMIT}
👥 Total Users: {user_count:,}
🎬 Indexed Movies: {movie_count:,}
⚙️ Status: Operational
⏰ Uptime: {get_uptime()}"""
    
    await safe_tg_call(message.answer(stats_msg))

@dp.message(Command("broadcast"), AdminFilter())
@handler_timeout(1800) # 30 minute timeout
async def broadcast_command(message: types.Message):
    if not message.reply_to_message:
        await safe_tg_call(message.answer("❌ Broadcast ke liye kisi message ko reply karein."))
        return
    users = await safe_db_call(db.get_all_users(), timeout=30, default=[])
    if not users:
        await safe_tg_call(message.answer("❌ Database mein koi active user nahi mila."))
        return
        
    total_users = len(users)
    success, failed = 0, 0
    
    progress_msg = await safe_tg_call(message.answer(f"📤 Broadcasting to <b>{total_users}</b> users…"))
    
    for uid in users:
        # `safe_tg_call` ab `False` return karega agar bot block ho
        result = await safe_tg_call(message.reply_to_message.copy_to(uid), timeout=3) 
        
        if result:
            success += 1
        else:
            failed += 1
            # Agar user ne bot block kar diya hai, toh use DB se inactive kar dein
            await safe_db_call(db.deactivate_user(uid), timeout=3)
            
        # Har 100 user par progress update karein
        if (success + failed) % 100 == 0 and (success + failed) > 0 and progress_msg:
            try:
                await safe_tg_call(progress_msg.edit_text(f"""📤 Broadcasting…
✅ Sent: {success} | ❌ Failed (or Blocked): {failed} | ⏳ Total: {total_users}"""))
            except TelegramBadRequest:
                pass # Message not modified
        
        await asyncio.sleep(0.05) # 20 messages per second (Telegram limit)
        
    if progress_msg:
        await safe_tg_call(progress_msg.edit_text(f"""✅ <b>Broadcast Complete!</b>

• Success: {success}
• Failed (or Blocked): {failed}"""))

@dp.message(Command("cleanup_users"), AdminFilter())
@handler_timeout(60) # Cleanup mein time lag sakta hai
async def cleanup_users_command(message: types.Message):
    await safe_tg_call(message.answer("🧹 30 din se inactive users ko clean kiya ja raha hai…"))
    removed_count = await safe_db_call(db.cleanup_inactive_users(days=30), timeout=45, default=0)
    new_count = await safe_db_call(db.get_user_count(), default=0)
    
    await safe_tg_call(message.answer(f"""✅ <b>Cleanup complete!</b>
• Deactivated: {removed_count}
• Active Users now: {new_count}"""))

@dp.message(Command("add_movie"), AdminFilter())
@handler_timeout(20)
async def add_movie_command(message: types.Message):
    if not message.reply_to_message or not (message.reply_to_message.video or message.reply_to_message.document):
        await safe_tg_call(message.answer("❌ Kripya video/document par reply karke command bhejein: /add_movie imdb_id | title | year"))
        return
    try:
        full_command = message.text.replace("/add_movie", "", 1).strip()
        parts = [p.strip() for p in full_command.split("|")]
        if len(parts) < 2:
            await safe_tg_call(message.answer("❌ Format galat hai; use: /add_movie imdb_id | title | year"))
            return
        imdb_id = parts[0]
        title = parts[1]
        year = parts[2] if len(parts) > 2 else None
    except Exception:
        await safe_tg_call(message.answer("❌ Format galat hai; use: /add_movie imdb_id | title | year"))
        return
        
    file_id = message.reply_to_message.video.file_id if message.reply_to_message.video else message.reply_to_message.document.file_id
    
    success = await safe_db_call(db.add_movie(
        imdb_id=imdb_id, title=title, year=year,
        file_id=file_id, message_id=message.reply_to_message.message_id, channel_id=message.reply_to_message.chat.id
    ), default=False)
    
    if success is True:
        await safe_tg_call(message.answer(f"✅ Movie '<b>{title}</b>' add ho gayi hai."))
    elif success == "duplicate":
        await safe_tg_call(message.answer(f"⚠️ Movie '<b>{title}</b>' pehle se database mein hai (IMDB ID ya File ID duplicate hai)."))
    else:
        await safe_tg_call(message.answer("❌ Movie add karne me error aaya (DB connection issue)."))

@dp.message(Command("import_json"), AdminFilter())
@handler_timeout(1800) # 30 minute timeout
async def import_json_command(message: types.Message):
    if not message.reply_to_message or not message.reply_to_message.document:
        await safe_tg_call(message.answer("❌ Kripya ek .json file par reply karke command bhejein."))
        return

    doc = message.reply_to_message.document
    if doc.mime_type != "application/json" and not doc.file_name.endswith(".json"):
        await safe_tg_call(message.answer("❌ File .json format mein honi chahiye."))
        return

    await safe_tg_call(message.answer("⏳ JSON file download ki ja rahi hai..."))

    try:
        file_io = io.BytesIO()
        # File download ke liye time badhayein
        file = await bot.get_file(doc.file_id)
        await bot.download_file(file.file_path, file_io, timeout=60)
        file_io.seek(0)
        data = file_io.read().decode('utf-8')
        movies = json.loads(data)
        
        if not isinstance(movies, list):
             await safe_tg_call(message.answer("❌ Error: JSON file ek list (array) nahi hai."))
             return

    except Exception as e:
        await safe_tg_call(message.answer(f"❌ JSON file padhne ya download karne me error: {e}"))
        return

    total = len(movies)
    added = 0
    skipped = 0
    failed = 0
    
    progress_msg = await safe_tg_call(message.answer(f"⏳ JSON import shuru ho raha hai... Total {total} movies."))
    if not progress_msg:
        logger.error("Could not send progress message for JSON import.")
        return

    for i, item in enumerate(movies):
        try:
            # --- JSON structure ke hisaab se ---
            # Aapki file mein 'file_id' aur 'title' (jo filename hai) keys hain
            file_id = item.get("file_id")
            filename = item.get("title") # 'title' key mein filename hai
            
            if not file_id or not filename:
                logger.warning(f"Skipping invalid item (missing file_id or title): {item}")
                skipped += 1
                continue
            
            # Hum file_id se ek unique IMDB ID banayenge, kyonki JSON mein real IMDB ID nahi hai
            # Yeh duplicates ko rokne ke liye zaroori hai
            imdb_id = f"json_{hashlib.md5(file_id.encode()).hexdigest()}"
            
            # Filename se title aur year nikaalein
            parsed_info = parse_filename(filename)
            
            # Database mein add karein
            success = await safe_db_call(db.add_movie(
                imdb_id=imdb_id,
                title=parsed_info["title"],
                year=parsed_info["year"],
                file_id=file_id,
                message_id=AUTO_MESSAGE_ID_PLACEHOLDER, # Kyonki yeh file se aa raha hai, iska koi real message_id nahi hai
                channel_id=0 # 0 ya koi placeholder
            ), default=False)
            
            if success is True:
                added += 1
            elif success == "duplicate":
                # Yeh error nahi hai, yeh feature hai (duplicate skip karna)
                skipped += 1
            else:
                failed += 1
                
        except Exception as e:
            logger.error(f"JSON import error for item {i}: {e}")
            failed += 1
        
        # Har 50 items par progress update karein
        if (i + 1) % 50 == 0 or (i + 1) == total:
            try:
                await safe_tg_call(progress_msg.edit_text(
                    f"⏳ Processing... {i+1}/{total}\n"
                    f"✅ Added: {added}\n"
                    f"↷ Skipped: {skipped}\n"
                    f"❌ Failed (DB error): {failed}"
                ))
            except TelegramBadRequest as e:
                if "message is not modified" not in str(e):
                    logger.warning(f"Progress update failed: {e}")
            
            await asyncio.sleep(0.5) # Thoda sa pause taaki API limit hit na ho

    # --- Clearer final message ---
    await safe_tg_call(progress_msg.edit_text(
        f"✅ <b>JSON Import Complete!</b>\n\n"
        f"• Total Items: {total}\n"
        f"• Successfully Added: {added}\n"
        f"• Skipped: {skipped} (Reason: file_id ya imdb_id pehle se database mein hai)\n"
        f"• Failed (DB errors): {failed}"
    ))

@dp.message(Command("remove_dead_movie"), AdminFilter())
@handler_timeout(15)
async def remove_dead_movie_command(message: types.Message):
    args = message.text.split()
    if len(args) < 2:
        await safe_tg_call(message.answer("❌ Use: /remove_dead_movie IMDB_ID"))
        return
    
    imdb_id = args[1].strip()
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id))
    
    if not movie:
        await safe_tg_call(message.answer(f"❌ Movie with IMDB ID <code>{imdb_id}</code> not found in database."))
        return
    
    success = await safe_db_call(db.remove_movie_by_imdb(imdb_id), default=False)
    
    if success:
        await safe_tg_call(message.answer(f"✅ Successfully removed movie: <b>{movie['title']}</b> (IMDB: {imdb_id})"))
        logger.info(f"Admin removed dead movie: {movie['title']} (IMDB: {imdb_id})")
    else:
        await safe_tg_call(message.answer(f"❌ Failed to remove movie (database error)."))

@dp.message(Command("rebuild_index"), AdminFilter())
@handler_timeout(300) # 5 minute timeout
async def rebuild_index_command(message: types.Message):
    await safe_tg_call(message.answer("🔧 Clean titles reindex ho rahe hain… yeh operation batched hai."))
    # Is operation ko 3 min ka DB timeout dein
    result = await safe_db_call(db.rebuild_clean_titles(), timeout=180, default=(0, 0))
    updated, total = result
    await safe_tg_call(message.answer(f"✅ Reindex complete: Updated <b>{updated}</b> of ~{total} titles. Ab search feature tez aur sahi kaam karega."))

@dp.message(Command("export_csv"), AdminFilter())
@handler_timeout(60) # Export mein time lag sakta hai
async def export_csv_command(message: types.Message):
    args = message.text.split()
    if len(args) < 2 or args[1] not in ("users", "movies"):
        await safe_tg_call(message.answer("Use: /export_csv users|movies [limit]"))
        return
    kind = args[1]
    limit = int(args[2]) if len(args) > 2 and args[2].isdigit() else 2000
    
    if kind == "users":
        rows = await safe_db_call(db.export_users(limit=limit), timeout=30, default=[])
        if not rows:
            await safe_tg_call(message.answer("❌ No user data or DB error."))
            return
        header = "user_id,username,first_name,last_name,joined_date,last_active,is_active\n"
        csv_data = [
            f"{r['user_id']},{r['username'] or ''},{r['first_name'] or ''},{r['last_name'] or ''},{r['joined_date']},{r['last_active']},{r['is_active']}"
            for r in rows
        ]
        csv = header + "\n".join(csv_data)
        await safe_tg_call(message.answer_document(BufferedInputFile(csv.encode("utf-8"), filename="users.csv"), caption=f"Users export (limit {limit})"))
    else:
        rows = await safe_db_call(db.export_movies(limit=limit), timeout=30, default=[])
        if not rows:
            await safe_tg_call(message.answer("❌ No movie data or DB error."))
            return
        header = "imdb_id,title,year,channel_id,message_id,added_date\n"
        csv_data = [
            f"{r['imdb_id']},\"{r['title'].replace('"', '""')}\",{r['year'] or ''},{r['channel_id']},{r['message_id']},{r['added_date']}"
            for r in rows
        ]
        csv = header + "\n".join(csv_data)
        await safe_tg_call(message.answer_document(BufferedInputFile(csv.encode("utf-8"), filename="movies.csv"), caption=f"Movies export (limit {limit})"))

@dp.message(Command("set_limit"), AdminFilter())
@handler_timeout(10)
async def set_limit_command(message: types.Message):
    global CURRENT_CONC_LIMIT
    args = message.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await safe_tg_call(message.answer(f"Use: /set_limit N (current: {CURRENT_CONC_LIMIT})"))
        return
    val = int(args[1])
    if val < 5 or val > 100:
        await safe_tg_call(message.answer("Allowed range: 5–100 for safety on free tier."))
        return
    CURRENT_CONC_LIMIT = val
    await safe_tg_call(message.answer(f"✅ Concurrency limit set to <b>{CURRENT_CONC_LIMIT}</b>"))

@dp.channel_post()
@handler_timeout(20)
async def auto_index_handler(message: types.Message):
    # Sirf configured library channel se hi index karein
    if message.chat.id != LIBRARY_CHANNEL_ID or not (message.video or message.document):
        return
        
    caption = message.caption or ""
    movie_info = extract_movie_info(caption)
    
    if not movie_info:
        logger.warning(f"Auto-index skipped: could not parse caption: {caption[:80]}")
        return
    
    file_id = message.video.file_id if message.video else message.document.file_id
    # Agar caption mein IMDB ID nahi hai, toh message_id se ek unique ID banayein
    imdb_id = movie_info.get("imdb_id", f"auto_{message.message_id}") 
    
    success = await safe_db_call(db.add_movie(
        imdb_id=imdb_id,
        title=movie_info.get("title"),
        year=movie_info.get("year"),
        file_id=file_id,
        message_id=message.message_id,
        channel_id=message.chat.id,
    ), default=False)
    
    if success is True:
        logger.info(f"Auto-indexed: {movie_info.get('title')}")
    elif success == "duplicate":
        logger.info(f"Auto-index skipped (duplicate): {movie_info.get('title')}")
    else:
        logger.error(f"Auto-index failed: {movie_info.get('title')} (DB connection issue).")

# Error handler (agar koi handler fail ho)
@dp.errors()
async def errors_handler(update: types.Update, exception: Exception):
    logger.error(f"Unhandled error in dispatcher: {exception}", exc_info=True)
    
    # User ko batayein ki kuch galat hua
    try:
        if update.message:
            await update.message.answer("❗️ Ek unexpected error hua. Kripya dobara try karein.")
        elif update.callback_query:
            await update.callback_query.message.answer("❗️ Ek unexpected error hua. Kripya dobara try karein.")
    except Exception as e:
        logger.error(f"Error handler failed to send message: {e}")
