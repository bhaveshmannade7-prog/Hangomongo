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
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("bot")

# ============ CONFIGURATION ============
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "123456789")) 
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", "-1003138949015"))
JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "MOVIEMAZASU")
USER_GROUP_USERNAME = os.getenv("USER_GROUP_USERNAME", "THEGREATMOVIESL9")

# --- FIX: Ab hum 5 alag variable ke bajaye seedha DATABASE_URL ka istemaal karenge ---
DATABASE_URL = os.getenv("DATABASE_URL")

RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
PUBLIC_URL = os.getenv("PUBLIC_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

DEFAULT_CONCURRENT_LIMIT = int(os.getenv("CONCURRENT_LIMIT", "35"))
ACTIVE_WINDOW_MINUTES = int(os.getenv("ACTIVE_WINDOW_MINUTES", "5"))
CURRENT_CONC_LIMIT = DEFAULT_CONCURRENT_LIMIT

ALTERNATE_BOTS = ["Moviemaza91bot", "Moviemaza92bot", "Mazamovie9bot"]

# ============ OPTIMIZED TIMEOUTS FOR FREE TIER ============
HANDLER_TIMEOUT = 15
DB_OP_TIMEOUT = 5
TG_OP_TIMEOUT = 3

# ============ SEMAPHORE FOR DB OPERATIONS ============
DB_SEMAPHORE = asyncio.Semaphore(10)

# --- FIX: Zaroori variables ko check karein ---
if not BOT_TOKEN or not DATABASE_URL:
    logger.critical("Missing BOT_TOKEN or DATABASE_URL! Exiting.")
    if not BOT_TOKEN:
        logger.critical("BOT_TOKEN is MISSING.")
    if not DATABASE_URL:
        logger.critical("DATABASE_URL is MISSING. (Kripya Render mein 5 puraane DB variables delete karke ise add karein).")
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
# Database class ab DATABASE_URL string se seedha connect hoga
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
                    if args and hasattr(args[0], 'answer'):
                        await bot.send_message(args[0].from_user.id, "⚠️ Request timeout - kripya dobara try karein.", parse_mode=ParseMode.HTML)
                except Exception:
                    pass
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
    
    match = re.search(r"\(((19|20)\d{2})\)", filename)
    if match:
        year = match.group(1)
    else:
        matches = re.findall(r"\b((19|20)\d{2})\b", filename)
        if matches:
            year = matches[-1]
    
    title = os.path.splitext(filename)[0]
    
    return {"title": title, "year": year}


def overflow_message(active_users: int) -> str:
    msg = f"""⚠️ <b>Capacity Reached</b>

Hamari free-tier service is waqt <b>{CURRENT_CONC_LIMIT}</b> concurrent users par chal rahi hai 
aur abhi <b>{active_users}</b> active hain; nayi requests temporarily hold par hain.

Be-rukavat access ke liye alternate bots use karein; neeche se choose karke turant dekhna shuru karein."""
    return msg

# ============ EVENT LOOP MONITOR ============
async def monitor_event_loop():
    """Monitors event loop for blocking operations."""
    while True:
        try:
            start = asyncio.get_event_loop().time()
            await asyncio.sleep(0)
            lag = asyncio.get_event_loop().time() - start
            if lag > 0.1:
                logger.warning(f"⚠️ Event loop lag detected: {lag:.3f}s")
            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"Event loop monitor error: {e}")
            await asyncio.sleep(60)

# ============ LIFESPAN MANAGEMENT ============
@asynccontextmanager
async def lifespan(app: FastAPI):
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    loop = asyncio.get_event_loop()
    loop.set_default_executor(executor)
    logger.info("ThreadPoolExecutor initialized with max_workers=10 (Free Tier optimized).")
    
    # db.init_db() ab DATABASE_URL ka istemaal karke run hoga
    await db.init_db() 
    
    monitor_task = asyncio.create_task(monitor_event_loop())
    logger.info("Event loop monitor started.")

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
        except Exception as e:
            logger.error(f"Webhook setup error: {e}", exc_info=True)
    else:
        logger.warning("WEBHOOK_URL is empty; public URL required.")

    yield

    # Cleanup
    monitor_task.cancel()
    try:
        await asyncio.sleep(2) 
        await bot.delete_webhook(drop_pending_updates=True)
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
        if WEBHOOK_SECRET:
            if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
                logger.warning("Invalid webhook secret token")
                raise HTTPException(status_code=403, detail="Forbidden")
        
        telegram_update = Update(**update)
        
        async def _process_with_timeout():
            try:
                await asyncio.wait_for(_process_update(telegram_update), timeout=15)
            except asyncio.TimeoutError:
                logger.error(f"Update processing timed out: {telegram_update.update_id}")
        
        background_tasks.add_task(_process_with_timeout)
        return {"ok": True}
    except Exception as e:
        logger.error(f"Webhook processing error: {e}", exc_info=True)
        return {"ok": False}

# ============ HEALTH CHECK ENDPOINT ============
@app.get("/")
async def ping():
    return {"status": "ok", "service": "Movie Bot is Live", "uptime": get_uptime()}

@app.get("/health")
async def health_check():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat(), "uptime": get_uptime()}

# ============ CAPACITY MANAGEMENT ============
async def ensure_capacity_or_inform(message: types.Message) -> bool:
    """Checks capacity, updates user's last_active time, and enforces limit."""
    user_id = message.from_user.id
    
    await safe_db_call(
        db.add_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name),
        timeout=5
    )

    if user_id == ADMIN_USER_ID:
        return True
    
    active = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), timeout=5, default=0)
    if active > CURRENT_CONC_LIMIT: 
        try:
            await asyncio.wait_for(message.answer(overflow_message(active), reply_markup=get_full_limit_keyboard()), timeout=3)
        except:
            pass
        return False
        
    return True

# ============ BOT HANDLERS ============
# (Yahan se neeche ka poora code waisa hi hai, usmein koi badlaav nahi hai)
# ... (poora code copy-paste karein jaisa pehle tha)
@dp.message(CommandStart())
@handler_timeout(15)
async def start_command(message: types.Message):
    user_id = message.from_user.id
    try:
        bot_info = await asyncio.wait_for(bot.get_me(), timeout=5)
    except (asyncio.TimeoutError, TelegramAPIError):
        await safe_tg_call(message.answer("⚠️ Technical error - kripya dobara /start karein"))
        return

    if user_id == ADMIN_USER_ID:
        await safe_db_call(db.add_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))
        user_count = await safe_db_call(db.get_user_count(), default=0)
        movie_count = await safe_db_call(db.get_movie_count(), default=0)
        concurrent_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        
        admin_message = f"""👑 <b>Admin Console: @{bot_info.username}</b>
Access Level: Full Management

<b>System Performance & Metrics</b>
• Active Users (5m): {concurrent_users:,}/<b>{CURRENT_CONC_LIMIT}</b>
• Total Users: {user_count:,}
• Indexed Movies: {movie_count:,}
• Uptime: {get_uptime()}

<b>Management Commands</b>
• /stats — Real-time stats
• /broadcast — Reply to message to send
• /import_json — Reply to a .json file to import
• /cleanup_users — Deactivate inactive users
• /add_movie — Reply: /add_movie imdb_id | title | year
• /remove_dead_movie IMDB_ID — Remove invalid movie
• /rebuild_index — Recompute clean titles
• /export_csv users|movies [limit]
• /set_limit N — Change concurrency cap"""
        
        await safe_tg_call(message.answer(admin_message))
        return

    if not await ensure_capacity_or_inform(message):
        return

    welcome_text = f"""🎬 Namaskar <b>{message.from_user.first_name}</b>!

Movie Search Bot me swagat hai — bas title ka naam bhejein; behtar results ke liye saal bhi likh sakte hain (jaise <b>Kantara 2022</b>).

Hamare Channel aur Group join karne ke baad niche "I Have Joined Both" dabayen aur turant access paayen.
Aap help ke liye /help command bhi use kar sakte hain."""
    
    await safe_tg_call(message.answer(welcome_text, reply_markup=get_join_keyboard()))

@dp.message(Command("help"))
@handler_timeout(15)
async def help_command(message: types.Message):
    await safe_db_call(db.add_user(message.from_user.id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))
    
    help_text = """❓ <b>Bot Ka Upyog Kaise Karein</b>

1.  <b>Search Karein:</b> Movie/Show ka naam seedha message mein bhejein. (Example: <code>Jawan</code> ya <code>Mirzapur Season 1</code>)
2.  <b>Behtar Results:</b> Naam ke saath saal (year) zaroor jodein. (Example: <code>Pushpa 2021</code>)
3.  <b>Bot Rukne Par:</b> Agar bot kuch der baad response dena band kar de, toh iska matlab hai ki server so gaya hai. Kripya **thoda intezar** karein ya bot ko dobara /start karein.
    
Agar Bot slow ho ya ruk jaaye, toh <b>Alternate Bots</b> use karein jo /start karne par dikhte hain."""
    
    await safe_tg_call(message.answer(help_text))

@dp.callback_query(F.data == "check_join")
@handler_timeout(15)
async def check_join_callback(callback: types.CallbackQuery):
    await safe_tg_call(callback.answer("Verifying…"))
    
    active_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    if active_users > CURRENT_CONC_LIMIT and callback.from_user.id != ADMIN_USER_ID:
        await safe_tg_call(callback.message.edit_text(overflow_message(active_users)))
        await safe_tg_call(bot.send_message(callback.from_user.id, "Alternate bots ka upyog karein:", reply_markup=get_full_limit_keyboard()))
        return
            
    success_text = f"""✅ Verification successful, <b>{callback.from_user.first_name}</b>!

Ab aap library access kar sakte hain — apni pasand ki title ka naam bhejein.

Free tier capacity: {CURRENT_CONC_LIMIT}, abhi active: {active_users}."""
        
    result = await safe_tg_call(callback.message.edit_text(success_text, reply_markup=None))
    if not result:
        await safe_tg_call(bot.send_message(callback.from_user.id, success_text, reply_markup=None))

@dp.message(F.text & ~F.text.startswith("/") & (F.chat.type == "private"))
@handler_timeout(20)
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
        return
    
    top = await safe_db_call(db.super_search_movies_advanced(original_query, limit=20), timeout=12, default=[])
    
    if not top:
        await safe_tg_call(searching_msg.edit_text(
            f"🥲 Maaf kijiye, <b>{original_query}</b> ke liye match nahi mila; spelling/variant try karein (jaise Katara/Katra)."
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
        
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id), timeout=6)
    if not movie:
        await safe_tg_call(callback.message.edit_text("❌ Yeh movie ab database me uplabdh nahi hai."))
        return
        
    success = False
    
    await safe_tg_call(callback.message.edit_text(f"✅ <b>{movie['title']}</b> — file forward ki ja rahi hai, kripya chat check karein."))
    
    try:
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
        
        if movie["message_id"] == AUTO_MESSAGE_ID_PLACEHOLDER or 'message to forward not found' in forward_failed_msg or 'bad request: message to forward not found' in forward_failed_msg:
            try:
                await asyncio.wait_for(
                    bot.send_document(
                        chat_id=callback.from_user.id,
                        document=movie["file_id"], 
                        caption=f"🎬 <b>{movie['title']}</b> ({movie['year'] or 'Year not specified'})" 
                    ),
                    timeout=TG_OP_TIMEOUT 
                )
                success = True
                
            except (asyncio.TimeoutError, TelegramAPIError) as e2:
                logger.error(f"❌ DEAD FILE: Movie '{movie['title']}' (IMDB: {imdb_id}) failed both forward and send_document. Error: {type(e2).__name__}. Use: /remove_dead_movie {imdb_id}")
                
            except Exception as e3:
                logger.error(f"Unexpected error during send_document fallback for {imdb_id}: {e3}")
                
    if not success:
        admin_hint = f"Admin Hint: /remove_dead_movie {imdb_id}" if callback.from_user.id == ADMIN_USER_ID else ""
        
        await safe_tg_call(bot.send_message(
            callback.from_user.id, 
            f"❗️ Takneeki samasya: <b>{movie['title']}</b> ki file uplabdh nahi hai. File channel se delete ho chuki hai ya **File ID** invalid hai. {admin_hint}"
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
@handler_timeout(600)
async def broadcast_command(message: types.Message):
    if not message.reply_to_message:
        await safe_tg_call(message.answer("❌ Broadcast ke liye kisi message ko reply karein."))
        return
    users = await safe_db_call(db.get_all_users(), timeout=10, default=[])
    total_users = len(users)
    success, failed = 0, 0
    
    progress_msg = await safe_tg_call(message.answer(f"📤 Broadcasting to <b>{total_users}</b> users…"))
    
    for uid in users:
        result = await safe_tg_call(message.reply_to_message.copy_to(uid), timeout=3)
        if result:
            success += 1
        else:
            failed += 1
            
        if (success + failed) % 100 == 0 and (success + failed) > 0 and progress_msg:
            await safe_tg_call(progress_msg.edit_text(f"""📤 Broadcasting…
✅ Sent: {success} | ❌ Failed: {failed} | ⏳ Total: {total_users}"""))
        await asyncio.sleep(0.05) 
        
    if progress_msg:
        await safe_tg_call(progress_msg.edit_text(f"""✅ <b>Broadcast Complete!</b>

• Success: {success}
• Failed: {failed}"""))

@dp.message(Command("cleanup_users"), AdminFilter())
@handler_timeout(30)
async def cleanup_users_command(message: types.Message):
    await safe_tg_call(message.answer("🧹 Inactive users ko clean kiya ja raha hai…"))
    removed_count = await safe_db_call(db.cleanup_inactive_users(days=30), timeout=15, default=0)
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
        
    existing = await safe_db_call(db.get_movie_by_imdb(imdb_id))
    if existing:
        await safe_tg_call(message.answer("⚠️ Is IMDB ID se movie pehle se maujood hai."))
        return
        
    file_id = message.reply_to_message.video.file_id if message.reply_to_message.video else message.reply_to_message.document.file_id
    success = await safe_db_call(db.add_movie(
        imdb_id=imdb_id, title=title, year=year,
        file_id=file_id, message_id=message.reply_to_message.message_id, channel_id=message.reply_to_message.chat.id
    ), default=False)
    
    if success:
        await safe_tg_call(message.answer(f"✅ Movie '<b>{title}</b>' add ho gayi hai."))
    else:
        await safe_tg_call(message.answer("❌ Movie add karne me error aaya (DB connection issue)."))

@dp.message(Command("import_json"), AdminFilter())
@handler_timeout(1800)
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
        file = await bot.get_file(doc.file_id)
        await bot.download_file(file.file_path, file_io)
        file_io.seek(0)
        data = file_io.read().decode('utf-8')
        movies = json.loads(data)
        
        if not isinstance(movies, list):
             await safe_tg_call(message.answer("❌ Error: JSON file ek list (array) nahi hai."))
             return

    except Exception as e:
        await safe_tg_call(message.answer(f"❌ JSON file padhne me error: {e}"))
        return

    total = len(movies)
    added = 0
    skipped = 0
    failed = 0
    
    progress_msg = await safe_tg_call(message.answer(f"⏳ JSON import shuru ho raha hai... Total {total} movies."))
    if not progress_msg:
        return

    for i, item in enumerate(movies):
        try:
            file_id = item.get("file_id")
            filename = item.get("title")
            
            if not file_id or not filename:
                skipped += 1
                continue
            
            imdb_id = f"json_{hashlib.md5(file_id.encode()).hexdigest()}"
            
            existing = await safe_db_call(db.get_movie_by_imdb(imdb_id), timeout=3)
            if existing:
                skipped += 1
                continue
                
            parsed_info = parse_filename(filename)
            
            success = await safe_db_call(db.add_movie(
                imdb_id=imdb_id,
                title=parsed_info["title"],
                year=parsed_info["year"],
                file_id=file_id,
                message_id=AUTO_MESSAGE_ID_PLACEHOLDER,
                channel_id=0
            ), default=False)
            
            if success:
                added += 1
            else:
                failed += 1
                
        except Exception as e:
            logger.error(f"JSON import error for item {i}: {e}")
            failed += 1
        
        if (i + 1) % 50 == 0 or (i + 1) == total:
            try:
                await safe_tg_call(progress_msg.edit_text(
                    f"⏳ Processing... {i+1}/{total}\n"
                    f"✅ Added: {added}\n"
                    f"↷ Skipped (duplicate/invalid): {skipped}\n"
                    f"❌ Failed (DB error): {failed}"
                ))
            except TelegramBadRequest as e:
                if "message is not modified" not in str(e):
                    logger.warning(f"Progress update failed: {e}")
            
            await asyncio.sleep(0.5)

    await safe_tg_call(progress_msg.edit_text(
        f"✅ <b>JSON Import Complete!</b>\n\n"
        f"• Total Items: {total}\n"
        f"• Successfully Added: {added}\n"
        f"• Skipped (duplicate/invalid): {skipped}\n"
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
@handler_timeout(300)
async def rebuild_index_command(message: types.Message):
    await safe_tg_call(message.answer("🔧 Clean titles reindex ho रहे हैं… yeh operation batched hai."))
    result = await safe_db_call(db.rebuild_clean_titles(), timeout=180, default=(0, 0))
    updated, total = result
    await safe_tg_call(message.answer(f"✅ Reindex complete: Updated <b>{updated}</b> of ~{total} titles. Ab search feature tez aur sahi kaam karega."))

@dp.message(Command("export_csv"), AdminFilter())
@handler_timeout(60)
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
        header = """user_id,username,first_name,last_name,joined_date,last_active,is_active
"""
        csv = header + "\n".join([
            f"{r['user_id']},{r['username'] or ''},{r['first_name'] or ''},{r['last_name'] or ''},{r['joined_date']},{r['last_active']},{r['is_active']}"
            for r in rows
        ])
        await safe_tg_call(message.answer_document(BufferedInputFile(csv.encode("utf-8"), filename="users.csv"), caption="Users export"))
    else:
        rows = await safe_db_call(db.export_movies(limit=limit), timeout=30, default=[])
        if not rows:
            await safe_tg_call(message.answer("❌ No movie data or DB error."))
            return
        header = """imdb_id,title,year,channel_id,message_id,added_date
"""
        csv = header + "\n".join([
            f"{r['imdb_id']},{r['title'].replace(',', ' ')},{r['year'] or ''},{r['channel_id']},{r['message_id']},{r['added_date']}"
            for r in rows
        ])
        await safe_tg_call(message.answer_document(BufferedInputFile(csv.encode("utf-8"), filename="movies.csv"), caption="Movies export"))

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
    if message.chat.id != LIBRARY_CHANNEL_ID or not (message.video or message.document):
        return
    caption = message.caption or ""
    movie_info = extract_movie_info(caption)
    if not movie_info:
        logger.warning(f"Auto-index skipped: could not parse caption: {caption[:80]}")
        return
    
    file_id = message.video.file_id if message.video else message.document.file_id
    imdb_id = movie_info.get("imdb_id", f"auto_{message.message_id}") 
    
    existing = await safe_db_call(db.get_movie_by_imdb(imdb_id))
    if existing:
        logger.info(f"Movie already indexed: {movie_info.get('title')}")
        return
        
    success = await safe_db_call(db.add_movie(
        imdb_id=imdb_id,
        title=movie_info.get("title"),
        year=movie_info.get("year"),
        file_id=file_id,
        message_id=message.message_id,
        channel_id=message.chat.id,
    ), default=False)
    
    if success:
        logger.info(f"Auto-indexed: {movie_info.get('title')}")
    else:
        logger.error(f"Auto-index failed: {movie_info.get('title')} (DB connection issue).")
```eof

**Step 4: `database.py` (Zaroori Fix)**

Aapke `database.py` mein `asyncpg` ke liye ek zaroori `ssl` setting missing ho sakti hai. Niche di gayi `database.py` file ka istemaal karein. Yeh Supabase ke liye behtar hai.

```python:database.py (Updated for Supabase SSL):database.py
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
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', ' ', text)
    text = re.sub(r'\b(s|season)\s*\d{1,2}\b', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def _normalize_for_fuzzy(text: str) -> str:
    t = text.lower()
    t = re.sub(r'[^a-z0-9]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    t = t.replace('ph', 'f').replace('aa', 'a').replace('kh', 'k').replace('gh', 'g')
    t = t.replace('ck', 'k').replace('cq', 'k').replace('qu', 'k').replace('q', 'k')
    t = t.replace('x', 'ks').replace('c', 'k')
    return t

def _consonant_signature(text: str) -> str:
    t = _normalize_for_fuzzy(text)
    t = re.sub(r'[aeiou]', '', t)
    t = re.sub(r'\s+', '', t)
    return t

def _process_fuzzy_candidates(candidates: List[Tuple[str, str, str]], query: str) -> List[Dict]:
    if len(candidates) > 100:
        candidates = candidates[:100]
    
    q_clean = clean_text_for_search(query)
    q_cons = _consonant_signature(query)
    tokens = q_clean.split()
    
    results = []
    for imdb_id, title, clean_title in candidates:
        if not any(t in clean_title for t in tokens if t):
            continue
        
        s_w_ratio = fuzz.WRatio(clean_title, q_clean)
        
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
        
        # FIX: Supabase/Render ke liye SSL settings ko force karein
        # Yeh 'asyncpg' ko batayega ki SSL ka istemaal karna hai
        if 'supabase.co' in database_url:
             connect_args['ssl'] = 'require'
                
        if database_url.startswith('postgres://'):
            database_url = database_url.replace('postgres://', 'postgresql+asyncpg://', 1)
        elif database_url.startswith('postgresql://'):
             database_url = database_url.replace('postgresql://', 'postgresql+asyncpg://', 1)

        self.database_url = database_url
        
        self.engine = create_async_engine(
            database_url, 
            echo=False, 
            connect_args=connect_args, # Yahan connect_args add kiye gaye
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=300,
            pool_timeout=8,
        )
        
        self.SessionLocal = sessionmaker(
            self.engine, 
            expire_on_commit=False, 
            class_=AsyncSession
        )
        logger.info(f"Database engine initialized (SSL: {connect_args.get('ssl', 'default')}) with pooling: pool_size=5, max_overflow=10.")
        
    async def _handle_db_error(self, e: Exception) -> bool:
        if isinstance(e, (OperationalError, DisconnectionError)):
            logger.error(f"Critical DB error detected: {type(e).__name__}. Attempting engine re-initialization.", exc_info=True)
            try:
                await self.engine.dispose()
                # Connect args ko dobara pass karein
                connect_args = {}
                if 'supabase.co' in self.database_url:
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
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with self.engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                    
                    if self.engine.dialect.name == 'postgresql':
                        try:
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
                                
                                update_query = text(r"""
                                    UPDATE movies 
                                    SET clean_title = trim(
                                        regexp_replace(
                                            regexp_replace(
                                                regexp_replace(
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
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.SessionLocal() as session:
                    q_clean = clean_text_for_search(query)
                    tokens = q_clean.split()
                    
                    exact_stmt = select(Movie).where(Movie.clean_title == q_clean).limit(5)
                    exact_result = await session.execute(exact_stmt)
                    exact_matches = exact_result.scalars().all()
                    if exact_matches:
                        return [{'imdb_id': m.imdb_id, 'title': m.title} for m in exact_matches[:limit]]
                    
                    if tokens:
                        conditions = [Movie.clean_title.contains(token) for token in tokens if token]
                        partial_stmt = select(Movie.imdb_id, Movie.title, Movie.clean_title).where(or_(*conditions)).limit(150)
                        partial_result = await session.execute(partial_stmt)
                        candidates = partial_result.all()
                        
                        if candidates:
                            loop = asyncio.get_event_loop()
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
                    
                    update_query = text(r"""
                        UPDATE movies 
                        SET clean_title = trim(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        lower(title), 
                                    '[^a-z0-Nahi]+', ' ', 'g'),
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
```eof

**Step 4: Redeploy Karein**

Apni service ko "Manual Deploy" -> "Deploy latest commit" karke redeploy karein.

Kripya is `DATABASE_URL` wale tarike ko try karein. Yeh 5 alag-alag variable daalne se 10 guna behtar hai aur galti ki gunjaish nahi hai. Agar yeh fail hota hai, toh error log alag aana chahiye (jaise password galat hai), na ki "Network is unreachable".
