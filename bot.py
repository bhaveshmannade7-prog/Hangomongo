# -*- coding: utf-8 -*-
import os
import asyncio
import logging
import re
import io 
from datetime import datetime
from contextlib import asynccontextmanager
from typing import List, Dict
from functools import wraps
import concurrent.futures # For ThreadPoolExecutor

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart, BaseFilter
from aiogram.types import Update, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest 
from aiogram.client.default import DefaultBotProperties

from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks, Request, HTTPException

from database import Database, clean_text_for_search, AUTO_MESSAGE_ID_PLACEHOLDER 

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("bot")

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "123456789")) 
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", "-1003138949015"))
JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "MOVIEMAZASU")
USER_GROUP_USERNAME = os.getenv("USER_GROUP_USERNAME", "THEGREATMOVIESL9")
DATABASE_URL = os.getenv("DATABASE_URL")

RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
PUBLIC_URL = os.getenv("PUBLIC_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

DEFAULT_CONCURRENT_LIMIT = int(os.getenv("CONCURRENT_LIMIT", "35"))
ACTIVE_WINDOW_MINUTES = int(os.getenv("ACTIVE_WINDOW_MINUTES", "5"))
CURRENT_CONC_LIMIT = DEFAULT_CONCURRENT_LIMIT

ALTERNATE_BOTS = ["Moviemaza91bot", "Moviemaza92bot", "Mazamovie9bot"]

HANDLER_TIMEOUT = 25
DB_OP_TIMEOUT = 10
TG_OP_TIMEOUT = 5 

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

# --- Timeout Decorator ---
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

# --- Safe Wrappers ---
async def safe_db_call(coro, timeout=DB_OP_TIMEOUT, default=None):
    """Safely execute database call with timeout and simple failure return."""
    try:
        return await asyncio.wait_for(coro, timeout=timeout) 
    except asyncio.TimeoutError:
        logger.error(f"DB operation timed out after {timeout}s")
        return default
    except Exception as e:
        logger.debug(f"DB operation error (handled internally): {e}") 
        return default

async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT):
    """Safely execute Telegram API call with timeout. Does NOT suppress TelegramAPIError."""
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

def overflow_message(active_users: int) -> str:
    msg = f"""⚠️ <b>Capacity Reached</b>

Hamari free-tier service is waqt <b>{CURRENT_CONC_LIMIT}</b> concurrent users par chal rahi hai 
aur abhi <b>{active_users}</b> active hain; nayi requests temporarily hold par hain.

Be-rukavat access ke liye alternate bots use karein; neeche se choose karke turant dekhna shuru karein."""
    return msg

async def keep_db_alive():
    """Keeps the database connection pool active by running a lightweight query."""
    while True:
        await asyncio.sleep(60) 
        try:
            count = await safe_db_call(db.get_user_count(), timeout=5, default=0)
            logger.info(f"DB keepalive successful (users: {count}).")
        except Exception as e:
            logger.error(f"DB keepalive failed: {e}") 


@asynccontextmanager
async def lifespan(app: FastAPI):
    # CRITICAL FIX 1: Increase the default executor size for CPU-bound tasks (Fuzzy Search)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=50) 
    loop = asyncio.get_event_loop()
    loop.set_default_executor(executor)
    logger.info("Custom ThreadPoolExecutor initialized with max_workers=50.")
    
    await db.init_db() 
    db_task = asyncio.create_task(keep_db_alive()) 

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

    db_task.cancel()
    try:
        await asyncio.sleep(2) 
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        logger.error(f"Webhook delete error: {e}", exc_info=True)
        
    # CRITICAL FIX 2: Shutdown the executor gracefully
    executor.shutdown(wait=False)
    logger.info("ThreadPoolExecutor shut down.")

app = FastAPI(lifespan=lifespan)

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
        background_tasks.add_task(_process_update, telegram_update) 
        return {"ok": True}
    except Exception as e:
        logger.error(f"Webhook processing error: {e}", exc_info=True)
        return {"ok": False}

@app.get("/")
async def ping():
    return {"status": "ok", "service": "Movie Bot is Live", "uptime": get_uptime()}

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
        await safe_tg_call(message.answer(overflow_message(active), reply_markup=get_full_limit_keyboard()))
        return False
        
    return True

@dp.message(CommandStart())
@handler_timeout(20)
async def start_command(message: types.Message):
    user_id = message.from_user.id
    bot_info = await safe_tg_call(bot.get_me(), timeout=5)
    if not bot_info:
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
@handler_timeout(25)
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
    
    # DB search is the longest operation, set a reasonable timeout
    top = await safe_db_call(db.super_search_movies_advanced(original_query, limit=20), timeout=15, default=[])
    
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
        
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id), timeout=8)
    if not movie:
        await safe_tg_call(callback.message.edit_text("❌ Yeh movie ab database me uplabdh nahi hai."))
        return
        
    success = False
    
    await safe_tg_call(callback.message.edit_text(f"✅ <b>{movie['title']}</b> — file forward ki ja rahi hai, kripya chat check karein."))
    
    # --- CRITICAL FILE DELIVERY LOGIC ---
    
    # 1. Attempt to Forward (Primary method)
    try:
        # Use strict timeout on Telegram API call to prevent event loop blocking
        await asyncio.wait_for(
            bot.forward_message(
                chat_id=callback.from_user.id,
                from_chat_id=int(movie["channel_id"]),
                message_id=movie["message_id"],
            ),
            timeout=TG_OP_TIMEOUT # Strict 5s timeout
        )
        success = True
        
    except (asyncio.TimeoutError, TelegramAPIError) as e:
        forward_failed_msg = str(e).lower()
        logger.error(f"Forward failed for {imdb_id}: {e}")
        
        # 2. Fallback to send_document using file_id (If message not found or ID is placeholder)
        if movie["message_id"] == AUTO_MESSAGE_ID_PLACEHOLDER or 'message to forward not found' in forward_failed_msg or 'bad request: message to forward not found' in forward_failed_msg:
            try:
                # Use strict timeout for send_document to prevent event loop blocking
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
                # This is the final dead file log entry
                logger.error(f"❌ DEAD FILE: Movie '{movie['title']}' (IMDB: {imdb_id}) failed both forward and send_document. Error: {type(e2).__name__}. Use: /remove_dead_movie {imdb_id}")
                
            except Exception as e3:
                logger.error(f"Unexpected error during send_document fallback for {imdb_id}: {e3}")
                
    # 3. Final failure message
    if not success:
        admin_hint = f"Admin Hint: /remove_dead_movie {imdb_id}" if callback.from_user.id == ADMIN_USER_ID else ""
        
        await safe_tg_call(bot.send_message(
            callback.from_user.id, 
            f"❗️ Takneeki samasya: <b>{movie['title']}</b> ki file uplabdh nahi hai. File channel se delete ho chuki hai ya **File ID** invalid hai. {admin_hint}"
        ))
        
        await safe_tg_call(callback.message.edit_text(f"❌ <b>{movie['title']}</b> ki file send nahi ho payi. Upar chat check karein."))


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
        header = """user_id,username,first_name,last_name,joined_date,last_active,is_active\n"""
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
        header = """imdb_id,title,year,channel_id,message_id,added_date\n"""
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
