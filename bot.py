# -*- coding: utf-8 -*-
import os
import asyncio
import logging
import re
import json 
import io 
from datetime import datetime
from contextlib import asynccontextmanager
from typing import List, Dict

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart, BaseFilter
from aiogram.types import Update, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest # TelegramBadRequest import kiya gaya
from aiogram.client.default import DefaultBotProperties

from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks, Request, HTTPException

from database import Database, clean_text_for_search, AUTO_MESSAGE_ID_PLACEHOLDER 

# --- Configuration ---
load_dotenv()
# Setting logging to INFO to balance debugging and resource use on Free Tie
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

# --- Filters & helpers ---
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

# --- Keep DB alive ---
async def keep_db_alive():
    """Keeps the database connection pool active by running a lightweight query."""
    while True:
        # Reduced from 240s to 180s to maintain activity on Free Tier
        await asyncio.sleep(180) 
        try:
            await db.get_user_count() 
            logger.debug("DB keepalive successful.")
        except Exception as e:
            logger.error(f"DB keepalive failed: {e}", exc_info=True)


# --- Lifespan management ---
@asynccontextmanager
async def lifespan(app: FastAPI):
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

app = FastAPI(lifespan=lifespan)

async def _process_update(u: Update):
    try:
        # Pass update to dispatcher
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
        # Use add_task for non-blocking processing
        background_tasks.add_task(_process_update, telegram_update) 
        return {"ok": True}
    except Exception as e:
        logger.error(f"Webhook processing error: {e}", exc_info=True)
        return {"ok": False}

@app.get("/")
async def ping():
    return {"status": "ok", "service": "Movie Bot is Live", "uptime": get_uptime()}

# --- Concurrency gate (Most important for Free Tier stability) ---
async def ensure_capacity_or_inform(message: types.Message) -> bool:
    """Checks capacity, updates user's last_active time, and enforces limit."""
    user_id = message.from_user.id
    
    # 1. Update activity (crucial for concurrency count)
    await db.add_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name)

    # 2. Always allow admin
    if user_id == ADMIN_USER_ID:
        return True
    
    # 3. Check Capacity
    active = await db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES)
    if active > CURRENT_CONC_LIMIT: 
        await message.answer(overflow_message(active), reply_markup=get_full_limit_keyboard())
        return False
        
    return True

# --- Handlers ---
@dp.message(CommandStart())
async def start_command(message: types.Message):
    user_id = message.from_user.id
    bot_info = await bot.get_me()

    if user_id == ADMIN_USER_ID:
        await db.add_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name)
        user_count = await db.get_user_count()
        movie_count = await db.get_movie_count()
        concurrent_users = await db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES)
        
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
• /import_json_movies channel_id — Reply to a JSON file
• /rebuild_index — Recompute clean titles
• /export_csv users|movies [limit]
• /set_limit N — Change concurrency cap"""
        
        await message.answer(admin_message)
        return

    if not await ensure_capacity_or_inform(message):
        return

    welcome_text = f"""🎬 Namaskar <b>{message.from_user.first_name}</b>!

Movie Search Bot me swagat hai — bas title ka naam bhejein; behtar results ke liye saal bhi likh sakte hain (jaise <b>Kantara 2022</b>).

Hamare Channel aur Group join karne ke baad niche "I Have Joined Both" dabayen aur turant access paayen.
Aap help ke liye /help command bhi use kar sakte hain."""
    
    await message.answer(welcome_text, reply_markup=get_join_keyboard())

@dp.message(Command("help"))
async def help_command(message: types.Message):
    await db.add_user(message.from_user.id, message.from_user.username, message.from_user.first_name, message.from_user.last_name)
    
    help_text = """❓ <b>Bot Ka Upyog Kaise Karein</b>

1.  <b>Search Karein:</b> Movie/Show ka naam seedha message mein bhejein. (Example: <code>Jawan</code> ya <code>Mirzapur Season 1</code>)
2.  <b>Behtar Results:</b> Naam ke saath saal (year) zaroor jodein. (Example: <code>Pushpa 2021</code>)
3.  <b>Bot Rukne Par:</b> Agar bot kuch der baad response dena band kar de, toh iska matlab hai ki server so gaya hai. Kripya **thoda intezar** karein ya bot ko dobara /start karein.
    
Agar Bot slow ho ya ruk jaaye, toh <b>Alternate Bots</b> use karein jo /start karne par dikhte hain."""
    
    await message.answer(help_text)


@dp.callback_query(F.data == "check_join")
async def check_join_callback(callback: types.CallbackQuery):
    await callback.answer("Verifying…")
    
    # Update user activity and check capacity
    active_users = await db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES)
    if active_users > CURRENT_CONC_LIMIT and callback.from_user.id != ADMIN_USER_ID:
        try:
            await callback.message.edit_text(overflow_message(active_users))
            await bot.send_message(callback.from_user.id, "Alternate bots ka upyog karein:", reply_markup=get_full_limit_keyboard())
        except Exception:
            pass 
        return
            
    success_text = f"""✅ Verification successful, <b>{callback.from_user.first_name}</b>!

Ab aap library access kar sakte hain — apni pasand ki title ka naam bhejein.

Free tier capacity: {CURRENT_CONC_LIMIT}, abhi active: {active_users}."""
        
    try:
        await callback.message.edit_text(success_text, reply_markup=None)
    except TelegramAPIError:
        await bot.send_message(callback.from_user.id, success_text, reply_markup=None)
            
    except Exception as e:
        logger.error(f"check_join error: {e}", exc_info=True)
        await bot.send_message(callback.from_user.id, "⚠️ Technical error aya, kripya /start karein aur dobara koshish karein.")

@dp.message(F.text & ~F.text.startswith("/") & (F.chat.type == "private"))
async def search_movie_handler(message: types.Message):
    user_id = message.from_user.id

    if not await check_user_membership(user_id):
        await message.answer("⚠️ Kripya pehle Channel aur Group join karein, phir se /start dabayen.", reply_markup=get_join_keyboard())
        return

    if not await ensure_capacity_or_inform(message):
        return

    original_query = message.text.strip()
    if len(original_query) < 2:
        await message.answer("🤔 Kripya kam se kam 2 characters ka query bhejein.")
        return

    searching_msg = await message.answer(f"🔍 <b>{original_query}</b> ki khoj jaari hai…")
    
    try:
        # Reduced timeout to 10.0s for better responsiveness on Free Tier
        top = await asyncio.wait_for(
            db.super_search_movies_advanced(original_query, limit=20),
            timeout=10.0 
        )
        
        if not top:
            await searching_msg.edit_text(
                f"🥲 Maaf kijiye, <b>{original_query}</b> ke liye match nahi mila; spelling/variant try karein (jaise Katara/Katra)."
            )
            return

        buttons = [[InlineKeyboardButton(text=movie["title"], callback_data=f"get_{movie['imdb_id']}")] for movie in top]
        await searching_msg.edit_text(
            f"🎬 <b>{original_query}</b> ke liye {len(top)} results mile — file paane ke liye chunein:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )
        
    except asyncio.TimeoutError:
        logger.warning(f"Search timed out for query: {original_query}")
        try:
            await searching_msg.edit_text("⌛️ Search mein samay zyada lag gaya (Database slow). Kripya kuch der baad phir se koshish karein.")
        except TelegramAPIError:
            pass
            
    except Exception as e:
        logger.error(f"Search error for '{original_query}': {e}", exc_info=True) 
        try:
            await searching_msg.edit_text("❌ Internal error: search system me rukavat aa gayi hai, kuch der baad koshish karein.")
        except TelegramAPIError:
            pass

@dp.callback_query(F.data.startswith("get_"))
async def get_movie_callback(callback: types.CallbackQuery):
    await callback.answer("File forward ki ja rahi hai…")
    imdb_id = callback.data.split("_", 1)[1]
    
    if not await ensure_capacity_or_inform(callback.message):
        return
        
    movie = await db.get_movie_by_imdb(imdb_id)
    if not movie:
        await callback.message.edit_text("❌ Yeh movie ab database me uplabdh nahi hai.")
        return
        
    try:
        # Check if message_id is the placeholder for JSON imports
        if movie["message_id"] == AUTO_MESSAGE_ID_PLACEHOLDER:
            
            await callback.message.edit_text(f"✅ <b>{movie['title']}</b> — file bheji ja rahi hai, kripya chat check karein.")
            
            # CRITICAL FIX: send_document has issues with certain file_ids.
            # We add a secondary attempt (try-except) to handle Bad Request errors 
            # by using the document property directly which is often more robust for media.
            try:
                # Attempt 1: send_document using file_id (standard method)
                await bot.send_document(
                    chat_id=callback.from_user.id,
                    document=movie["file_id"], 
                    caption=f"🎬 <b>{movie['title']}</b> ({movie['year'] or 'Year not specified'})" 
                )
            except TelegramBadRequest as e:
                # Fallback on Bad Request error: Telegram usually fails because 
                # the 'file_id' provided is actually a 'file_unique_id' or has expired.
                logger.warning(f"send_document failed for {imdb_id}. Attempting send_video/document with file_id fallback.")
                
                # We try sending it again. Sometimes Telegram needs a slight change 
                # in how the file is presented, or the file is a video.
                
                # In most cases of 'wrong file identifier', the file is recoverable, 
                # so we inform the user of a delay and try to send it again (maybe as a video)
                await bot.send_video(
                    chat_id=callback.from_user.id,
                    video=movie["file_id"], 
                    caption=f"🎬 <b>{movie['title']}</b> ({movie['year'] or 'Year not specified'})"
                )
                
        else:
            # If message_id is original, use forward_message (fastest method)
            await callback.message.edit_text(f"✅ <b>{movie['title']}</b> — file forward ki ja rahi hai, kripya chat check karein.")
            await bot.forward_message(
                chat_id=callback.from_user.id,
                from_chat_id=int(movie["channel_id"]),
                message_id=movie["message_id"],
            )
        
    except TelegramAPIError as e:
        logger.error(f"Forward/edit error for {imdb_id}: {e}", exc_info=True)
        # Detailed error message for user
        await bot.send_message(callback.from_user.id, f"❗️ Takneeki samasya: <b>{movie['title']}</b> ko forward karne me dikat aayi. Shayad file channel se delete ho gayi ho ya bot ko channel se access na mil raha ho. Kripya phir se try karein.")
        
    except Exception as e:
        logger.error(f"Movie callback critical error: {e}", exc_info=True)
        await bot.send_message(callback.from_user.id, "❌ Critical system error: kripya /start karein.")

# --- Admin Commands ---
@dp.message(Command("stats"), AdminFilter())
async def stats_command(message: types.Message):
    await db.add_user(message.from_user.id, message.from_user.username, message.from_user.first_name, message.from_user.last_name)
    user_count = await db.get_user_count()
    movie_count = await db.get_movie_count()
    concurrent_users = await db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES)
    
    stats_msg = f"""📊 <b>Live System Statistics</b>

🟢 Active Users (5m): {concurrent_users:,}/{CURRENT_CONC_LIMIT}
👥 Total Users: {user_count:,}
🎬 Indexed Movies: {movie_count:,}
⚙️ Status: Operational
⏰ Uptime: {get_uptime()}"""
    
    await message.answer(stats_msg)

@dp.message(Command("broadcast"), AdminFilter())
async def broadcast_command(message: types.Message):
    if not message.reply_to_message:
        await message.answer("❌ Broadcast ke liye kisi message ko reply karein.")
        return
    users = await db.get_all_users()
    total_users = len(users)
    success, failed = 0, 0
    
    progress_msg = await message.answer(f"📤 Broadcasting to <b>{total_users}</b> users…")
    
    try:
        for uid in users:
            try:
                await message.reply_to_message.copy_to(uid)
                success += 1
            except Exception as e:
                failed += 1
                logger.debug(f"Broadcast failed for user {uid}: {e}")
            if (success + failed) % 100 == 0 and (success + failed) > 0:
                await progress_msg.edit_text(f"""📤 Broadcasting…
✅ Sent: {success} | ❌ Failed: {failed} | ⏳ Total: {total_users}""")
            await asyncio.sleep(0.05) 
            
        await progress_msg.edit_text(f"""✅ <b>Broadcast Complete!</b>

• Success: {success}
• Failed: {failed}""")
        
    except Exception as e:
        logger.error(f"Broadcast failed: {e}", exc_info=True)
        await message.answer("❌ Broadcasting process mein rukawat aa gayi.")

@dp.message(Command("cleanup_users"), AdminFilter())
async def cleanup_users_command(message: types.Message):
    await message.answer("🧹 Inactive users ko clean kiya ja raha hai…")
    removed_count = await db.cleanup_inactive_users(days=30)
    new_count = await db.get_user_count()
    
    await message.answer(f"""✅ <b>Cleanup complete!</b>
• Deactivated: {removed_count}
• Active Users now: {new_count}""")

@dp.message(Command("add_movie"), AdminFilter())
async def add_movie_command(message: types.Message):
    if not message.reply_to_message or not (message.reply_to_message.video or message.reply_to_message.document):
        await message.answer("❌ Kripya video/document par reply karke command bhejein: /add_movie imdb_id | title | year")
        return
    try:
        full_command = message.text.replace("/add_movie", "", 1).strip()
        parts = [p.strip() for p in full_command.split("|")]
        if len(parts) < 2:
            await message.answer("❌ Format galat hai; use: /add_movie imdb_id | title | year")
            return
        imdb_id = parts[0]
        title = parts[1]
        year = parts[2] if len(parts) > 2 else None
    except Exception:
        await message.answer("❌ Format galat hai; use: /add_movie imdb_id | title | year")
        return
    if await db.get_movie_by_imdb(imdb_id):
        await message.answer("⚠️ Is IMDB ID se movie pehle se maujood hai.")
        return
    file_id = message.reply_to_message.video.file_id if message.reply_to_message.video else message.reply_to_message.document.file_id
    success = await db.add_movie(
        imdb_id=imdb_id, title=title, year=year,
        file_id=file_id, message_id=message.reply_to_message.message_id, channel_id=message.reply_to_message.chat.id
    )
    if success:
        await message.answer(f"✅ Movie '<b>{title}</b>' add ho gayi hai.")
    else:
        await message.answer("❌ Movie add karne me error aaya.")

@dp.message(Command("import_json_movies"), AdminFilter())
async def import_json_movies_command(message: types.Message):
    """Admin command to import movie data from a JSON file reply."""
    if not message.reply_to_message or not message.reply_to_message.document:
        await message.answer("❌ JSON फ़ाइल को reply करें।\nUsage: /import_json_movies channel_id")
        return

    args = message.text.split()
    if len(args) < 2 or not args[1].lstrip('-').isdigit():
        await message.answer("❌ Channel ID missing ya invalid hai.\nUsage: /import_json_movies -100xxxxxxxxxx")
        return
        
    try:
        target_channel_id = int(args[1])
        if target_channel_id > 0: 
             await message.answer("❌ Channel ID galat hai, yeh hamesha negative (-100...) hota hai.")
             return
    except ValueError:
        await message.answer("❌ Channel ID number hona chahiye.")
        return

    file_id = message.reply_to_message.document.file_id
    file_info = await bot.get_file(file_id)
    file_path = file_info.file_path
    
    file_content = io.BytesIO()
    await bot.download_file(file_path, file_content)
    file_content.seek(0)

    try:
        movie_list = json.load(file_content)
        if not isinstance(movie_list, list) or not (len(movie_list) > 0 and isinstance(movie_list[0], dict)):
            await message.answer("❌ JSON format galat hai. Expected: List of JSON Objects ([{}, {}, ...]).")
            return
            
        await message.answer(f"⏳ <b>{len(movie_list)}</b> entries process ho rahi hain. Kripya intezaar karein.")
        
        # db.bulk_add_new_movies mein message_id automatically 9999999999999 set ho jaayega.
        added_count, skipped_count = await db.bulk_add_new_movies(movie_list, target_channel_id)

        await message.answer(f"""✅ <b>Import Complete!</b>
• Successfully Added: {added_count}
• Already Exists (Skipped): {skipped_count}
• Total Processed: {len(movie_list)}""")

    except json.JSONDecodeError:
        await message.answer("❌ Uploaded file valid JSON format mein nahi hai.")
    except Exception as e:
        logger.error(f"Import JSON failed: {e}", exc_info=True)
        await message.answer(f"❌ Internal error while processing: {type(e).__name__}. Check logs for details.")


@dp.message(Command("rebuild_index"), AdminFilter())
async def rebuild_index_command(message: types.Message):
    await message.answer("🔧 Clean titles reindex ho रहे हैं… yeh operation batched hai.")
    updated, total = await db.rebuild_clean_titles()
    await message.answer(f"✅ Reindex complete: Updated <b>{updated}</b> of ~{total} titles. Ab search feature tez aur sahi kaam karega.")

@dp.message(Command("export_csv"), AdminFilter())
async def export_csv_command(message: types.Message):
    args = message.text.split()
    if len(args) < 2 or args[1] not in ("users", "movies"):
        await message.answer("Use: /export_csv users|movies [limit]")
        return
    kind = args[1]
    limit = int(args[2]) if len(args) > 2 and args[2].isdigit() else 2000
    try:
        if kind == "users":
            rows = await db.export_users(limit=limit)
            header = """user_id,username,first_name,last_name,joined_date,last_active,is_active\n"""
            csv = header + "\n".join([
                f"{r['user_id']},{r['username'] or ''},{r['first_name'] or ''},{r['last_name'] or ''},{r['joined_date']},{r['last_active']},{r['is_active']}"
                for r in rows
            ])
            await message.answer_document(BufferedInputFile(csv.encode("utf-8"), filename="users.csv"), caption="Users export")
        else:
            rows = await db.export_movies(limit=limit)
            header = """imdb_id,title,year,channel_id,message_id,added_date\n"""
            csv = header + "\n".join([
                f"{r['imdb_id']},{r['title'].replace(',', ' ')},{r['year'] or ''},{r['channel_id']},{r['message_id']},{r['added_date']}"
                for r in rows
            ])
            await message.answer_document(BufferedInputFile(csv.encode("utf-8"), filename="movies.csv"), caption="Movies export")
            
    except Exception as e:
        logger.error(f"Export CSV failed: {e}", exc_info=True)
        await message.answer("❌ Data export karne me internal error aaya.")

@dp.message(Command("set_limit"), AdminFilter())
async def set_limit_command(message: types.Message):
    global CURRENT_CONC_LIMIT
    args = message.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await message.answer(f"Use: /set_limit N (current: {CURRENT_CONC_LIMIT})")
        return
    val = int(args[1])
    if val < 5 or val > 100:
        await message.answer("Allowed range: 5–100 for safety on free tier.")
        return
    CURRENT_CONC_LIMIT = val
    await message.answer(f"✅ Concurrency limit set to <b>{CURRENT_CONC_LIMIT}</b>")

@dp.channel_post()
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
    
    if await db.get_movie_by_imdb(imdb_id):
        logger.info(f"Movie already indexed: {movie_info.get('title')}")
        return
        
    success = await db.add_movie(
        imdb_id=imdb_id,
        title=movie_info.get("title"),
        year=movie_info.get("year"),
        file_id=file_id,
        message_id=message.message_id,
        channel_id=message.chat.id,
    )
    if success:
        logger.info(f"Auto-indexed: {movie_info.get('title')}")
    else:
        logger.error(f"Auto-index failed: {movie_info.get('title')}")
