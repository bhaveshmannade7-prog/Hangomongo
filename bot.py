# -*- coding: utf-8 -*-
import os
import asyncio
import logging
import re
from datetime import datetime
from contextlib import asynccontextmanager

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart, BaseFilter
from aiogram.types import Update, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError
from aiogram.client.default import DefaultBotProperties

from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks, Request

from database import Database, clean_text_for_search

# --- Configuration ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("bot")

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "7263519581"))
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", "-1003138949015"))
JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "MOVIEMAZASU")
USER_GROUP_USERNAME = os.getenv("USER_GROUP_USERNAME", "THEGREATMOVIESL9")
DATABASE_URL = os.getenv("DATABASE_URL")

# Render URL detection: prefer full external URL on Render
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")  # e.g. https://your-service.onrender.com
PUBLIC_URL = os.getenv("PUBLIC_URL")  # optional manual fallback (https://your-domain)
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")  # optional secret for Telegram header verification

CONCURRENT_LIMIT = 35
ACTIVE_WINDOW_MINUTES = 5
ALTERNATE_BOTS = ["Moviemaza91bot", "Moviemaza92bot", "Mazamovie9bot"]

if not BOT_TOKEN or not DATABASE_URL:
    logger.critical("Missing BOT_TOKEN or DATABASE_URL!")
    raise SystemExit(1)

# Build webhook URL robustly
def build_webhook_url() -> str:
    base = None
    if RENDER_EXTERNAL_URL:
        base = RENDER_EXTERNAL_URL.rstrip("/")
    elif PUBLIC_URL:
        base = PUBLIC_URL.rstrip("/")
    else:
        logger.warning("No external URL found; set RENDER_EXTERNAL_URL or PUBLIC_URL for webhook to work.")
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

def get_uptime():
    delta = datetime.utcnow() - start_time
    hours, remainder = divmod(int(delta.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    days, hours = divmod(hours, 24)
    if days > 0: return f"{days}d {hours}h {minutes}m"
    if hours > 0: return f"{hours}h {minutes}m"
    return f"{minutes}m {seconds}s"

async def check_user_membership(user_id: int) -> bool:
    # Membership bypass as requested
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
    lines = caption.strip().split("
")
    if lines:
        title = lines[0].strip()
        if len(lines) > 1 and re.search(r"Sd{1,2}", lines[1], re.IGNORECASE):
            title += " " + lines[1].strip()
        info["title"] = title
    imdb_match = re.search(r"(ttd{7,})", caption)
    if imdb_match:
        info["imdb_id"] = imdb_match.group(1)
    year_match = re.search(r"\b(19|20)d{2}\b", caption)
    if year_match:
        info["year"] = year_match.group(0)
    return info if "title" in info else None

# --- Keep DB alive ---
async def keep_db_alive():
    while True:
        try:
            await db.get_user_count()
        except Exception as e:
            logger.error(f"DB keepalive failed: {e}")
        await asyncio.sleep(240)

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
            logger.error(f"Webhook setup error: {e}")
    else:
        logger.warning("WEBHOOK_URL is empty; bot cannot receive updates until a public URL is set.")

    yield

    db_task.cancel()
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        logger.error(f"Webhook delete error: {e}")

app = FastAPI(lifespan=lifespan)

# Background processing wrapper to avoid blocking Telegram webhooks
async def _process_update(u: Update):
    try:
        await dp.feed_update(bot=bot, update=u)
    except Exception as e:
        logger.exception(f"feed_update failed: {e}")

@app.post(f"/bot/{BOT_TOKEN}")
async def bot_webhook(update: dict, background_tasks: BackgroundTasks, request: Request):
    if WEBHOOK_SECRET:
        if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
            return {"ok": False}
    telegram_update = Update(**update)
    background_tasks.add_task(_process_update, telegram_update)
    return {"ok": True}

@app.get("/")
async def ping():
    return {"status": "ok", "service": "Movie Bot is Live", "uptime": get_uptime()}

# --- Handlers ---
@dp.message(CommandStart())
async def start_command(message: types.Message):
    user_id = message.from_user.id
    bot_info = await bot.get_me()

    await db.add_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name)

    if user_id == ADMIN_USER_ID:
        user_count = await db.get_user_count()
        movie_count = await db.get_movie_count()
        concurrent_users = await db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES)
        admin_message = (
            f"👑 <b>Admin Console: @{bot_info.username}</b>
"
            f"<i>Access Level: Full Management</i>

"
            f"<u>System Performance & Metrics</u>
"
            f"📈 Active Users (5m): <code>{concurrent_users:,}/{CONCURRENT_LIMIT}</code>
"
            f"👥 Total Users: <code>{user_count:,}</code>
"
            f"🎬 Indexed Movies: <code>{movie_count:,}</code>
"
            f"⏰ Uptime: <code>{get_uptime()}</code>

"
            f"<u>Management Commands</u>
"
            f"• /stats — Real-time stats
"
            f"• /broadcast — Reply to a message to send
"
            f"• /cleanup_users — Deactivate inactive users
"
            f"• /add_movie — Reply to file: <code>/add_movie imdb_id | title | year</code>
"
        )
        await message.answer(admin_message)
        return

    welcome_text = (
        f"🎬 <b>Namaskar {message.from_user.first_name}!</b>
"
        f"Movie Search Bot me swagat hai.

"
        f"➡️ Kripya hamare <b>Channel</b> aur <b>Group</b> join karein, phir niche <b>I Have Joined Both</b> dabayen.
"
        f"Iske baad bas movie ya web series ka <b>naam</b> type karein (behtar results ke liye saal bhi likhein, jaise <i>Avatar 2009</i>)."
    )
    await message.answer(welcome_text, reply_markup=get_join_keyboard())

@dp.callback_query(F.data == "check_join")
async def check_join_callback(callback: types.CallbackQuery):
    await callback.answer("Verifying…")
    try:
        active_users = await db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES)
        success_text = (
            f"✅ <b>Verification successful, {callback.from_user.first_name}!</b>

"
            f"Ab aap library access kar sakte hain — apni pasand ki title ka naam bhejein.

"
            f"ℹ️ Free tier capacity: <b>{CONCURRENT_LIMIT}</b>, abhi active: <b>{active_users}</b>."
        )
        try:
            await callback.message.edit_text(success_text)
        except TelegramAPIError:
            await bot.send_message(callback.from_user.id, success_text)
    except Exception as e:
        logger.error(f"check_join error: {e}")
        await bot.send_message(callback.from_user.id, "⚠️ Technical error aya, kripya /start karein aur dobara koshish karein.")

@dp.message(F.text & ~F.text.startswith("/") & (F.chat.type == "private"))
async def search_movie_handler(message: types.Message):
    user_id = message.from_user.id
    await db.add_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name)

    if not await check_user_membership(user_id):
        return

    if user_id != ADMIN_USER_ID:
        concurrent_users = await db.get_concurrent_user_count(minutes=ACTIVE_WINDOW_MINUTES)
        if concurrent_users > CONCURRENT_LIMIT:
            limit_message = (
                f"⚠️ <b>Service capacity reached</b>

"
                f"Kripya niche diye gaye alternate bots ka upyog karein."
            )
            await message.answer(limit_message, reply_markup=get_full_limit_keyboard())
            return

    original_query = message.text.strip()
    if len(original_query) < 2:
        await message.answer("🤔 Kripya kam se kam 2 characters ka query bhejein.")
        return

    searching_msg = await message.answer(f"🔍 <b>{original_query}</b> ki khoj jaari hai…")

    try:
        processed_query = clean_text_for_search(original_query)
        best_results = await db.super_search_movies(processed_query, limit=20)
        if not best_results:
            await searching_msg.edit_text(f"🥲 Maaf kijiye, <b>{original_query}</b> ke liye match nahi mila. Doosra naam/spelling try karein.")
            return

        buttons = [[InlineKeyboardButton(text=movie["title"], callback_data=f"get_{movie['imdb_id']}")] for movie in best_results]
        await searching_msg.edit_text(
            f"🎬 <b>{original_query}</b> ke liye {len(best_results)} results mile — file paane ke liye chunein:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )
    except Exception as e:
        logger.error(f"Search error: {e}")
        await searching_msg.edit_text("❌ Internal error: search system me rukavat aa gayi hai. Kripya kuch der baad koshish karein.")

@dp.callback_query(F.data.startswith("get_"))
async def get_movie_callback(callback: types.CallbackQuery):
    await callback.answer("File forward ki ja rahi hai…")
    imdb_id = callback.data.split("_", 1)[1]
    movie = await db.get_movie_by_imdb(imdb_id)
    if not movie:
        await callback.message.edit_text("❌ Yeh movie ab database me uplabdh nahi hai.")
        return
    try:
        await callback.message.edit_text(f"✅ <b>{movie['title']}</b> — file bheji ja rahi hai, kripya chat check karein.")
        await bot.forward_message(
            chat_id=callback.from_user.id,
            from_chat_id=int(movie["channel_id"]),
            message_id=movie["message_id"],
        )
    except TelegramAPIError as e:
        logger.error(f"Forward/edit error for {imdb_id}: {e}")
        await bot.send_message(callback.from_user.id, f"❗️ Takneeki samasya: <b>{movie['title']}</b> ko forward karne me dikat aayi. Kripya phir se try karein.")
    except Exception as e:
        logger.error(f"Movie callback critical error: {e}")
        await bot.send_message(callback.from_user.id, "❌ Critical system error: kripya /start karein.")

# --- Admin Commands ---
@dp.message(Command("stats"), AdminFilter())
async def stats_command(message: types.Message):
    await db.add_user(message.from_user.id, message.from_user.username, message.from_user.first_name, message.from_user.last_name)
    user_count = await db.get_user_count()
    movie_count = await db.get_movie_count()
    concurrent_users = await db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES)
    await message.answer(
        "📊 <b>Live System Statistics</b>

"
        f"🟢 Active Users (5m): <code>{concurrent_users:,}/{CONCURRENT_LIMIT}</code>
"
        f"👥 Total Users: <code>{user_count:,}</code>
"
        f"🎬 Indexed Movies: <code>{movie_count:,}</code>
"
        f"⚙️ Status: Operational ✅
"
        f"⏰ Uptime: <code>{get_uptime()}</code>"
    )

@dp.message(Command("help"), AdminFilter())
async def admin_help(message: types.Message):
    await message.answer(
        "👑 <b>Admin Command Panel</b>
"
        "• /stats — Live statistics
"
        "• /broadcast — Reply to a message to send to all
"
        "• /cleanup_users — Deactivate users inactive for 30 days
"
        "• /add_movie — Reply to video/document and run: <code>/add_movie imdb_id | title | year</code>"
    )

@dp.message(Command("broadcast"), AdminFilter())
async def broadcast_command(message: types.Message):
    if not message.reply_to_message:
        await message.answer("❌ Broadcast ke liye kisi message ko reply karein.")
        return

    users = await db.get_all_users()
    total_users = len(users)
    success, failed = 0, 0
    progress_msg = await message.answer(f"📤 Broadcasting to {total_users} users…")

    for uid in users:
        try:
            await message.reply_to_message.copy_to(uid)
            success += 1
        except Exception:
            failed += 1
        if (success + failed) % 100 == 0 and (success + failed) > 0:
            await progress_msg.edit_text(f"📤 Broadcasting…
✅ Sent: {success} | ❌ Failed: {failed} | ⏳ Total: {total_users}")
        await asyncio.sleep(0.05)

    await progress_msg.edit_text(f"✅ <b>Broadcast Complete!</b>

• Success: {success}
• Failed: {failed}")

@dp.message(Command("cleanup_users"), AdminFilter())
async def cleanup_users_command(message: types.Message):
    await message.answer("🧹 Inactive users ko clean kiya ja raha hai…")
    removed_count = await db.cleanup_inactive_users(days=30)
    new_count = await db.get_user_count()
    await message.answer(f"✅ Cleanup complete!
• Deactivated: {removed_count}
• Active Users now: {new_count}")

@dp.message(Command("add_movie"), AdminFilter())
async def add_movie_command(message: types.Message):
    if not message.reply_to_message or not (message.reply_to_message.video or message.reply_to_message.document):
        await message.answer("❌ Kripya video/document par reply karke command bhejein: <code>/add_movie imdb_id | title | year</code>")
        return

    try:
        full_command = message.text.replace("/add_movie", "", 1).strip()
        parts = [p.strip() for p in full_command.split("|")]
        if len(parts) < 2:
            await message.answer("❌ Format galat hai. Use: <code>/add_movie imdb_id | title | year</code>")
            return
        imdb_id = parts[0]
        title = parts[1]
        year = parts[2] if len(parts) > 2 else None
    except Exception:
        await message.answer("❌ Format galat hai. Use: <code>/add_movie imdb_id | title | year</code>")
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
