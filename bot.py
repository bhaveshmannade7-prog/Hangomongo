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
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv
from fastapi import FastAPI

from database import Database, clean_text_for_search

# --- Step 1: Configuration and Environment Variables ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "7263519581"))
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", "-1003138949015"))
JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "MOVIEMAZASU")
USER_GROUP_USERNAME = os.getenv("USER_GROUP_USERNAME", "THEGREATMOVIESL9")
DATABASE_URL = os.getenv("DATABASE_URL")

# --- User Limit and Alternate Bots ---
CONCURRENT_LIMIT = 35 
ACTIVE_WINDOW_MINUTES = 5
ALTERNATE_BOTS = ["Moviemaza92bot", "Moviemaza91bot"]

RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME") 
WEBHOOK_PATH = f"/bot/{BOT_TOKEN}"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

if not DATABASE_URL or not BOT_TOKEN:
    logger.critical("Zaroori Environment Variables (BOT_TOKEN, DATABASE_URL) nahi mile!")
    exit()

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
db = Database(DATABASE_URL)
start_time = datetime.utcnow()

# --- Custom Admin Filter ---
class AdminFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user.id == ADMIN_USER_ID

# --- FastAPI App & DB Keep Alive ---
async def keep_db_alive():
    while True:
        try:
            # DB connection ko zinda rakhne ke liye ek simple query
            await db.get_user_count() 
        except Exception as e:
            logger.error(f"DB warming failed: {e}")
        # Har 4 minute mein DB ko ping karein
        await asyncio.sleep(4 * 60) 

@asynccontextmanager
async def lifespan(app: FastAPI):
    # App start hone par DB init karein
    await db.init_db()
    db_task = asyncio.create_task(keep_db_alive())
    
    # Webhook set karein agar hostname available hai
    if RENDER_EXTERNAL_HOSTNAME:
        try:
            current_webhook = await bot.get_webhook_info()
            if current_webhook.url != WEBHOOK_URL:
                await bot.set_webhook(url=WEBHOOK_URL, allowed_updates=dp.resolve_used_update_types())
                logger.info(f"Webhook set to: {WEBHOOK_URL}")
        except Exception as e:
            logger.error(f"Error setting webhook: {e}")
            
    yield
    
    # App band hone par cleanup karein
    db_task.cancel() 
    if RENDER_EXTERNAL_HOSTNAME:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception as e:
            logger.error(f"Error deleting webhook: {e}")

app = FastAPI(lifespan=lifespan)

@app.post(WEBHOOK_PATH)
async def bot_webhook(update: dict):
    # Incoming Telegram update ko Dispatcher mein feed karein
    telegram_update = Update(**update)
    await dp.feed_update(bot=bot, update=telegram_update)
    return {"ok": True}

@app.get("/")
async def ping():
    return {"status": "ok", "service": "Movie Bot is Live"}

# --- Helper Functions ---
def get_uptime():
    delta = datetime.utcnow() - start_time
    hours, remainder = divmod(int(delta.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    days, hours = divmod(hours, 24)
    if days > 0: return f"{days}d {hours}h {minutes}m"
    if hours > 0: return f"{hours}h {minutes}m"
    return f"{minutes}m {seconds}s"

async def check_user_membership(user_id: int) -> bool:
    try:
        channel_member = await bot.get_chat_member(f"@{JOIN_CHANNEL_USERNAME}", user_id)
        if channel_member.status not in ['member', 'administrator', 'creator']: return False
        group_member = await bot.get_chat_member(f"@{USER_GROUP_USERNAME}", user_id)
        if group_member.status not in ['member', 'administrator', 'creator']: return False
        return True
    except: return False

def get_join_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Channel Join Karein", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}"),
         InlineKeyboardButton(text="👥 Group Join Karein", url=f"https://t.me/{USER_GROUP_USERNAME}")],
        [InlineKeyboardButton(text="✅ Maine Dono Join Kar Liye", callback_data="check_join")]
    ])

def get_full_limit_keyboard():
    # Alternet bots ke liye buttons
    buttons = [[InlineKeyboardButton(text=f"🚀 @{bot_user}", url=f"https://t.me/{bot_user}")] for bot_user in ALTERNATE_BOTS]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def extract_movie_info(caption: str):
    # Caption se movie/series ki jaankari nikalne ka logic
    if not caption: return None
    info = {}
    lines = caption.strip().split('\n')
    if lines:
        # Title extract karein
        title = lines[0].strip()
        if len(lines) > 1 and re.search(r'S\d{1,2}', lines[1], re.IGNORECASE):
             title += " " + lines[1].strip()
        info['title'] = title
    # IMDB ID extract karein
    imdb_match = re.search(r'(tt\d{7,})', caption)
    if imdb_match: info['imdb_id'] = imdb_match.group(1)
    # Year extract karein
    year_match = re.search(r'\b(19|20)\d{2}\b', caption)
    if year_match: info['year'] = year_match.group(0)
    return info if 'title' in info else None

# --- Step 4: Bot Features (Handlers) ---

@dp.message(CommandStart())
async def start_command(message: types.Message):
    user_id = message.from_user.id
    first_name = message.from_user.first_name
    
    # User activity log karein (Zaroori: last_active update karne ke liye)
    await db.add_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name)

    if user_id == ADMIN_USER_ID:
        # --- Admin Path ---
        user_count = await db.get_user_count()
        movie_count = await db.get_movie_count() 
        concurrent_users = await db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES)
        
        admin_message = (
            f"👑 **Admin Dashboard: @{await bot.get_me().username}**\n\n"
            f"<b><u>System Status & Stats:</u></b>\n"
            f"- **Active Users (5m):** <pre>{concurrent_users:,}/{CONCURRENT_LIMIT}</pre>\n"
            f"- **Total Users:** <pre>{user_count:,}</pre>\n"
            f"- **Total Movies:** <pre>{movie_count:,}</pre>\n"
            f"- **Uptime:** <pre>{get_uptime()}</pre>\n\n"
            f"<b><u>Admin Commands:</u></b>\n"
            f"🔹 /stats - Live statistics dekhein.\n"
            f"🔹 /broadcast - Sabhi users ko message bhejein (message ko reply karke).\n"
            f"🔹 /cleanup_users - 30 din se inactive users ko deactivate karein.\n"
            f"🔹 /add_movie - Movie file ko reply karke manual index karein."
        )
        await message.answer(admin_message)
        return

    # --- Regular User Path ---
    if not await check_user_membership(user_id):
        welcome_text = (
            f"👋 **Namaskar {first_name}!** Film Khoj Bot Mein Aapka Swagat Hai.\n\n"
            f"➡️ Is bot ki Free Service ka laabh uthane ke liye, kripya neeche diye gaye zaroori **Channel aur Group dono** ko turant join karein. Iske baad, **'Maine Dono Join Kar Liye'** button par zaroor click karein."
        )
        await message.answer(welcome_text, reply_markup=get_join_keyboard())
    else:
        active_users = await db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES)
        welcome_text = (
            f"🎉 **Badhai Ho, {first_name}!** Aap Safaltapoorvak Jud Chuke Hain.\n\n"
            f"🚀 Ab aap seedhe **movie ya web series ka naam likhkar** file search shuru kar sakte hain.\n\n"
            f"<i>💡 **Seva Suchna (Service Notice):** Humari Free Tier mein {CONCURRENT_LIMIT} users ki seema hai. Abhi **{active_users}** users active hain.</i>"
        )
        await message.answer(welcome_text)


@dp.callback_query(F.data == "check_join")
async def check_join_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    first_name = callback.from_user.first_name
    
    if await check_user_membership(user_id):
        active_users = await db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES)
        welcome_text = (
            f"✅ **Shukriya, {first_name}!** Aapne Safaltapoorvak Dono Join Kar Liye Hain.\n\n"
            f"Ab aap movie ya web series ka naam likhkar search shuru kar sakte hain.\n\n"
            f"<i>💡 **Seema Suchna (Limit Info):** Bot par {CONCURRENT_LIMIT} users ki seema hai. Abhi **{active_users}** users active hain.</i>"
        )
        # Message ko update karke keyboard hatayein
        await callback.message.edit_text(welcome_text)
        await callback.answer("Membership check successful!")
    else:
        await callback.answer("❌ Maaf kijiye! Aisa lagta hai aapne dono Channel aur Group join nahi kiye hain. Kripya pehle dono join karein.", show_alert=True)


@dp.message(F.text & ~F.text.startswith('/') & F.chat.type == 'private')
async def search_movie_handler(message: types.Message):
    user_id = message.from_user.id
    
    # 1. User activity log karein
    await db.add_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name)

    # 2. Membership Check
    if user_id != ADMIN_USER_ID and not await check_user_membership(user_id):
        await message.answer("❌ **Pehle Zaroori Channels Join Karein** ❌\n\nKripya movie search karne se pehle zaroori Channel aur Group join karein aur **'Maine Dono Join Kar Liye'** button dabayein.", reply_markup=get_join_keyboard())
        return

    # 3. Concurrency Limit Check (Zaroori: Sirf search requests ko block karein)
    if user_id != ADMIN_USER_ID:
        concurrent_users = await db.get_concurrent_user_count(minutes=ACTIVE_WINDOW_MINUTES)
        
        if concurrent_users > CONCURRENT_LIMIT:
            await message.answer(
                f"⚠️ **Seva (Service) Abhi Vyast Hai**\n\n"
                f"Humari Free Tier stability policy ke anusaar, bot par **{CONCURRENT_LIMIT} users** ki seema (limit) hai. Abhi yeh seema poori ho chuki hai, isliye aapki search request roka jaa raha hai.\n\n"
                f"Aap turant movie search ke liye humare **doosre bots** ka upyog kar sakte hain:"
                , reply_markup=get_full_limit_keyboard()
            )
            return
            
    # 4. Search Logic
    original_query = message.text.strip()
    if len(original_query) < 2:
        await message.answer("🤔 Search ke liye kripya kam se kam 2 akshar likhein.")
        return

    searching_msg = await message.answer(f"🔍 **'{original_query}'** ki khoj jaari hai...")
    
    processed_query = clean_text_for_search(original_query)
    best_results = await db.super_search_movies(processed_query, limit=20) 

    if not best_results:
        await searching_msg.edit_text(f"🥲 Maaf kijiye, **'{original_query}'** ke liye koi bhi perfect match nahi mila. Kripya doosra naam try karein.")
        return

    buttons = [[InlineKeyboardButton(text=movie['title'], callback_data=f"get_{movie['imdb_id']}")] for movie in best_results]
    await searching_msg.edit_text(f"🎬 **'{original_query}'** ke liye behtareen results. File paane ke liye chunein:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(F.data.startswith("get_"))
async def get_movie_callback(callback: types.CallbackQuery):
    # Callback queries mein activity log ki zaroorat nahi, sirf action execute karein.
    await callback.answer("File forward ki jaa rahi hai...")
    imdb_id = callback.data.split('_', 1)[1]
    
    movie = await db.get_movie_by_imdb(imdb_id) 

    if not movie:
        await callback.message.edit_text("❌ Yeh movie ab database mein uplabdh nahi hai.")
        return

    # User ko file forward ki jaa rahi hai
    await callback.message.edit_text(f"✅ **{movie['title']}** - File bheji jaa rahi hai. Kripya apne chats check karein.")
    
    try:
        await bot.forward_message(chat_id=callback.from_user.id, from_chat_id=int(movie['channel_id']), message_id=movie['message_id']) 
    except Exception as e:
        logger.error(f"Movie forward karne mein error: {e}")
        await callback.message.answer(f"❗️ **Takneeki Samasya:** Movie **{movie['title']}** ko forward karne mein koi rukavat aa gayi hai. Kripya phir se prayas karein.")

# --- Admin Commands ---
# Admin commands mein bhi db.add_user call kiya gaya hai, taaki admin ki activity bhi track ho.

@dp.message(Command("stats"), AdminFilter())
async def stats_command(message: types.Message):
    await db.add_user(message.from_user.id, message.from_user.username, message.from_user.first_name, message.from_user.last_name)
    user_count = await db.get_user_count()
    movie_count = await db.get_movie_count()
    concurrent_users = await db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES)
    await message.answer(f"📊 **Live System Statistics**\n\n"
                         f"🟢 **Active Users (5 Min):** {concurrent_users:,}/{CONCURRENT_LIMIT}\n"
                         f"👥 **Total Registered Users:** {user_count:,}\n"
                         f"🎬 **Total Indexed Movies:** {movie_count:,}\n"
                         f"⚙️ **Status:** Operational ✅\n"
                         f"⏰ **Instance Uptime:** {get_uptime()}")

@dp.message(Command("help"), AdminFilter())
async def admin_help(message: types.Message):
    await message.answer("""👑 <b>Admin Command Panel</b> 👑
🔹 /stats - Live statistics dekhein.
🔹 /broadcast - Sabhi users ko message bhejein (message ko reply karke).
🔹 /cleanup_users - 30 din se inactive users ko deactivate karein.
🔹 /add_movie - Movie file ko reply karke manual index karein.""")

@dp.message(Command("broadcast"), AdminFilter())
async def broadcast_command(message: types.Message):
    if not message.reply_to_message:
        await message.answer("❌ Broadcast karne ke liye kisi message ko reply karein.")
        return
    
    users = await db.get_all_users()
    total_users = len(users)
    success, failed = 0, 0
    progress_msg = await message.answer(f"📤 Broadcasting to {total_users} users...")
    
    for user_id in users:
        try:
            await message.reply_to_message.copy_to(user_id)
            success += 1
        except Exception: 
            failed += 1
        if (success + failed) % 100 == 0 and (success + failed) > 0:
            await progress_msg.edit_text(f"📤 Broadcasting...\n✅ Sent: {success} | ❌ Failed: {failed} | ⏳ Total: {total_users}")
        await asyncio.sleep(0.05)
    
    await progress_msg.edit_text(f"✅ <b>Broadcast Complete!</b>\n\n- Success: {success}\n- Failed: {failed}")

@dp.message(Command("cleanup_users"), AdminFilter())
async def cleanup_users_command(message: types.Message):
    await message.answer("🧹 Inactive users ko clean kar rahe hain...")
    removed_count = await db.cleanup_inactive_users(days=30)
    new_count = await db.get_user_count()
    await message.answer(f"✅ Cleanup complete!\n- Deactivated: {removed_count} users\n- Active Users now: {new_count}")

@dp.message(Command("add_movie"), AdminFilter())
async def add_movie_command(message: types.Message):
    if not message.reply_to_message or not (message.reply_to_message.video or message.reply_to_message.document):
        await message.answer("❌ Movie file ko reply karke command likhein: `/add_movie imdb_id | title | year`")
        return
    
    try:
        # Command se arguments parse karein
        full_command = message.text.replace('/add_movie', '', 1).strip()
        parts = [p.strip() for p in full_command.split('|')]
        
        if len(parts) < 2:
            await message.answer("❌ Format galat hai. Use: `/add_movie imdb_id | title | year`")
            return
            
        imdb_id = parts[0]
        title = parts[1]
        year = parts[2] if len(parts) > 2 else None
        
    except Exception:
        await message.answer("❌ Format galat hai. Use: `/add_movie imdb_id | title | year`")
        return

    if await db.get_movie_by_imdb(imdb_id):
        await message.answer("⚠️ Is IMDB ID se movie pehle se hai!")
        return
        
    file_id = message.reply_to_message.video.file_id if message.reply_to_message.video else message.reply_to_message.document.file_id
    success = await db.add_movie(
        imdb_id=imdb_id, title=title, year=year,
        file_id=file_id, channel_id=message.reply_to_message.chat.id, message_id=message.reply_to_message.message_id
    )
    if success: await message.answer(f"✅ Movie '{title}' add ho gayi hai.")
    else: await message.answer("❌ Movie add karne mein error aaya.")

@dp.channel_post()
async def auto_index_handler(message: types.Message):
    # Channel post sirf LIBRARY_CHANNEL_ID se aana chahiye
    if message.chat.id != LIBRARY_CHANNEL_ID or not (message.video or message.document): return
    caption = message.caption or ""
    movie_info = extract_movie_info(caption)
    if not movie_info: 
        logger.warning(f"Auto-index failed: Could not extract info from caption: {caption[:50]}")
        return
    
    file_id = message.video.file_id if message.video else message.document.file_id
    imdb_id = movie_info.get('imdb_id', f'auto_{message.message_id}')
    
    if await db.get_movie_by_imdb(imdb_id):
        logger.info(f"Movie {movie_info.get('title')} pehle se hai. Skipping.")
        return
        
    success = await db.add_movie(
        imdb_id=imdb_id, title=movie_info.get('title'), year=movie_info.get('year'),
        file_id=file_id, channel_id=LIBRARY_CHANNEL_ID, message_id=message.message_id
    )
    if success: logger.info(f"✅ Auto-indexed: {movie_info.get('title')}")
    else: logger.error(f"Auto-index database error for: {movie_info.get('title')}")
