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

from database import Database

# --- Step 1: Sabhi Variables aur IDs ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "7263519581"))
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", "-1003138949015"))
JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "MOVIEMAZASU")
USER_GROUP_USERNAME = os.getenv("USER_GROUP_USERNAME", "THEGREATMOVIESL9")
DATABASE_URL = os.getenv("DATABASE_URL")

# --- Step 2: Stable Architecture ke liye Webhook Setup ---
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

# --- NEW: Custom Admin Filter (Isse Admin Commands Theek Honge) ---
class AdminFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user.id == ADMIN_USER_ID

# --- FastAPI App (Bot ko 24/7 online rakhne ke liye) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    if RENDER_EXTERNAL_HOSTNAME:
        await bot.set_webhook(url=WEBHOOK_URL, allowed_updates=dp.resolve_used_update_types())
        logger.info(f"Webhook set to: {WEBHOOK_URL}")
    yield
    if RENDER_EXTERNAL_HOSTNAME:
        await bot.delete_webhook()

app = FastAPI(lifespan=lifespan)

@app.post(WEBHOOK_PATH)
async def bot_webhook(update: dict):
    telegram_update = Update(**update)
    await dp.feed_update(bot=bot, update=telegram_update)

# --- Step 3: Helper Functions + UPGRADED Search Processor ---

def get_uptime():
    delta = datetime.utcnow() - start_time
    # ... (Uptime logic)
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
        [InlineKeyboardButton(text="📢 Join Channel", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}"),
         InlineKeyboardButton(text="👥 Join Group", url=f"https://t.me/{USER_GROUP_USERNAME}")],
        [InlineKeyboardButton(text="✅ I Joined Both", callback_data="check_join")]
    ])

def clean_text_for_search(text: str) -> str:
    # Yeh function title aur search query, dono ko saaf karta hai
    text = text.lower()
    # "season 1", "s1", "complete season" jaise shabdon ko aasan banata hai
    text = re.sub(r'\b(s|season|seson|sisan)\s*(\d{1,2})\b', r's\2', text)
    text = re.sub(r'complete season', '', text)
    # Special characters hatata hai
    text = re.sub(r'[\W_]+', ' ', text)
    return text.strip()

def extract_movie_info(caption: str):
    if not caption: return None
    info = {}
    imdb_match = re.search(r'(tt\d{7,})', caption)
    if imdb_match: info['imdb_id'] = imdb_match.group(1)
    
    lines = caption.strip().split('\n')
    if lines:
        title = lines[0].strip()
        if len(lines) > 1 and re.search(r'S\d{1,2}', lines[1], re.IGNORECASE):
             title += " " + lines[1].strip()
        info['title'] = title
        
    year_match = re.search(r'\b(19|20)\d{2}\b', caption)
    if year_match: info['year'] = year_match.group(0)
    
    return info if 'imdb_id' in info or 'title' in info else None

# --- Step 4: Sabhi Features (Handlers) ---

@dp.message(CommandStart())
async def start_command(message: types.Message):
    user_id = message.from_user.id
    first_name = message.from_user.first_name
    await db.add_user(user_id, message.from_user.username, first_name, message.from_user.last_name)
    
    if user_id == ADMIN_USER_ID:
        # NEW Professional Admin Welcome Message
        user_count = await db.get_user_count()
        movie_count = await db.get_movie_count()
        admin_message = (f"👑 <b>Welcome Boss!</b>\n\n"
                         f"Aapka Movie Search Bot poori tarah se operational hai.\n\n"
                         f"<u><b>System Overview:</b></u>\n"
                         f"- <b>Status:</b> <pre>✅ Online & Stable</pre>\n"
                         f"- <b>Database:</b> <pre>✅ Connected</pre>\n"
                         f"- <b>Auto-Index:</b> <pre>✅ Active</pre>\n\n"
                         f"<u><b>Live Statistics:</b></u>\n"
                         f"- <b>Total Users:</b> <pre>{user_count:,}</pre>\n"
                         f"- <b>Total Movies:</b> <pre>{movie_count:,}</pre>\n\n"
                         f"<i>Admin commands ke liye /help type karein.</i>")
        await message.answer(admin_message)
    else:
        # NEW User-Friendly Welcome Message
        if not await check_user_membership(user_id):
            await message.answer(f"👋 <b>नमस्ते {first_name}!</b>\n\nFilmy duniya mein aapka swagat hai! Movies search karne ke liye, bas neeche diye gaye channel aur group ko join karein.", reply_markup=get_join_keyboard())
        else:
            await message.answer(f"🎬 <b>Aapka Swagat Hai, {first_name}!</b>\n\nAb aap taiyar hain! Kisi bhi movie ya web series ka naam likhein aur jaadu dekhein.")

@dp.callback_query(F.data == "check_join")
async def check_join_callback(callback: types.CallbackQuery):
    if await check_user_membership(callback.from_user.id):
        await callback.message.edit_text(f"✅ <b>Dhanyavaad, {callback.from_user.first_name}!</b>\n\nAapne successfully join kar liya hai. Ab search shuru karein!")
    else:
        await callback.answer("❌ Oops! Aisa lagta hai aapne dono join nahi kiye hain. Kripya dobara try karein.", show_alert=True)

@dp.message(F.text & ~F.text.startswith('/'))
async def search_movie_handler(message: types.Message):
    user_id = message.from_user.id
    if user_id != ADMIN_USER_ID and not await check_user_membership(user_id):
        await message.answer("❌ Search karne se pehle, kripya Channel aur Group join karein.", reply_markup=get_join_keyboard())
        return

    original_query = message.text.strip()
    if len(original_query) < 2:
        await message.answer("🤔 Thoda aur likhein... Search ke liye kam se kam 2 akshar zaroori hain.")
        return

    searching_msg = await message.answer(f"🔍 <b>'{original_query}'</b>... dhoondh rahe hain...")
    
    # UPGRADED SEARCH LOGIC
    processed_query = clean_text_for_search(original_query)
    best_results = await db.super_search_movies(processed_query, limit=20)

    if not best_results:
        await searching_msg.edit_text(f"🥲 Maaf kijiye, <b>'{original_query}'</b> ke liye koi result nahi mila. Ek baar spelling check karke dekhein?")
        return

    buttons = [[InlineKeyboardButton(text=movie['title'], callback_data=f"get_{movie['imdb_id']}")] for movie in best_results]
    await searching_msg.edit_text(f"🎬 <b>'{original_query}'</b> ke liye yeh results mile hain:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(F.data.startswith("get_"))
async def get_movie_callback(callback: types.CallbackQuery):
    await callback.answer("File forward kar rahe hain...")
    imdb_id = callback.data.split('_', 1)[1]
    movie = await db.get_movie_by_imdb(imdb_id)

    if not movie:
        await callback.message.edit_text("❌ Yeh movie ab database mein uplabdh nahi hai.")
        return

    await callback.message.edit_text(f"✅ Aapne chuna: <b>{movie['title']}</b>\n\nFile bheji jaa rahi hai...")
    
    try:
        await bot.forward_message(chat_id=callback.from_user.id, from_chat_id=movie['channel_id'], message_id=movie['message_id'])
    except Exception as e:
        logger.error(f"Movie forward karne mein error: {e}")
        await callback.message.answer(f"❗️ Movie <b>{movie['title']}</b> ko forward karne mein koi takneeki samasya aa gayi hai.")

@dp.channel_post()
async def auto_index_handler(message: types.Message):
    if message.chat.id != LIBRARY_CHANNEL_ID or not (message.video or message.document): return
    caption = message.caption or ""
    movie_info = extract_movie_info(caption)
    if not movie_info: return
    
    file_id = message.video.file_id if message.video else message.document.file_id
    imdb_id = movie_info.get('imdb_id', f'auto_{message.message_id}')
    
    if await db.get_movie_by_imdb(imdb_id):
        logger.info(f"Movie {movie_info.get('title')} pehle se hai. Skipping.")
        return
        
    success = await db.add_movie(
        imdb_id=imdb_id, title=movie_info.get('title', 'Unknown'), year=movie_info.get('year'),
        file_id=file_id, channel_id=LIBRARY_CHANNEL_ID, message_id=message.message_id
    )
    if success: logger.info(f"✅ Auto-indexed: {movie_info.get('title')}")

# --- Admin Features (Ab Theek se Chalenge) ---
@dp.message(Command("help"), AdminFilter())
async def admin_help(message: types.Message):
    await message.answer("""👑 <b>Admin Command Panel</b> 👑
/stats - Bot ke live statistics dekhein.
/broadcast - Sabhi users ko message bhejein.
/cleanup_users - 30 din se inactive users ko deactivate karein.
/add_movie - Kisi movie ko manual roop se add karein.""")

@dp.message(Command("stats"), AdminFilter())
async def stats_command(message: types.Message):
    user_count = await db.get_user_count()
    movie_count = await db.get_movie_count()
    await message.answer(f"📊 <b>System Health & Stats</b>\n\n"
                         f"👥 <b>Total Users:</b> {user_count:,}\n"
                         f"🎬 <b>Total Movies:</b> {movie_count:,}\n"
                         f"⚙️ <b>Status:</b> Operational ✅\n"
                         f"⏰ <b>Instance Uptime:</b> {get_uptime()}")

@dp.message(Command("broadcast"), AdminFilter())
async def broadcast_command(message: types.Message):
    # ... (Broadcast logic) ...
    if not message.reply_to_message:
        await message.answer("❌ Broadcast karne ke liye kisi message ko reply karein.")
        return
    
    users = await db.get_all_users()
    # ... (baaki broadcast logic)

@dp.message(Command("cleanup_users"), AdminFilter())
async def cleanup_users_command(message: types.Message):
    await message.answer("🧹 Inactive users ko clean kar rahe hain...")
    removed_count = await db.cleanup_inactive_users(days=30)
    new_count = await db.get_user_count()
    await message.answer(f"✅ Cleanup complete!\n- Deactivated: {removed_count} users\n- Active Users: {new_count}")

@dp.message(Command("add_movie"), AdminFilter())
async def add_movie_command(message: types.Message):
    # ... (add_movie logic) ...
    if not message.reply_to_message or not (message.reply_to_message.video or message.reply_to_message.document):
        await message.answer("❌ Movie file ko reply karke command likhein: `/add_movie imdb_id | title | year`")
        return
    # ... (baaki add_movie logic)
