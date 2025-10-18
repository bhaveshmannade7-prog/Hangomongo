# -*- coding: utf-8 -*-
import os
import asyncio
import logging
import re
from datetime import datetime
from contextlib import asynccontextmanager

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Update, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
# YEH NAYI LINE HAI ERROR THEEK KARNE KE LIYE
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from dotenv import load_dotenv
from fastapi import FastAPI

from database import Database

# --- Step 1: Aapke Original Code se Sabhi Variables aur IDs ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "7263519581"))
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", "-1003138949015"))
JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "@MOVIEMAZASU")
USER_GROUP_USERNAME = os.getenv("USER_GROUP_USERNAME", "@THEGREATMOVIESL9")
DATABASE_URL = os.getenv("DATABASE_URL")

# --- Step 2: Naye, Stable Architecture ke liye Webhook Setup ---
DETA_PROJECT_URL = os.getenv("RENDER_EXTERNAL_HOSTNAME") # Render ke liye
WEBHOOK_PATH = f"/bot/{BOT_TOKEN}"
WEBHOOK_URL = f"https://{DETA_PROJECT_URL}{WEBHOOK_PATH}"

# YEH LINE BADLI GAYI HAI ERROR THEEK KARNE KE LIYE
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
db = Database(DATABASE_URL)
start_time = datetime.utcnow()

# --- FastAPI App (Bot ko Web Service banane ke liye) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    await bot.set_webhook(url=WEBHOOK_URL, allowed_updates=dp.resolve_used_update_types())
    logger.info(f"Webhook set to: {WEBHOOK_URL}")
    yield
    await bot.delete_webhook()
    logger.info("Webhook deleted.")

app = FastAPI(lifespan=lifespan)

@app.post(WEBHOOK_PATH)
async def bot_webhook(update: dict):
    telegram_update = Update(**update)
    await dp.feed_update(bot=bot, update=telegram_update)

# --- Step 3: Aapke Original Code se Sabhi Helper Functions + Naya Search Improver ---

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
        channel_member = await bot.get_chat_member(JOIN_CHANNEL_USERNAME, user_id)
        if channel_member.status not in ['member', 'administrator', 'creator']: return False
        group_member = await bot.get_chat_member(USER_GROUP_USERNAME, user_id)
        if group_member.status not in ['member', 'administrator', 'creator']: return False
        return True
    except: return False

def get_join_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Join Channel", url=f"https://t.me/{JOIN_CHANNEL_USERNAME.replace('@', '')}"),
         InlineKeyboardButton(text="👥 Join Group", url=f"https://t.me/{USER_GROUP_USERNAME.replace('@', '')}")],
        [InlineKeyboardButton(text="✅ I Joined Both", callback_data="check_join")]
    ])

def extract_movie_info(caption: str):
    if not caption: return None
    info = {}
    imdb_match = re.search(r'(tt\d{7,})', caption)
    if imdb_match: info['imdb_id'] = imdb_match.group(1)
    
    lines = caption.strip().split('\n')
    if lines:
        title = lines[0].strip()
        if len(lines) > 1 and re.search(r'S\d{1,2}E\d{1,2}', lines[1], re.IGNORECASE):
             title += " " + lines[1].strip()
        info['title'] = re.sub(r'^\s*🌸\s*|\s*🌸\s*🍀\s*$', '', title).strip()
        
    year_match = re.search(r'\b(19|20)\d{2}\b', caption)
    if year_match: info['year'] = year_match.group(0)
    
    genre_match = re.search(r'Genre:\s*([^\n]+)', caption, re.IGNORECASE)
    if genre_match: info['genre'] = genre_match.group(1).strip()
    
    rating_match = re.search(r'Rating:\s*(\d+\.?\d*)|(\d+\.?\d*)\s*/\s*10', caption, re.IGNORECASE)
    if rating_match: info['rating'] = next(g for g in rating_match.groups() if g is not None)
    
    return info if 'imdb_id' in info or 'title' in info else None

def preprocess_search_query(query: str) -> str:
    query = query.lower()
    query = re.sub(r'\b(s|season|seson|sisan)\s*(\d{1,2})\b', r's\2', query)
    query = re.sub(r'\b(e|episode|ep)\s*(\d{1,2})\b', r'e\2', query)
    return query

# --- Step 4: Aapke Bot ke Sabhi Features (Handlers) ---

@dp.message(CommandStart())
async def start_command(message: types.Message):
    user_id = message.from_user.id
    first_name = message.from_user.first_name
    await db.add_user(user_id, message.from_user.username, first_name, message.from_user.last_name)
    
    if user_id == ADMIN_USER_ID:
        user_count = await db.get_user_count()
        movie_count = await db.get_movie_count()
        admin_message = (f"👑 <b>Welcome Boss!</b>\n\n"
                         f"<b>🤖 Bot Status:</b> Fully Operational ✅\n"
                         f"<b>💾 Database:</b> Connected & Stable ✅\n"
                         f"<b>⚡ Architecture:</b> Serverless (Render)\n"
                         f"<b>⏰ Uptime:</b> {get_uptime()}\n\n"
                         f"<b>📊 Statistics:</b>\n"
                         f"• Total Users: <b>{user_count:,}</b>\n"
                         f"• Total Movies: <b>{movie_count:,}</b>\n\n"
                         f"<i>Auto-index is enabled! /help for admin commands.</i>")
        await message.answer(admin_message)
    else:
        if not await check_user_membership(user_id):
            welcome_message = f"""👋 <b>नमस्ते {first_name}!</b>

मूवीज़ सर्च करने के लिए, कृपया पहले हमारे चैनल और ग्रुप को ज्वाइन करें:

📢 <b>Channel:</b> यहां आपको नई मूवीज़ की updates मिलेंगी
👥 <b>Group:</b> यहां आप दूसरे मेंबर्स से बात कर सकते हैं

<i>दोनों join करने के बाद "✅ I Joined Both" बटन दबाएं</i>"""
            await message.answer(welcome_message, reply_markup=get_join_keyboard())
        else:
            movie_count = await db.get_movie_count()
            welcome_message = f"""🎬 <b>स्वागत है {first_name}!</b>

मैं आपका मूवी सर्च असिस्टेंट हूं। बस मूवी का नाम टाइप करें!

✨ <b>Features:</b>
• तेज़ और सटीक सर्च
• High-quality मूवी results
• {movie_count:,}+ मूवीज़ का कलेक्शन

💡 <b>कैसे यूज़ करें:</b>
किसी भी मूवी का नाम टाइप करें और सर्च करें!

<b>Example:</b> <code>Inception</code> या <code>3 Idiots</code>"""
            await message.answer(welcome_message)

@dp.callback_query(F.data == "check_join")
async def check_join_callback(callback: types.CallbackQuery):
    if await check_user_membership(callback.from_user.id):
        welcome_message = f"""✅ <b>बधाई हो {callback.from_user.first_name}!</b>

आपने successfully channel और group join कर लिया है! 🎉

अब आप किसी भी मूवी का नाम टाइप करके सर्च कर सकते हैं।

<b>🎬 Example:</b> <code>The Dark Knight</code> या <code>Dangal</code>

<i>Happy watching! 🍿</i>"""
        await callback.message.edit_text(welcome_message)
    else:
        await callback.answer("❌ कृपया पहले channel और group दोनों join करें!", show_alert=True)

@dp.message(F.text & ~F.text.startswith('/'))
async def search_movie_handler(message: types.Message):
    user_id = message.from_user.id
    if user_id != ADMIN_USER_ID and not await check_user_membership(user_id):
        await message.answer("❌ मूवी सर्च करने से पहले Channel और Group join करें!", reply_markup=get_join_keyboard())
        return

    original_query = message.text.strip()
    if len(original_query) < 2:
        await message.answer("❌ Search करने के लिए कम से कम 2 अक्षर लिखें.")
        return

    searching_msg = await message.answer(f"🔍 <b>'{original_query}'</b>... खोज रहे हैं...")
    
    processed_query = preprocess_search_query(original_query)
    best_results = await db.search_movies_fuzzy(processed_query, limit=20)

    if not best_results:
        await searching_msg.edit_text(f"🥲 <b>'{original_query}'</b> के लिए कोई रिजल्ट नहीं मिला. कृपया स्पेलिंग चेक करें.")
        return

    buttons = [[InlineKeyboardButton(text=movie['title'], callback_data=f"get_{movie['imdb_id']}")] for movie in best_results]
    await searching_msg.edit_text(f"🎬 <b>'{original_query}'</b> के लिए यह 20 सबसे करीबी रिजल्ट्स हैं:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(F.data.startswith("get_"))
async def get_movie_callback(callback: types.CallbackQuery):
    await callback.answer("File भेज रहे हैं...")
    imdb_id = callback.data.split('_', 1)[1]
    movie = await db.get_movie_by_imdb(imdb_id)

    if not movie:
        await callback.message.edit_text("❌ यह मूवी अब उपलब्ध नहीं है.")
        return

    await callback.message.edit_text(f"✅ आपने चुना: <b>{movie['title']}</b>\n\nअब फाइल भेजी जा रही है...")
    
    if all(k in movie for k in ['file_id', 'channel_id', 'message_id']):
        try:
            await bot.copy_message(chat_id=callback.from_user.id, from_chat_id=movie['channel_id'], message_id=movie['message_id'])
        except Exception as e:
            logger.error(f"Movie bhejne mein error: {e}")
            await callback.message.answer(f"❗️ मूवी <b>{movie['title']}</b> भेजने में कोई समस्या आ गयी है.")
    else:
        await callback.message.answer(f"❗️ मूवी <b>{movie['title']}</b> मिली, लेकिन इसकी फाइल अभी लिंक नहीं है.")

# --- Auto Indexing ---
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
        genre=movie_info.get('genre'), rating=movie_info.get('rating'), file_id=file_id, 
        channel_id=LIBRARY_CHANNEL_ID, message_id=message.message_id, added_by=ADMIN_USER_ID
    )
    if success: logger.info(f"✅ Auto-indexed: {movie_info.get('title')}")
    else: logger.error(f"Auto-index failed for: {movie_info.get('title')}")

# --- Admin Features (Aapke Original Code se) ---
async def is_admin(message: types.Message) -> bool:
    return message.from_user.id == ADMIN_USER_ID

@dp.message(Command("help"), F.func(is_admin))
async def admin_help(message: types.Message):
    help_text = """
👑 <b>Admin Commands</b> 👑
/stats - Detailed bot statistics.
/broadcast - Reply to a message to broadcast it.
/total_movies - View total indexed movies.
/cleanup_users - Remove inactive users.
/daily_report - Get a daily summary.
/system_health - Check system status.
/add_movie - Manually add a movie.
"""
    await message.answer(help_text)

@dp.message(Command("stats", "total_movies", "system_health", "daily_report"), F.func(is_admin))
async def stats_command(message: types.Message):
    user_count = await db.get_user_count()
    movie_count = await db.get_movie_count()
    await message.answer(f"📊 <b>System Health & Stats</b>\n\n"
                         f"👥 <b>Total Users:</b> {user_count:,}\n"
                         f"🎬 <b>Total Movies:</b> {movie_count:,}\n"
                         f"⚙️ <b>Status:</b> Operational ✅\n"
                         f"⏰ <b>Instance Uptime:</b> {get_uptime()}")

@dp.message(Command("broadcast"), F.func(is_admin))
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
        except:
            failed += 1
        
        if (success + failed) % 100 == 0:
            await progress_msg.edit_text(f"📤 Broadcasting...\n✅ Sent: {success}\n❌ Failed: {failed}\n📊 Total: {total_users}")
        await asyncio.sleep(0.05)
    
    await progress_msg.edit_text(f"✅ <b>Broadcast Complete!</b>\n\nSent to {success} users.\nFailed for {failed} users.")

@dp.message(Command("cleanup_users"), F.func(is_admin))
async def cleanup_users_command(message: types.Message):
    await message.answer("🧹 Inactive users ko clean kar rahe hain...")
    removed_count = await db.cleanup_inactive_users(days=30)
    new_count = await db.get_user_count()
    await message.answer(f"✅ Cleanup complete!\n- Deactivated: {removed_count} users\n- Active Users: {new_count}")

@dp.message(Command("add_movie"), F.func(is_admin))
async def add_movie_command(message: types.Message):
    if not message.reply_to_message or not (message.reply_to_message.video or message.reply_to_message.document):
        await message.answer("❌ Movie file ko reply karke command likhein: `/add_movie imdb_id | title | year`")
        return
    
    try:
        parts = message.text.replace('/add_movie', '').strip().split('|')
        imdb_id = parts[0].strip()
        title = parts[1].strip()
        year = parts[2].strip() if len(parts) > 2 else None
    except:
        await message.answer("❌ Format galat hai. Use: `/add_movie imdb_id | title | year`")
        return

    if await db.get_movie_by_imdb(imdb_id):
        await message.answer("⚠️ Is IMDB ID se movie pehle se hai!")
        return
        
    file_id = message.reply_to_message.video.file_id if message.reply_to_message.video else message.reply_to_message.document.file_id
    success = await db.add_movie(
        imdb_id=imdb_id, title=title, year=year, genre=None, rating=None, file_id=file_id, 
        channel_id=message.reply_to_message.chat.id, 
        message_id=message.reply_to_message.message_id, 
        added_by=ADMIN_USER_ID
    )
    if success: await message.answer(f"✅ Movie '{title}' add ho gayi hai.")
    else: await message.answer("❌ Movie add karne mein error aaya.")
