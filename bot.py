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
# CRITICAL FIX: Sabhi exceptions ko top-level 'aiogram.exceptions' se import karein.
from aiogram.exceptions import TelegramAPIError, ChatNotFound, TelegramBadRequest

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
# Environment variable mein set kiye gaye usernames
JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "MOVIEMAZASU")
USER_GROUP_USERNAME = os.getenv("USER_GROUP_USERNAME", "THEGREATMOVIESL9")
DATABASE_URL = os.getenv("DATABASE_URL")

# --- User Limit and Alternate Bots ---
CONCURRENT_LIMIT = 35 
ACTIVE_WINDOW_MINUTES = 5
# Sabhi alternate bots ki list
ALTERNATE_BOTS = ["Moviemaza91bot", "Moviemaza92bot", "Mazamovie9bot"]

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
    """Database connection ko zinda (alive) rakhne ke liye periodic ping."""
    while True:
        try:
            # Simple read operation to keep the connection open
            await db.get_user_count() 
        except Exception as e:
            logger.error(f"DB warming failed: {e}")
        await asyncio.sleep(4 * 60) # Har 4 minute mein check karein

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    db_task = asyncio.create_task(keep_db_alive())
    
    if RENDER_EXTERNAL_HOSTNAME:
        try:
            current_webhook = await bot.get_webhook_info()
            if current_webhook.url != WEBHOOK_URL:
                # Webhook URL ko set/update karein
                await bot.set_webhook(url=WEBHOOK_URL, allowed_updates=dp.resolve_used_update_types())
                logger.info(f"Webhook set to: {WEBHOOK_URL}")
        except Exception as e:
            logger.error(f"Error setting webhook: {e}")
            
    yield
    
    db_task.cancel() 
    if RENDER_EXTERNAL_HOSTNAME:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception as e:
            logger.error(f"Error deleting webhook: {e}")

app = FastAPI(lifespan=lifespan)

@app.post(WEBHOOK_PATH)
async def bot_webhook(update: dict):
    # CRITICAL: Yahan koi blocking code nahi hona chahiye
    telegram_update = Update(**update)
    await dp.feed_update(bot=bot, update=telegram_update)
    return {"ok": True}

@app.get("/")
async def ping():
    return {"status": "ok", "service": "Movie Bot is Live"}

# --- Helper Functions ---
def get_uptime():
    """Bot instance ka uptime calculation."""
    delta = datetime.utcnow() - start_time
    hours, remainder = divmod(int(delta.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    days, hours = divmod(hours, 24)
    if days > 0: return f"{days}d {hours}h {minutes}m"
    if hours > 0: return f"{hours}h {minutes}m"
    return f"{minutes}m {seconds}s"

async def check_user_membership(user_id: int) -> bool:
    """Channel aur Group dono mein user ki membership check karta hai."""
    try:
        # Channel check
        channel_member = await bot.get_chat_member(f"@{JOIN_CHANNEL_USERNAME}", user_id)
        if channel_member.status not in ['member', 'administrator', 'creator']:
            logger.info(f"User {user_id} failed channel check: @{JOIN_CHANNEL_USERNAME}")
            return False
        
        # Group check
        group_member = await bot.get_chat_member(f"@{USER_GROUP_USERNAME}", user_id)
        if group_member.status not in ['member', 'administrator', 'creator']:
            logger.info(f"User {user_id} failed group check: @{USER_GROUP_USERNAME}")
            return False
        
        return True
    except ChatNotFound as e:
        # Agar Channel/Group username galat hai (critical for user's ENV check)
        logger.error(f"MEMBERSHIP CHECK FAILED: ChatNotFound. Check JOIN_CHANNEL_USERNAME/USER_GROUP_USERNAME in ENV. Error: {e}")
        return False 
    except Exception as e: 
        # Kisi aur Telegram API error ya network issue ke liye
        logger.error(f"Membership check general failure for user {user_id}: {e}")
        return False

def get_join_keyboard():
    """Zaroori channels join karne ke liye keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Channel Join Karein (Compulsory)", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}")],
        [InlineKeyboardButton(text="👥 Group Join Karein (Compulsory)", url=f"https://t.me/{USER_GROUP_USERNAME}")],
        [InlineKeyboardButton(text="✅ I Have Joined Both (Maine Dono Join Kar Liye)", callback_data="check_join")]
    ])

def get_full_limit_keyboard():
    """Limit full hone par alternate bots ke liye keyboard."""
    # Har alternate bot ke liye ek naya row, jisse mobile UI behtar ho
    buttons = [[InlineKeyboardButton(text=f"🚀 @{bot_user} (Alternate Bot)", url=f"https://t.me/{bot_user}")] for bot_user in ALTERNATE_BOTS]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def extract_movie_info(caption: str):
    # Existing logic for extracting movie info
    if not caption: return None
    info = {}
    lines = caption.strip().split('\n')
    if lines:
        title = lines[0].strip()
        if len(lines) > 1 and re.search(r'S\d{1,2}', lines[1], re.IGNORECASE):
             title += " " + lines[1].strip()
        info['title'] = title
    imdb_match = re.search(r'(tt\d{7,})', caption)
    if imdb_match: info['imdb_id'] = imdb_match.group(1)
    year_match = re.search(r'\b(19|20)\d{2}\b', caption)
    if year_match: info['year'] = year_match.group(0)
    return info if 'title' in info else None

# --- Step 4: Bot Features (Handlers) ---

@dp.message(CommandStart())
async def start_command(message: types.Message):
    user_id = message.from_user.id
    first_name = message.from_user.first_name
    
    # User activity log karein (CRITICAL for concurrency check)
    await db.add_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name)

    if user_id == ADMIN_USER_ID:
        # --- Admin Path (Highly Professional Message) ---
        user_count = await db.get_user_count()
        movie_count = await db.get_movie_count() 
        concurrent_users = await db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES)
        
        admin_message = (
            f"👑 **Admin Console: {await bot.get_me().username}**\n"
            f"<i>Access Level: Full Management. All systems are Go.</i>\n\n"
            f"<b><u>System Performance & Metrics:</u></b>\n"
            f"📈 **Active Users (5 Min):** <pre>{concurrent_users:,}/{CONCURRENT_LIMIT}</pre>\n"
            f"👥 **Total Registered Users:** <pre>{user_count:,}</pre>\n"
            f"🎬 **Total Indexed Movies:** <pre>{movie_count:,}</pre>\n"
            f"⏰ **Service Uptime:** <pre>{get_uptime()}</pre>\n\n"
            f"<b><u>Management Commands:</u></b>\n"
            f"🔹 /stats - View real-time system stats.\n"
            f"🔹 /broadcast - Send announcement (reply to message).\n"
            f"🔹 /cleanup_users - Deactivate old accounts.\n"
            f"🔹 /add_movie - Manually index content."
        )
        await message.answer(admin_message)
        return

    # --- Regular User Path (Hindi-English Mix Message) ---
    if not await check_user_membership(user_id):
        welcome_text = (
            f"🎬 **Namaskar {first_name}!** \n\n"
            f"Welcome to the **High-Speed Movie Library Bot**. To begin your search, a quick, mandatory verification is required.\n\n"
            f"➡️ **Follow these steps carefully:**\n"
            f"1. **Join** our official Channel.\n"
            f"2. **Join** our official Group.\n"
            f"3. Click the **'I Have Joined Both'** button below."
        )
        await message.answer(welcome_text, reply_markup=get_join_keyboard())
    else:
        active_users = await db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES)
        welcome_text = (
            f"🎉 **Welcome Back, {first_name}!** You have full access. \n\n"
            f"➡️ **Search Shuru Karein:** Bas **movie ya web series ka naam likhein** aur hum aapko file bhejenge. For better results, include the year (e.g., *Avatar 2009*).\n\n"
            f"<i>ℹ️ **Service Notice:** Humari Free Tier service mein {CONCURRENT_LIMIT} users ki seema hai. Abhi **{active_users}** users active hain.</i>"
        )
        await message.answer(welcome_text)


@dp.callback_query(F.data == "check_join")
async def check_join_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    first_name = callback.from_user.first_name
    
    # CRITICAL FIX: Callback query ko immediately answer karein, taaki Telegram timeout na de.
    await callback.answer("Membership check ki jaa rahi hai...")
    
    try:
        is_member = await check_user_membership(user_id)
        
        if is_member:
            # Success Case
            active_users = await db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES)
            success_text = (
                f"✅ **Verification Successful, {first_name}!**\n\n"
                f"You now have **unlimited access** to the library. Start searching for your favorite content now!\n\n"
                f"<i>ℹ️ **Service Notice:** {CONCURRENT_LIMIT} users ki seema hai. Abhi **{active_users}** users active hain.</i>"
            )
            try:
                # Message edit karne ki koshish karein
                await callback.message.edit_text(success_text)
            except (TelegramAPIError, TelegramBadRequest):
                 # Agar edit fail hota hai (jo ki mobile par aam hai), to naya message bhejein
                logger.warning(f"Failed to edit message in check_join_callback for user {user_id}. Sending new success message.")
                await bot.send_message(user_id, success_text)
        else:
            # Failure Case
            fail_text = "❌ **Verification Failed!**\n\nKripya **dhyan se** dekhein ki aapne Channel aur Group **dono** ko join kar liya hai. Agar aapne abhi join kiya hai, to phir se **'I Have Joined Both'** button dabayein."
            # Message edit karein aur keyboard wahi rehne dein
            await callback.message.edit_text(fail_text, reply_markup=get_join_keyboard())
    
    except Exception as e:
        logger.error(f"Critical error in check_join_callback for user {user_id}: {e}")
        await bot.send_message(user_id,
            "⚠️ **Technical Error:** Verification process mein rukavat aa gayi hai. Kripya thodi der baad phir koshish karein ya /start command ka upyog karein."
        )


@dp.message(F.text & ~F.text.startswith('/') & F.chat.type == 'private')
async def search_movie_handler(message: types.Message):
    user_id = message.from_user.id
    
    # 1. User activity log karein
    await db.add_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name)

    # 2. Membership Check
    if user_id != ADMIN_USER_ID and not await check_user_membership(user_id):
        await message.answer("❌ **Pehle Zaroori Channels Join Karein** ❌\n\nKripya movie search karne se pehle zaroori Channel aur Group join karein aur **'I Have Joined Both'** button dabayein.", reply_markup=get_join_keyboard())
        return

    # 3. Concurrency Limit Check (Robust and Professional Message)
    if user_id != ADMIN_USER_ID:
        concurrent_users = await db.get_concurrent_user_count(minutes=ACTIVE_WINDOW_MINUTES)
        
        if concurrent_users > CONCURRENT_LIMIT:
            limit_message = (
                f"⚠️ **Service Capacity Full: {CONCURRENT_LIMIT} User Limit Reached** ⚠️\n\n"
                f"Dear User, our free service tier is currently operating at **maximum capacity**. We apologize for the temporary delay in service.\n\n"
                f"For uninterrupted and high-speed movie access, please use one of our **Alternate Bots** below:"
            )
            await message.answer(limit_message, reply_markup=get_full_limit_keyboard())
            return
            
    # 4. Search Logic
    original_query = message.text.strip()
    if len(original_query) < 2:
        await message.answer("🤔 Search ke liye kripya kam se kam 2 akshar (characters) likhein.")
        return

    searching_msg = await message.answer(f"🔍 **'{original_query}'** ki khoj jaari hai... Please wait.")
    
    try:
        processed_query = clean_text_for_search(original_query)
        # Search limit 20 aur threshold 65 use karein
        best_results = await db.super_search_movies(processed_query, limit=20) 

        if not best_results:
            await searching_msg.edit_text(f"🥲 Maaf kijiye, **'{original_query}'** ke liye koi bhi perfect match nahi mila. Kripya doosra naam ya spelling try karein.")
            return

        buttons = [[InlineKeyboardButton(text=movie['title'], callback_data=f"get_{movie['imdb_id']}")] for movie in best_results]
        await searching_msg.edit_text(f"🎬 **'{original_query}'** ke liye {len(best_results)} behtareen results. File paane ke liye chunein:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    except Exception as e:
        logger.error(f"Search operation failed for user {user_id}. Error: {e}")
        await searching_msg.edit_text("❌ **Internal Error:** Search system mein koi rukavat aa gayi hai. Kripya thodi der baad phir prayas karein.")


@dp.callback_query(F.data.startswith("get_"))
async def get_movie_callback(callback: types.CallbackQuery):
    # CRITICAL: Callback ko answer karein
    await callback.answer("File forward ki jaa rahi hai...")
    imdb_id = callback.data.split('_', 1)[1]
    
    movie = await db.get_movie_by_imdb(imdb_id) 

    if not movie:
        await callback.message.edit_text("❌ Yeh movie ab database mein uplabdh nahi hai.")
        return
        
    try:
        # Message update karein (sirf ek line mein)
        await callback.message.edit_text(f"✅ **{movie['title']}** - File bheji jaa rahi hai. Kripya apne chats check karein.")
        
        # File forward karein
        await bot.forward_message(chat_id=callback.from_user.id, from_chat_id=int(movie['channel_id']), message_id=movie['message_id']) 
        
    except TelegramAPIError as e:
        logger.error(f"Movie forward/edit error for {imdb_id} to user {callback.from_user.id}. Error: {e}")
        # Agar forward ya edit fail hota hai, to naya message bhejkar inform karein
        await bot.send_message(callback.from_user.id, f"❗️ **Takneeki Samasya:** Movie **{movie['title']}** ko forward karne mein koi rukavat aa gayi hai. Kripya phir se prayas karein.")
    except Exception as e:
        logger.error(f"Movie callback critical error for {imdb_id}. Error: {e}")
        await bot.send_message(callback.from_user.id, "❌ **Critical System Error:** Request poora nahi ho paya. Kripya /start karein.")

# --- Admin Commands (Stable) ---

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
        await message.answer("❌ Movie file ko reply k करके command likhein: `/add_movie imdb_id | title | year`")
        return
    
    try:
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
