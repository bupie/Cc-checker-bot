import logging
import threading
from os import getenv

# Flask health endpoint so Render sees a listening web process
from flask import Flask

from huepy import bad
from pyromod import Client
from pyrogram import filters
from pyrogram.enums import ParseMode, ChatMemberStatus
from pyrogram.types import CallbackQuery, Message

from utilsdf.functions import bot_on
from utilsdf.db import Database
from utilsdf.vars import PREFIXES

# --- Load secrets / config from environment (no hardcoded secrets) ---
# Note: API_ID must be an int in Pyrogram
API_ID = int(getenv("API_ID", "28386099"))  # set on Render
API_HASH = getenv("API_HASH", "a0057fbf1ca49ce5e9d26fd4afd6e78b")
BOT_TOKEN = getenv("BOT_TOKEN", "")
CHANNEL_LOGS = getenv("CHANNEL_LOGS", "-1002257940704")  # e.g. -100xxxxx

# basic safety: fail fast if critical envs missing (adjust behavior if needed)
if not API_ID or not API_HASH or not BOT_TOKEN:
    logging.basicConfig(level=logging.INFO)
    logging.critical("Missing required environment variables: API_ID/API_HASH/BOT_TOKEN")
    raise SystemExit("Set API_ID, API_HASH and BOT_TOKEN in environment before starting.")

# --- Pyrogram client ---
app = Client(
    "bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    plugins=dict(root="plugins"),
    parse_mode=ParseMode.HTML,
)

bot_on()
logging.basicConfig(level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.CRITICAL)

# --- Flask health app (so Render can keep process alive and show web up) ---
flask_app = Flask("health")

@flask_app.route("/", methods=["GET"])
def health_index():
    return "OK", 200

def run_flask():
    port = int(getenv("PORT", "8000"))
    # Bind to 0.0.0.0 so Render can reach it
    flask_app.run(host="0.0.0.0", port=port)

# Start flask in background thread (daemon so it won't block shutdown)
threading.Thread(target=run_flask, daemon=True).start()

# --- Bot handlers (your existing logic, env-safe) ---
@app.on_callback_query()
async def warn_user(client: Client, callback_query: CallbackQuery):
    # guard: reply_to_message may be None
    if callback_query.message.reply_to_message and callback_query.message.reply_to_message.from_user and (
        callback_query.from_user.id
        != callback_query.message.reply_to_message.from_user.id
    ):
        await callback_query.answer("Usa tu menu! ⚠️", show_alert=True)
        return
    await callback_query.continue_propagation()

@app.on_message(filters.text)
async def user_ban(client: Client, m: Message):
    if not m.from_user:
        return
    if not m.text:
        return
    try:
        if not m.text[0] in PREFIXES:
            return
    except UnicodeDecodeError:
        return

    chat_id = m.chat.id
    with Database() as db:
        if chat_id == -1002257940704:
            async for member in m.chat.get_members():
                if not member.user:
                    continue
                if member.status == ChatMemberStatus.ADMINISTRATOR:
                    continue
                user_id = member.user.id
                if db.is_seller_or_admin(user_id):
                    continue
                is_premium = db.is_premium(user_id)
                if is_premium:
                    continue
                if db.user_has_credits(user_id):
                    continue
                await m.chat.ban_member(user_id)
                info = db.get_info_user(user_id)
                # prefer using CHANNEL_LOGS env var if you want
                log_chat = int(CHANNEL_LOGS) if CHANNEL_LOGS else -1001897182152
                await client.send_message(log_chat, f"<b>User eliminado: @{info.get('USERNAME','')}</b>")

        user_id = m.from_user.id
        username = m.from_user.username
        db.remove_expireds_users()
        banned = db.is_ban(user_id)
        if banned:
            return
        db.register_user(user_id, username)
        await m.continue_propagation()

if __name__ == "__main__":
    # Pyrogram will start and keep process alive; Flask thread serves health checks.
    app.run()
