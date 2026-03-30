from __future__ import annotations
import logging
import subprocess
import asyncio

from telethon import TelegramClient

from core.config import (
    API_ID, API_HASH, BOT_TOKEN, SESSION_FILE,
    FFMPEG_PATH
)

logger = logging.getLogger(__name__)

client = TelegramClient(str(SESSION_FILE), API_ID, API_HASH)

currently_processing = False
processing_lock = asyncio.Lock()

PYROFORK_AVAILABLE = False
pyro_client = None

try:
    from pyrogram import Client as PyroClient
    from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
    PYROFORK_AVAILABLE = True
    logger.info("Pyrofork imported successfully for fast uploads")
except ImportError:
    PYROFORK_AVAILABLE = False
    logger.warning("Pyrofork not available. Using standard Telethon uploads.")

if PYROFORK_AVAILABLE:
    try:
        pyro_client = PyroClient(
            "DramaMaza",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN
        )
        logger.info("Pyrofork client initialized for fast uploads")
    except Exception as e:
        logger.error(f"Failed to initialize Pyrofork client: {e}")
        pyro_client = None

FFMPEG_AVAILABLE = False
try:
    subprocess.run([FFMPEG_PATH, "-version"], check=True, capture_output=True)
    FFMPEG_AVAILABLE = True
    logger.info("FFmpeg is available")
except (subprocess.CalledProcessError, FileNotFoundError):
    logger.error("FFmpeg is not available. Add the FFmpeg buildpack on Heroku. Video conversion will be skipped.")

try:
    import yt_dlp
except ImportError:
    logger.warning("yt-dlp not installed. Add it to requirements.txt.")
