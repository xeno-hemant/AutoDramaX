from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from typing import Any

if not load_dotenv():
    logging.warning("No .env file found or failed to load environment variables")

env_file = Path(".env")
if not env_file.exists():
    logging.warning(f"No .env file found at {env_file.absolute()}")
elif not env_file.read_text().strip():
    logging.warning(f".env file exists but is empty at {env_file.absolute()}")

BASE_DIR = Path.cwd()
LOG_DIR = BASE_DIR / "logs"
DOWNLOAD_DIR = BASE_DIR / "drama_downloads"
THUMBNAIL_DIR = BASE_DIR / "thumbnails"
DB_NAME = "DramaMaza"

for directory in [LOG_DIR, DOWNLOAD_DIR, THUMBNAIL_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "bot.log"
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(LOG_FILE))
    ]
)
logger = logging.getLogger(__name__)


class Config:
    ABC = 1


def get_env_var(key: str, default: Any = None, required: bool = True) -> Any:
    value = os.environ.get(key, default)

    if required and (value is None or (isinstance(value, str) and value.strip() == "")):
        env_file = Path(".env")
        if not env_file.exists():
            raise ValueError(
                f"Environment variable {key} is required but not set.\n"
                f"Please create a .env file in {env_file.absolute()}"
            )
        else:
            raise ValueError(
                f"Environment variable {key} is required but not set.\n"
                f"Please add {key}=your_value to your .env file"
            )

    logger.debug(f"Loaded environment variable: {key}")
    return value


logger.info("Loading essential configuration...")
try:
    API_ID = int(get_env_var("API_ID", "26657288"))
    API_HASH = get_env_var("API_HASH", "00536e431477dbb16583d5b85813aa72")
    BOT_TOKEN = get_env_var("BOT_TOKEN", "8532741763:AAHosMQZezai7tt2qnm34iqgxFzb4XLxl3I")
    ADMIN_CHAT_ID = int(get_env_var("ADMIN_CHAT_ID", "1008989961"))
    MONGO_URI = get_env_var("MONGO_URI", "mongodb+srv://vishalleaks:vishalleaks@cluster0.hhhbseg.mongodb.net/?appName=Cluster0", required=False)
    PORT = int(get_env_var("PORT", "8090"))
    BOT_USERNAME = get_env_var("BOT_USERNAME", "DramaXAutoBot")
    logger.info("Successfully loaded all environment variables")
except ValueError as e:
    logger.error(f"Environment variable error: {e}")
    raise

DB_NAME = get_env_var("DB_NAME", "DramaMaza", required=False)

# TMDB API Key for drama metadata
TMDB_API_KEY = get_env_var("TMDB_API_KEY", "", required=False)

def get_admins_from_env():
    raw = get_env_var("ADMIN_CHAT_ID", "1008989961")
    return [int(x.strip()) for x in raw.split(",") if x.strip()]

ADMINS = get_admins_from_env()
OWNER_ID = ADMINS[0]

CHANNEL_ID = get_env_var("CHANNEL_ID", "-1003748655987", required=False)
CHANNEL_NAME = get_env_var("CHANNEL_NAME", "𝖠ɴɪ𝖶ᴇʙ 𝖲ʜᴏɢᴜɴᴀᴛᴇ", required=False)
CHANNEL_USERNAME = get_env_var("CHANNEL_USERNAME", "@testautoshit", required=False)

DUMP_CHANNEL_ID = get_env_var("DUMP_CHANNEL_ID", "-1003558271224", required=False)
DUMP_CHANNEL_USERNAME = get_env_var("DUMP_CHANNEL_USERNAME", "@testdumpsbots", required=False)

if CHANNEL_ID:
    try:
        CHANNEL_ID = int(CHANNEL_ID)
        logger.info(f"Channel ID configured: {CHANNEL_ID}")
    except ValueError:
        logger.warning("Invalid CHANNEL_ID provided - must be a number. Falling back to username if available.")
        CHANNEL_ID = None

if CHANNEL_USERNAME:
    if not CHANNEL_USERNAME.startswith('@'):
        CHANNEL_USERNAME = f"{CHANNEL_USERNAME}"
    logger.info(f"Channel username configured: {CHANNEL_USERNAME}")

if DUMP_CHANNEL_ID:
    try:
        DUMP_CHANNEL_ID = int(DUMP_CHANNEL_ID)
        logger.info(f"Dump Channel ID configured: {DUMP_CHANNEL_ID}")
    except ValueError:
        logger.warning("Invalid DUMP_CHANNEL_ID provided - must be a number. Falling back to username if available.")
        DUMP_CHANNEL_ID = None

if DUMP_CHANNEL_USERNAME:
    if not DUMP_CHANNEL_USERNAME.startswith('@'):
        DUMP_CHANNEL_USERNAME = f"@{DUMP_CHANNEL_USERNAME}"
    logger.info(f"Dump channel username configured: {DUMP_CHANNEL_USERNAME}")

if not CHANNEL_ID and not CHANNEL_USERNAME:
    logger.warning("No main channel ID or username configured. Files will only be sent to dump channel.")

if not DUMP_CHANNEL_ID and not DUMP_CHANNEL_USERNAME:
    logger.warning("No dump channel ID or username configured. Files will only be sent to users directly.")

FIXED_THUMBNAIL_URL = get_env_var(
    "FIXED_THUMBNAIL_PIC",
    "https://graph.org/file/62a19dea40490c518c572-a8967056c53715ab8f.jpg",
    required=False
)

START_PIC_URL = get_env_var(
    "START_PIC_URL",
    "https://graph.org/file/eb66e7c467c86c1e35621-da161d06cb706996c2.jpg",
    required=False
)

STICKER_ID = get_env_var(
    "STICKER_ID",
    "CAACAgUAAxkBAAEQ1G9pxpfrnwRNoo21sE1xYpZlOzGOdAACQBkAAohvoFdjV3SWX3VB-joE",
    required=False
)

DELETE_TIMER = int(get_env_var("DELETE_TIMER", 1800, required=False))
AUTO_DOWNLOAD_STATE_FILE = BASE_DIR / "auto_download_state.json"
QUALITY_SETTINGS_FILE = BASE_DIR / "quality_settings.json"
SESSION_FILE = BASE_DIR / "drama_bot.session"
JSON_DATA_FILE = BASE_DIR / "drama_data.json"
FFMPEG_PATH = "ffmpeg"

# Headers for kdramamaza.net requests
DRAMA_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

# yt-dlp download headers
YTDLP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
}

SEARCH, SELECT_DRAMA, SELECT_EPISODE, DOWNLOADING = range(4)
AUTO_DISABLED, AUTO_ENABLED = range(2)

HELP_TEXT = '''<b>
<blockquote>✦ 𝗛𝗘𝗟𝗣𝗘𝗥 ✦</blockquote>
──────────────────
<blockquote>シ 𝗖𝗢𝗠𝗠𝗔𝗡𝗗𝗦:</blockquote>
<blockquote expandable><code>/cancel</code> - ᴄᴀɴᴄᴇʟ ᴄᴜʀʀᴇɴᴛ ᴏᴘᴇʀᴀᴛɪᴏɴ
<code>/latest</code> - ɢᴇᴛ ʟᴀᴛᴇsᴛ ᴅʀᴀᴍᴀs ғʀᴏᴍ ᴋᴅʀᴀᴍᴀᴍᴀᴢᴀ
<code>/ongoing</code> - ɢᴇᴛ ᴄᴜʀʀᴇɴᴛʟʏ ᴏɴɢᴏɪɴɢ ᴅʀᴀᴍᴀs
<code>/del_timer</code> - sᴇᴛ ғɪʟᴇ ᴅᴇʟᴇᴛᴇ ᴛɪᴍᴇʀ
<code>/addchnl [id] [name]</code> - sᴇᴛ ᴀ ᴅʀᴀᴍᴀ-sᴘᴇᴄɪғɪᴄ ᴄʜᴀɴɴᴇʟ
<code>/removechnl [id] [name]</code> - ʀᴇᴍᴏᴠᴇ ᴀ ᴅʀᴀᴍᴀ ᴄʜᴀɴɴᴇʟ
<code>/listchnl</code> - ʟɪsᴛ ᴀʟʟ ᴅʀᴀᴍᴀ ᴄʜᴀɴɴᴇʟs
<code>/set_request_time [HH:MM]</code> - sᴇᴛ ᴅᴀɪʟʏ ʀᴇǫᴜᴇsᴛ ᴘʀᴏᴄᴇssɪɴɢ ᴛɪᴍᴇ (IST)
<code>/set_max_requests [number]</code> - sᴇᴛ ᴍᴀx ᴄᴏɴᴄᴜʀʀᴇɴᴛ ʀᴇǫᴜᴇsᴛs
<code>/view_requests</code> - sʜᴏᴡ ᴘᴇɴᴅɪɴɢ ʀᴇǫᴜᴇsᴛs
<code>/set_request_group [group_id]</code> - sᴇᴛ ᴛʜᴇ ʀᴇǫᴜᴇsᴛ ɢʀᴏᴜᴘ
<code>/request [drama name]</code> or <code>#request [drama name]</code> - ʀᴇǫᴜᴇsᴛ ᴀ ᴅʀᴀᴍᴀ
<code>/addtask [number]</code> - ᴅᴏᴡɴʟᴏᴀᴅ sᴘᴇᴄɪғɪᴄ ᴅʀᴀᴍᴀ ғʀᴏᴍ ʟᴀᴛᴇsᴛ ʟɪsᴛ
<code>/redownload [number]</code> - ғᴏʀᴄᴇ ʀᴇᴅᴏᴡɴʟᴏᴀᴅ ᴀ sᴘᴇᴄɪғɪᴄ ᴅʀᴀᴍᴀ
<code>/add_admin [user_id]</code> - ᴀᴅᴅ ᴀ ɴᴇᴡ ᴀᴅᴍɪɴ
<code>/remove_admin [user_id]</code> - ʀᴇᴍᴏᴠᴇ ᴀɴ ᴀᴅᴍɪɴ</blockquote expandable>
──────────────────
<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/aniweb_shogunate'>𝖠ɴɪ𝖶ᴇʙ 𝖲ʜᴏɢᴜɴᴀᴛᴇ</a></blockquote></b>'''
