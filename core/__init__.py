from core.config import (
    BASE_DIR, LOG_DIR, DOWNLOAD_DIR, THUMBNAIL_DIR, DB_NAME,
    API_ID, API_HASH, BOT_TOKEN, ADMIN_CHAT_ID, MONGO_URI, PORT, BOT_USERNAME,
    CHANNEL_ID, CHANNEL_USERNAME, DUMP_CHANNEL_ID, DUMP_CHANNEL_USERNAME,
    FIXED_THUMBNAIL_URL, START_PIC_URL, STICKER_ID,
    AUTO_DOWNLOAD_STATE_FILE, QUALITY_SETTINGS_FILE, SESSION_FILE, JSON_DATA_FILE,
    DRAMA_HEADERS, YTDLP_HEADERS, TMDB_API_KEY,
    Config, logger
)

from core.database import (
    mongo_client, db,
    processed_episodes_collection, drama_banners_collection,
    drama_hashtags_collection, admins_collection, bot_settings_collection,
    drama_channels_collection,
    load_json_data, save_json_data,
    save_bot_setting, load_bot_setting,
    add_drama_channel, remove_drama_channel, get_drama_channel, get_all_drama_channels
)

from core.client import (
    client, pyro_client, PYROFORK_AVAILABLE, FFMPEG_AVAILABLE,
    currently_processing, processing_lock
)

from core.state import (
    DramaQueue, QualitySettings, BotSettings, AutoDownloadState, UserState,
    drama_queue, quality_settings, bot_settings, auto_download_state, user_states,
    EpisodeState, EpisodeTracker, episode_tracker
)

from core.utils import (
    sanitize_filename, create_short_name, format_size, format_speed, format_time, format_filename,
    download_start_pic, download_start_pic_if_not_exists,
    get_fixed_thumbnail, is_admin, add_admin, remove_admin,
    is_episode_processed, update_processed_episode, update_processed_qualities,
    mark_episode_processed,
    is_banner_posted, mark_banner_posted, get_drama_hashtag, get_anime_hashtag,
    encode, generate_batch_link, generate_single_link,
    ProgressMessage, UploadProgressBar,
    safe_edit, safe_respond, safe_send_message
)

from core.drama_scraper import (
    get_latest_dramas, search_drama, get_episode_list,
    get_episode_download_links, bypass_hubcloud,
    get_drama_info, download_drama_poster,
    detect_audio_type, extract_episode_number, extract_drama_title,
)

from core.download import (
    rename_video_with_ffmpeg, fast_upload_file,
    post_drama_with_buttons,
    post_drama_batch_with_buttons,
    download_episode
)

from core.scheduler import (
    setup_scheduler, check_for_new_episodes, auto_download_latest_episode,
    process_pending_queue, process_single_episode,
    check_and_process_next_episode, get_currently_processing, set_currently_processing
)

from core.handlers import register_handlers

__all__ = [
    'BASE_DIR', 'LOG_DIR', 'DOWNLOAD_DIR', 'THUMBNAIL_DIR', 'DB_NAME',
    'API_ID', 'API_HASH', 'BOT_TOKEN', 'ADMIN_CHAT_ID', 'MONGO_URI', 'PORT', 'BOT_USERNAME',
    'CHANNEL_ID', 'CHANNEL_USERNAME', 'DUMP_CHANNEL_ID', 'DUMP_CHANNEL_USERNAME',
    'FIXED_THUMBNAIL_URL', 'START_PIC_URL', 'STICKER_ID',
    'DRAMA_HEADERS', 'YTDLP_HEADERS', 'TMDB_API_KEY',
    'Config', 'logger',

    'mongo_client', 'db',
    'processed_episodes_collection', 'drama_banners_collection',
    'drama_hashtags_collection', 'admins_collection', 'bot_settings_collection',
    'drama_channels_collection',
    'load_json_data', 'save_json_data',
    'add_drama_channel', 'remove_drama_channel', 'get_drama_channel', 'get_all_drama_channels',

    'client', 'pyro_client', 'PYROFORK_AVAILABLE', 'FFMPEG_AVAILABLE',
    'currently_processing', 'processing_lock',

    'DramaQueue', 'QualitySettings', 'BotSettings', 'AutoDownloadState', 'UserState',
    'drama_queue', 'quality_settings', 'bot_settings', 'auto_download_state', 'user_states',
    'EpisodeState', 'EpisodeTracker', 'episode_tracker',

    'sanitize_filename', 'create_short_name', 'format_size', 'format_speed', 'format_time', 'format_filename',
    'download_start_pic', 'download_start_pic_if_not_exists',
    'get_fixed_thumbnail', 'is_admin', 'add_admin', 'remove_admin',
    'is_episode_processed', 'update_processed_episode', 'update_processed_qualities', 'mark_episode_processed',
    'is_banner_posted', 'mark_banner_posted', 'get_drama_hashtag', 'get_anime_hashtag',
    'encode', 'generate_batch_link', 'generate_single_link',
    'ProgressMessage', 'UploadProgressBar',
    'safe_edit', 'safe_respond', 'safe_send_message',

    'get_latest_dramas', 'search_drama', 'get_episode_list',
    'get_episode_download_links', 'bypass_hubcloud',
    'get_drama_info', 'download_drama_poster',
    'detect_audio_type', 'extract_episode_number', 'extract_drama_title',

    'rename_video_with_ffmpeg', 'fast_upload_file',
    'post_drama_with_buttons', 'post_drama_batch_with_buttons', 'download_episode',

    'setup_scheduler', 'check_for_new_episodes', 'auto_download_latest_episode',
    'process_pending_queue', 'process_single_episode',
    'check_and_process_next_episode', 'get_currently_processing', 'set_currently_processing',

    'register_handlers'
]
