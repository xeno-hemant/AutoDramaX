from __future__ import annotations
import os
import time
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

import schedule
from zoneinfo import ZoneInfo

from core.config import (
    DOWNLOAD_DIR, YTDLP_HEADERS, ADMIN_CHAT_ID,
    CHANNEL_USERNAME, BOT_USERNAME, CHANNEL_NAME,
    DUMP_CHANNEL_ID, DUMP_CHANNEL_USERNAME
)
from core.client import client, FFMPEG_AVAILABLE, currently_processing
from core.state import (
    auto_download_state, drama_queue,
    episode_tracker, EpisodeState
)
from core.utils import (
    sanitize_filename, format_filename, format_size, format_speed,
    get_fixed_thumbnail, is_episode_processed, update_processed_episode, mark_episode_processed,
    ProgressMessage, UploadProgressBar, safe_edit
)
from core.drama_scraper import (
    get_latest_dramas, get_episode_download_links, bypass_hubcloud,
    get_drama_info, download_drama_poster, detect_audio_type,
    search_drama, get_episode_list
)
from core.download import (
    rename_video_with_ffmpeg, post_drama_with_buttons, robust_upload_file
)

logger = logging.getLogger(__name__)

try:
    import yt_dlp
except ImportError:
    logger.error("yt-dlp not installed")

from telethon.errors import FloodWaitError


_currently_processing = False
_scheduler_lock = asyncio.Lock() if asyncio else None


def get_currently_processing():
    global _currently_processing
    return _currently_processing


def set_currently_processing(value: bool):
    global _currently_processing
    _currently_processing = value


def _get_scheduler_lock():
    global _scheduler_lock
    if _scheduler_lock is None:
        try:
            _scheduler_lock = asyncio.Lock()
        except RuntimeError:
            pass
    return _scheduler_lock


async def auto_download_latest_episode():
    global _currently_processing

    logger.info("Starting auto drama download process...")

    if _currently_processing:
        logger.info("Already processing a drama. Skipping auto check.")
        return False

    _currently_processing = True
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    progress = None
    if ADMIN_CHAT_ID:
        progress = ProgressMessage(client, ADMIN_CHAT_ID, "<b>Auto drama processing started...</b>")
        await progress.send()

    try:
        # Cooldown check
        if auto_download_state.last_checked:
            last_check = datetime.fromisoformat(auto_download_state.last_checked)
            time_since_last_check = (datetime.now() - last_check).total_seconds()
            cooldown_period = auto_download_state.interval / 2
            if time_since_last_check < cooldown_period:
                logger.info(f"Skipping auto check (last check was {time_since_last_check:.1f}s ago)")
                return False

        if progress:
            await progress.update("<b><blockquote>ᴄʜᴇᴄᴋɪɴɢ ᴋᴅʀᴀᴍᴀᴍᴀᴢᴀ ғᴇᴇᴅ...</blockquote></b>", parse_mode='html')

        # Step 1: Fetch latest dramas from RSS
        latest_dramas = await get_latest_dramas(page=1)
        if not latest_dramas:
            logger.error("Failed to get latest dramas from RSS")
            if progress:
                await progress.update("<b><blockquote>ғᴀɪʟᴇᴅ ᴛᴏ ɢᴇᴛ ʟᴀᴛᴇsᴛ ᴅʀᴀᴍᴀs</blockquote></b>", parse_mode='html')
            return False

        # Find first unprocessed drama episode
        target_drama = None
        for drama in latest_dramas:
            drama_title = drama.get('drama_title', 'Unknown Drama')
            episode_number = drama.get('episode', 1)
            if not is_episode_processed(drama_title, episode_number):
                target_drama = drama
                break

        if not target_drama:
            logger.info("All recent drama episodes already processed.")
            if progress:
                await progress.update("<b><blockquote>ᴀʟʟ ʀᴇᴄᴇɴᴛ ᴅʀᴀᴍᴀs ᴀʟʀᴇᴀᴅʏ ᴘʀᴏᴄᴇssᴇᴅ.</blockquote></b>", parse_mode='html')
            return True

        drama_title = target_drama['drama_title']
        episode_number = target_drama['episode']
        episode_url = target_drama['url']
        audio_type = target_drama['audio_type']

        logger.info(f"Processing drama: {drama_title} Episode {episode_number} ({audio_type})")

        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗖𝗛𝗘𝗖𝗞𝗜𝗡𝗚 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Dʀᴀᴍᴀ: {drama_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Aᴜᴅɪᴏ: {audio_type}\n"
                f"・ Sᴛᴀᴛᴜs: Fɪɴᴅɪɴɢ ᴅᴏᴡɴʟᴏᴀᴅ ʟɪɴᴋs</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )

        # Step 2: Get HubCloud download links from episode page
        hub_links = await get_episode_download_links(episode_url)
        if not hub_links:
            logger.error(f"No HubCloud links found for {drama_title} Ep{episode_number}")
            if progress:
                await progress.update(
                    f"<b><blockquote>ɴᴏ ᴅᴏᴡɴʟᴏᴀᴅ ʟɪɴᴋs ғᴏᴜɴᴅ ғᴏʀ {drama_title} Ep{episode_number}</blockquote></b>",
                    parse_mode='html'
                )
            return False

        # Pick the best link (use audio_type match or first link)
        hub_link_info = hub_links[0]
        for lnk in hub_links:
            if lnk.get('audio_type') == audio_type:
                hub_link_info = lnk
                break

        hub_url = hub_link_info['href']
        audio_type = hub_link_info.get('audio_type', audio_type)

        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗕𝗬𝗣𝗔𝗦𝗦𝗜𝗡𝗚 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Dʀᴀᴍᴀ: {drama_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Sᴛᴀᴛᴜs: Bʏᴘᴀssɪɴɢ HᴜʙCʟᴏᴜᴅ...</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )

        # Step 3: Bypass HubCloud to get direct download URL
        loop = asyncio.get_event_loop()
        direct_url = None
        try:
            direct_url = await loop.run_in_executor(None, bypass_hubcloud, hub_url)
        except Exception as e:
            logger.error(f"HubCloud bypass failed: {e}")

        if not direct_url:
            logger.error(f"Could not bypass HubCloud for {drama_title} Ep{episode_number}")
            if progress:
                await progress.update(
                    f"<b><blockquote>Hᴜʙᴄʟᴏᴜᴅ ʙʏᴘᴀss ғᴀɪʟᴇᴅ ғᴏʀ {drama_title} Ep{episode_number}</blockquote></b>",
                    parse_mode='html'
                )
            return False

        logger.info(f"Direct URL obtained for {drama_title} Ep{episode_number}: {direct_url[:80]}...")

        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Dʀᴀᴍᴀ: {drama_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Aᴜᴅɪᴏ: {audio_type}\n"
                f"・ Sᴛᴀᴛᴜs: Dᴏᴡɴʟᴏᴀᴅɪɴɢ...</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )

        # Step 4: Download with yt-dlp
        safe_title = sanitize_filename(drama_title)
        ep_str = f"E{episode_number:02d}" if isinstance(episode_number, int) else f"E{episode_number}"
        audio_short = "HIN" if "Hindi" in audio_type else "SUB"
        filename_base = f"{safe_title}_{ep_str}_{audio_short}"
        download_path = os.path.join(str(DOWNLOAD_DIR), f"{filename_base}.%(ext)s")

        last_update = time.time()

        def progress_hook(d):
            nonlocal last_update
            if d['status'] == 'downloading':
                current_time = time.time()
                if current_time - last_update >= 4:
                    downloaded = d.get('downloaded_bytes', 0) or 0
                    total = d.get('total_bytes', 1) or 1
                    speed = d.get('speed', 0) or 0
                    percent = min(100, (downloaded / total) * 100) if total > 0 else 0
                    if progress:
                        txt = (
                            f"<b><blockquote>✦ 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ✦</blockquote>\n"
                            f"──────────────────\n"
                            f"<blockquote>・ Dʀᴀᴍᴀ: {drama_title}\n"
                            f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                            f"・ Pʀᴏɢʀᴇss: {percent:.1f}%\n"
                            f"・ Sɪᴢᴇ: {format_size(downloaded)}/{format_size(total)}\n"
                            f"・ Sᴘᴇᴇᴅ: {format_speed(speed)}</blockquote>\n"
                            f"──────────────────\n"
                            f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>"
                        )
                        try:
                            asyncio.create_task(progress.update(txt, parse_mode='html'))
                        except Exception:
                            pass
                    last_update = current_time

        ydl_opts = {
            'outtmpl': download_path,
            'format': 'bestvideo+bestaudio/best',
            'merge_output_format': 'mkv',
            'quiet': True,
            'http_headers': YTDLP_HEADERS,
            'progress_hooks': [progress_hook],
            'retries': 5,
            'fragment_retries': 10,
            'continuedl': True,
            'noprogress': True,
            'noplaylist': True,
        }

        download_success = False
        local_file = None
        try:
            def _download():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([direct_url])

            await loop.run_in_executor(None, _download)
            download_success = True
        except Exception as dl_err:
            logger.error(f"yt-dlp download failed: {dl_err}")
            if progress:
                await progress.update(
                    f"<b><blockquote>Dᴏᴡɴʟᴏᴀᴅ ғᴀɪʟᴇᴅ: {str(dl_err)[:100]}</blockquote></b>",
                    parse_mode='html'
                )
            return False

        # Find the actual downloaded file
        for ext in ['mkv', 'mp4', 'avi', 'webm']:
            candidate = os.path.join(str(DOWNLOAD_DIR), f"{filename_base}.{ext}")
            if os.path.exists(candidate) and os.path.getsize(candidate) > 1000:
                local_file = candidate
                break

        if not local_file:
            # Try finding by prefix
            try:
                files = sorted(
                    [os.path.join(str(DOWNLOAD_DIR), f) for f in os.listdir(str(DOWNLOAD_DIR)) if f.startswith(filename_base)],
                    key=os.path.getmtime, reverse=True
                )
                if files and os.path.getsize(files[0]) > 1000:
                    local_file = files[0]
            except Exception:
                pass

        if not local_file:
            logger.error(f"Downloaded file not found for {filename_base}")
            if progress:
                await progress.update(
                    "<b><blockquote>ᴅᴏᴡɴʟᴏᴀᴅᴇᴅ ғɪʟᴇ ɴᴏᴛ ғᴏᴜɴᴅ</blockquote></b>",
                    parse_mode='html'
                )
            return False

        logger.info(f"Downloaded file: {local_file}")

        # Step 5: Upload to dump channel
        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗨𝗣𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ✦</blockquote>\n"
                f"<blockquote>・ Dʀᴀᴍᴀ: {drama_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Sᴛᴀᴛᴜs: Uᴘʟᴏᴀᴅɪɴɢ ᴛᴏ ᴅᴜᴍᴘ ᴄʜᴀɴɴᴇʟ...</blockquote></b>",
                parse_mode='html'
            )

        caption = f"**[{audio_type}] {drama_title} {ep_str}**"
        thumb = await get_fixed_thumbnail()

        dump_msg_id = await robust_upload_file(
            file_path=local_file,
            caption=caption,
            thumb_path=thumb,
            max_retries=3
        )

        # Cleanup local file
        try:
            os.remove(local_file)
        except Exception:
            pass

        if not dump_msg_id:
            logger.error(f"Upload failed for {drama_title} Ep{episode_number}")
            if progress:
                await progress.update(
                    "<b><blockquote>Uᴘʟᴏᴀᴅ ғᴀɪʟᴇᴅ</blockquote></b>",
                    parse_mode='html'
                )
            return False

        logger.info(f"Uploaded successfully: msg_id={dump_msg_id}")

        # Step 6: Get TMDB drama info
        drama_info = await get_drama_info(drama_title)

        # Step 7: Post to main channel with drama info
        await post_drama_with_buttons(client, drama_title, drama_info, episode_number, audio_type, dump_msg_id)

        # Mark as processed
        mark_episode_processed(drama_title, episode_number)
        auto_download_state.last_checked = datetime.now().isoformat()

        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗖𝗢𝗠𝗣𝗟𝗘𝗧𝗘 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Dʀᴀᴍᴀ: {drama_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Aᴜᴅɪᴏ: {audio_type}\n"
                f"・ Sᴛᴀᴛᴜs: Pᴏsᴛᴇᴅ ✓</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )

        logger.info(f"Successfully processed: {drama_title} Episode {episode_number}")
        return True

    except Exception as e:
        logger.error(f"Error in auto drama download process: {e}")
        import traceback; logger.error(traceback.format_exc())
        if progress:
            await progress.update(
                f"<b>Error in auto download process:</b> <i>{str(e)}</i>",
                parse_mode='html'
            )
        return False
    finally:
        _currently_processing = False




async def check_and_process_next_episode(progress=None):
    try:
        logger.info("Checking for other new drama episodes to process...")
        channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
        
        latest_data = await get_latest_dramas(page=1)
        if not latest_data:
            return False
        
        for idx, drama_data in enumerate(latest_data):
            if idx >= 5:
                break
                
            drama_title = drama_data.get('drama_title', drama_data.get('title', 'Unknown Drama'))
            episode_number = drama_data.get('episode', 0)
            
            if drama_queue.is_processed(drama_title, episode_number):
                continue
            
            episode_id = f"{drama_title}_{episode_number}"
            if episode_id in [item['id'] for item in drama_queue.pending_queue]:
                continue
            
            logger.info(f"Found unprocessed drama episode: {drama_title} Episode {episode_number}")
            
            if progress:
                await progress.update(
                f"<b><blockquote>✦ 𝗘𝗣𝗜𝗦𝗢𝗗𝗘 𝗖𝗛𝗘𝗖𝗞𝗜𝗡𝗚 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Aɴɪᴍᴇ: {drama_title} \n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Sᴛᴀᴛᴜs: Cʜᴇᴄᴋɪɴɢ</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )
            
            success = await process_single_episode(drama_title, episode_number, progress)
            if success:
                return True
        
        return await process_pending_queue(progress)
        
    except Exception as e:
        logger.error(f"Error checking next episode: {e}")
        return False


async def process_pending_queue(progress=None):
    try:
        channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
        pending_item = drama_queue.get_next_pending()
        if not pending_item:
            logger.info("No items in pending queue")
            return False
        
        logger.info(f"Processing from queue: {pending_item['id']}")
        
        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗤𝗨𝗘𝗨𝗘 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Dʀᴀᴍᴀ: {pending_item['title']}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {pending_item['episode']}\n"
                f"・ Qᴜᴇᴜᴇ sɪᴢᴇ: {len(drama_queue.pending_queue)}\n"
                f"・ Sᴛᴀᴛᴜs: Pʀᴏᴄᴇssɪɴɢ</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )
        
        success = await process_single_episode(
            pending_item['title'],
            pending_item['episode'],
            progress,
            from_queue=True
        )
        
        if success:
            drama_queue.remove_from_pending(pending_item['id'])
            logger.info(f"Successfully processed from queue: {pending_item['id']}")
        else:
            pending_item['last_checked'] = datetime.now().isoformat()
            drama_queue.save_queue()
        
        return success
        
    except Exception as e:
        logger.error(f"Error processing pending queue: {e}")
        return False


async def process_single_episode(drama_title, episode_number, progress=None, from_queue=False):
    global _currently_processing
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    try:
        if _currently_processing:
            logger.info("Already processing an episode. Adding to queue.")
            return False
        
        _currently_processing = True

        # Search for the drama on kdramamaza
        search_results = await search_drama(drama_title)
        if not search_results:
            logger.error(f"Drama not found: {drama_title}")
            return False
        
        drama_info = search_results[0]
        drama_url = drama_info.get('url', '')
        
        # Get episode list
        episodes = await get_episode_list(drama_url)
        if not episodes:
            logger.error(f"Failed to get episode list for {drama_title}")
            return False
        
        target_episode = None
        for ep in episodes:
            try:
                if int(ep.get('episode', 0)) == episode_number:
                    target_episode = ep
                    break
            except (ValueError, TypeError):
                continue
        
        if not target_episode:
            logger.error(f"Episode {episode_number} not found for {drama_title}")
            return False
        
        episode_url = target_episode.get('url', '')
        audio_type = detect_audio_type(target_episode.get('title', ''))

        # Get HubCloud download links
        download_links = await get_episode_download_links(episode_url)
        if not download_links:
            logger.error(f"No download links found for {drama_title} Episode {episode_number}")
            if not from_queue:
                queue_info = {
                    'title': drama_title,
                    'episode': episode_number,
                    'url': drama_url,
                    'episode_url': episode_url,
                    'audio_type': audio_type
                }
                drama_queue.add_to_pending(queue_info)
            return False
        
        drama_queue.mark_as_processed(drama_title, episode_number)
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing episode: {e}")
        return False
    finally:
        _currently_processing = False


async def check_for_new_episodes(client):
    global _currently_processing
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    progress = None
    
    if not auto_download_state.enabled:
        return
    
    scheduler_lock = _get_scheduler_lock()
    if scheduler_lock:
        if scheduler_lock.locked():
            logger.info("Scheduler lock held by another task. Skipping this check.")
            return
        
        acquired = scheduler_lock.locked() == False
        if not acquired:
            logger.info("Could not acquire scheduler lock. Skipping this check.")
            return
    
    if _currently_processing:
        logger.info("Already processing an episode. Skipping auto check.")
        return
    
    async with scheduler_lock if scheduler_lock else asyncio.Lock():
        logger.info("Checking for new episodes and pending queue...")
        
        if drama_queue.pending_queue:
            logger.info(f"Processing {len(drama_queue.pending_queue)} pending dramas first...")
            await process_pending_queue()
        
        try:
            if auto_download_state.last_checked:
                last_check = datetime.fromisoformat(auto_download_state.last_checked)
                time_since_last_check = (datetime.now() - last_check).total_seconds()
                
                cooldown_period = auto_download_state.interval / 2
                if time_since_last_check < cooldown_period:
                    logger.info(f"Skipping auto check, last check was {time_since_last_check:.1f} seconds ago")
                    return
            
            latest_data = await get_latest_dramas(page=1)
            if not latest_data:
                logger.error("Failed to get latest dramas")
                return
            
            unprocessed_dramas = []
            for drama_data in latest_data:
                drama_title = drama_data.get('drama_title', drama_data.get('title', 'Unknown Drama'))
                episode_number = drama_data.get('episode', 0)
                
                if is_episode_processed(drama_title, episode_number):
                    logger.debug(f"Skipping {drama_title} Ep{episode_number}: already processed")
                    continue
                
                if episode_tracker.is_posted(drama_title, episode_number):
                    logger.debug(f"Skipping {drama_title} Ep{episode_number}: already POSTED")
                    continue
                
                if episode_tracker.is_processing(drama_title, episode_number):
                    logger.debug(f"Skipping {drama_title} Ep{episode_number}: currently PROCESSING")
                    continue
                
                unprocessed_dramas.append(drama_data)
                logger.info(f"Found unprocessed: {drama_title} Episode {episode_number}")
            
            if not unprocessed_dramas:
                logger.info("No new unprocessed dramas found.")
                auto_download_state.last_checked = datetime.now().isoformat()
                return
            
            logger.info(f"Found {len(unprocessed_dramas)} unprocessed dramas to process sequentially")
            
            if ADMIN_CHAT_ID:
                progress = ProgressMessage(client, ADMIN_CHAT_ID, f"<b><blockquote>ғᴏᴜɴᴅ {len(unprocessed_dramas)} ɴᴇᴡ ᴅʀᴀᴍᴀs ᴛᴏ ᴘʀᴏᴄᴇss...</blockquote></b>", parse_mode='html')
                await progress.send()
            
            processed_count = 0
            failed_count = 0
            skipped_count = 0
            
            for idx, drama_data in enumerate(unprocessed_dramas):
                drama_title = drama_data.get('drama_title', drama_data.get('title', 'Unknown Drama'))
                episode_number = drama_data.get('episode', 0)
                
                if not episode_tracker.try_start_processing(drama_title, episode_number):
                    logger.info(f"Skipping {drama_title} Ep{episode_number}: could not acquire processing lock")
                    skipped_count += 1
                    continue
                
                logger.info(f"Processing drama {idx + 1}/{len(unprocessed_dramas)}: {drama_title} Episode {episode_number}")
                
                if progress:
                    await progress.update(
                        f"<b><blockquote>✦ 𝗣𝗥𝗢𝗖𝗘𝗦𝗦𝗜𝗡𝗚 ✦</blockquote>\n"
                        f"──────────────────\n"
                        f"<blockquote>・ Dʀᴀᴍᴀ: {drama_title}\n"
                        f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                        f"・ Pʀᴏɢʀᴇss: {idx + 1}/{len(unprocessed_dramas)}\n"
                        f"・ Sᴛᴀᴛᴜs: Pʀᴏᴄᴇssɪɴɢ</blockquote>\n"
                        f"──────────────────\n"
                        f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                        parse_mode='html'
                    )
                
                try:
                    success = await process_specific_drama(drama_data, progress)
                    
                    if success:
                        processed_count += 1
                        logger.info(f"Successfully processed: {drama_title} Episode {episode_number}")
                    else:
                        failed_count += 1
                        logger.warning(f"Failed to process: {drama_title} Episode {episode_number}")
                        episode_tracker.release_processing(drama_title, episode_number, success=False)
                        
                except Exception as e:
                    failed_count += 1
                    logger.error(f"Error processing {drama_title} Episode {episode_number}: {e}")
                    episode_tracker.release_processing(drama_title, episode_number, success=False)
                    continue
            
            auto_download_state.last_checked = datetime.now().isoformat()
            
            if progress:
                await progress.update(
                    f"<b><blockquote>✦ 𝗖𝗢𝗠𝗣𝗟𝗘𝗧𝗘𝗗 ✦</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>・ Pʀᴏᴄᴇssᴇᴅ: {processed_count}\n"
                    f"・ Fᴀɪʟᴇᴅ: {failed_count}\n"
                    f"・ Sᴋɪᴘᴘᴇᴅ: {skipped_count}\n"
                    f"・ Tᴏᴛᴀʟ: {len(unprocessed_anime)}</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                    parse_mode='html'
                )
            
            logger.info(f"Batch processing complete: {processed_count} processed, {failed_count} failed, {skipped_count} skipped")
            
        except Exception as e:
            logger.error(f"Error checking for new episodes: {str(e)}")
            if progress:
                await progress.update(
                    f"<b><blockquote>ᴇʀʀᴏʀ ᴘʀᴏᴄᴇssɪɴɢ ᴀɴɪᴍᴇ:</b> {str(e)}</blockquote>",
                    parse_mode='html'
                )


async def process_specific_drama(drama_data: dict, progress=None) -> bool:
    """Process a single drama episode: scrape HubCloud link → download → upload → post."""
    global _currently_processing
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    
    if _currently_processing:
        logger.info("Already processing, skipping this drama for now")
        return False
    
    _currently_processing = True
    
    drama_title = drama_data.get('drama_title', drama_data.get('title', 'Unknown Drama'))
    episode_number = drama_data.get('episode', 0)
    episode_url = drama_data.get('url', '')
    audio_type = drama_data.get('audio_type', detect_audio_type(drama_data.get('title', '')))
    
    files_to_cleanup = []
    
    try:
        logger.info(f"Processing drama: {drama_title} Episode {episode_number}")
        
        # Get HubCloud links from episode page
        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗚𝗘𝗧𝗧𝗜𝗡𝗚 𝗟𝗜𝗡𝗜𝗖 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Dʀᴀᴍᴀ: {drama_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Sᴛᴀᴛᴜs: Fᴇᴛᴄʜɪɴɢ lɪɴᴏ...</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )
        
        download_links = await get_episode_download_links(episode_url)
        if not download_links:
            logger.error(f"No download links for {drama_title} Ep{episode_number}")
            return False
        
        # Use first available HubCloud link
        hubcloud_url = download_links[0] if isinstance(download_links[0], str) else download_links[0].get('url', '')
        
        # Bypass HubCloud to get direct download URL
        direct_url = await bypass_hubcloud(hubcloud_url)
        if not direct_url:
            logger.error(f"Failed to bypass HubCloud for {drama_title} Ep{episode_number}")
            return False
        
        # Build filename
        base_name = format_filename(drama_title, episode_number, audio_type)
        main_channel_username = CHANNEL_USERNAME if CHANNEL_USERNAME else BOT_USERNAME
        full_caption = f"**{base_name} {main_channel_username}.mkv**"
        filename = sanitize_filename(full_caption)
        download_path = os.path.join(DOWNLOAD_DIR, filename)
        
        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗗𝗢𝗘𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Dʀᴀᴍᴀ: {drama_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Aᴜᴅɪᴏ: {audio_type}\n"
                f"・ Sᴛᴀᴛᴜs: Dᴏᴡɴʟᴏᴀᴅɪɴɢ...</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )
        
        # Download with yt-dlp
        ydl_opts = {
            'outtmpl': download_path,
            'quiet': True,
            'no_warnings': True,
            'http_headers': YTDLP_HEADERS,
            'nocheckcertificate': True,
            'retries': 5,
            'fragment_retries': 10,
        }
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([direct_url])
        except Exception as dl_error:
            logger.error(f"Download error for {drama_title}: {dl_error}")
            return False
        
        if not os.path.exists(download_path):
            logger.error(f"Downloaded file does not exist for {drama_title}")
            return False
        
        file_size = os.path.getsize(download_path)
        if file_size < 1000:
            logger.error(f"Downloaded file too small: {file_size} bytes")
            try:
                os.remove(download_path)
            except:
                pass
            return False
        
        files_to_cleanup.append(download_path)
        logger.info(f"Download SUCCESS: {format_size(file_size)}")
        
        # Optional ffmpeg rename
        if FFMPEG_AVAILABLE:
            final_path = os.path.join(DOWNLOAD_DIR, f"[E{episode_number:02d}] - {drama_title} [{audio_type}].mkv")
            if await rename_video_with_ffmpeg(download_path, final_path):
                try:
                    os.remove(download_path)
                except:
                    pass
                download_path = final_path
                files_to_cleanup = [download_path]
        
        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗨𝗣𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Dʀᴀᴍᴀ: {drama_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Sɪᴢᴇ: {format_size(file_size)}\n"
                f"・ Sᴛᴀᴛᴜs: Uᴘʟᴏᴀᴅɪɴɢ...</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )
        
        # Upload to dump channel
        thumb = await get_fixed_thumbnail()
        dump_msg_id = await robust_upload_file(
            file_path=download_path,
            caption=full_caption,
            thumb_path=thumb,
            max_retries=3
        )
        
        if not dump_msg_id:
            logger.error(f"Upload failed for {drama_title} Ep{episode_number}")
            return False
        
        mark_episode_processed(drama_title, episode_number)
        episode_tracker.mark_completed(drama_title, episode_number)
        
        # Clean up local file
        for f_path in files_to_cleanup:
            try:
                if os.path.exists(f_path):
                    os.remove(f_path)
            except Exception as cleanup_err:
                logger.warning(f"Could not cleanup file {f_path}: {cleanup_err}")
        
        # Get TMDB info and post to main channel
        tmdb_info = await get_drama_info(drama_title)
        poster_path = None
        if tmdb_info:
            poster_path = await download_drama_poster(tmdb_info)
        
        await post_drama_with_buttons(
            client,
            drama_title,
            tmdb_info,
            episode_number,
            audio_type,
            dump_msg_id
        )
        
        if poster_path and os.path.exists(poster_path):
            try:
                os.remove(poster_path)
            except:
                pass
        
        episode_tracker.mark_posted(drama_title, episode_number)
        
        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗖𝗢𝗠𝗣𝗟𝗘𝗧𝗘 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Dʀᴀᴍᴀ: {drama_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Aᴜᴅɪᴏ: {audio_type}\n"
                f"・ Sᴛᴀᴛᴜs: Pᴏᴀᴛᴇᴅ ✓</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )
        
        logger.info(f"=== SUCCESS: {drama_title} Ep{episode_number} fully processed ===")
        return True
            
    except Exception as e:
        logger.error(f"Error in process_specific_drama: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        _currently_processing = False
    


async def process_latest_drama(client):
    """Process the latest drama episode from kdramamaza.net."""
    global _currently_processing
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    if _currently_processing:
        logger.info("Already processing an episode. Skipping.")
        return
    
    logger.info("Processing latest drama episode...")
    
    try:
        latest_data = await get_latest_dramas(page=1)
        if not latest_data:
            logger.error("Failed to get latest dramas")
            return

        latest_drama = latest_data[0]
        drama_title = latest_drama.get('drama_title', latest_drama.get('title', 'Unknown Drama'))
        episode_number = latest_drama.get('episode', 0)

        if is_episode_processed(drama_title, episode_number):
            logger.info(f"Episode {episode_number} of {drama_title} already processed. Skipping.")
            return
        
        progress = None
        if ADMIN_CHAT_ID:
            progress = ProgressMessage(client, ADMIN_CHAT_ID, 
                f"<b><blockquote>ᴘʀᴏᴄᴇssɪɴɢ ʟᴀᴛᴇsᴛ ᴅʀᴀᴍᴀ ᴇᴘɪsᴏᴅᴇ...</blockquote></b>", 
                parse_mode='html'
            )
            if not await progress.send():
                logger.error("Failed to send progress message")
                return
            
            await progress.update(
                f"<b><blockquote>✦ 𝗣𝗥𝗢𝗖𝗘𝗦𝗦𝗜𝗡𝗚 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Dʀᴀᴍᴀ: {drama_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Sᴛᴀᴛᴜs: Pʀᴏᴄᴇssɪɴɢ</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )
        
        success = await process_specific_drama(latest_drama, progress)
        
        if success:
            logger.info(f"Successfully processed latest drama: {drama_title} Episode {episode_number}")
            if progress:
                await progress.update(
                    f"<b><blockquote>sᴜᴄᴄᴇssғᴜʟʟʏ ᴘʀᴏᴄᴇssᴇᴅ: ᴅʀᴀᴍᴀ {drama_title} | ᴇᴘɪsᴏᴅᴇ {episode_number}</blockquote></b>",
                    parse_mode='html'
                )
        else:
            logger.error(f"Failed to process latest drama: {drama_title}")
            if progress:
                await progress.update(
                    f"<b><blockquote>ғᴀɪʟᴇᴅ ᴛᴏ ᴘʀᴏᴄᴇss: ᴅʀᴀᴍᴀ {drama_title} | ᴇᴘɪsᴏᴅᴇ {episode_number}</blockquote></b>",
                    parse_mode='html'
                )
    except Exception as e:
        logger.error(f"Error processing latest drama: {str(e)}")
        pass  # error already logged above
async def process_daily_requests(client):
    global _currently_processing
    
    from core.database import (
        get_all_pending_requests, mark_request_processed, 
        get_processed_request_results, add_processed_request_result
    )
    from core.download import post_drama_batch_with_buttons
    
    logger.info("Processing daily requests...")
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    
    _currently_processing = True
    logger.info("Request processing started - auto-processing PAUSED")
    
    try:
        pending_requests = await get_all_pending_requests()
        
        if not pending_requests:
            logger.info("No pending requests to process")
            return
        
        logger.info(f"Found {len(pending_requests)} pending requests to process")
        
        for idx, request in enumerate(pending_requests, 1):
            try:
                request_text = request.get('text')
                request_id = request.get('_id')
                user_id = request.get('user_id')
                
                logger.info(f"Processing request {idx}/{len(pending_requests)}: {request_text}")
                
                if ADMIN_CHAT_ID:
                    progress = ProgressMessage(client, ADMIN_CHAT_ID, 
                        f"<b><blockquote>ᴘʀᴏᴄᴇssɪɴɢ ʀᴇǫᴜᴇsᴛ ({idx}/{len(pending_requests)})...</b></blockquote>",
                        parse_mode='html'
                    )
                    if not await progress.send():
                        logger.error("Failed to send progress message")
                        continue
                else:
                    progress = None
                
                search_results = await search_drama(request_text)
                
                if not search_results:
                    logger.warning(f"No results found for request: {request_text}")
                    if progress:
                        await progress.update(
                            f"<b><blockquote>ɴᴏ ʀᴇsᴜʟᴛs ғᴏᴜɴᴅ ғᴏʀ: {request_text}</b></blockquote>",
                            parse_mode='html'
                        )
                    mark_request_processed(request_id)
                    continue
                
                processed_results = await get_processed_request_results(request_text)
                logger.info(f"Previously processed results for '{request_text}': {processed_results}")
                
                remaining_results = []
                for result in search_results:
                    drama_title = result.get('title', result.get('drama_title', result.get('drama_title', 'Unknown')))
                    if drama_title not in processed_results:
                        remaining_results.append(result)
                
                if not remaining_results:
                    logger.info(f"All search results for '{request_text}' have been processed")
                    if progress:
                        await progress.update(
                            f"<b><blockquote>ᴀʟʟ ʀᴇsᴜʟᴛs ғᴏʀ '{request_text}' ʜᴀᴠᴇ ʙᴇᴇɴ ᴘʀᴏᴄᴇssᴇᴅ</b></blockquote>",
                            parse_mode='html'
                        )
                    mark_request_processed(request_id)
                    continue
                
                if progress:
                    await progress.update(
                        f"<b><blockquote>ғᴏᴜɴᴅ {len(remaining_results)} ɴᴇᴡ ʀᴇsᴜʟᴛs ғᴏʀ: {request_text}\n"
                        f"ᴘʀᴏᴄᴇssɪɴɢ...</b></blockquote>",
                        parse_mode='html'
                    )
                
                processed_any = False
                for result_idx, anime_result in enumerate(remaining_results[:1], 1):
                    try:
                        drama_title = anime_result.get('title', anime_result.get('drama_title', anime_result.get('drama_title', 'Unknown')))
                        drama_url = anime_result.get('url', '')
                        
                        if progress:
                            await progress.update(
                                f"<b><blockquote>ᴘʀᴏᴄᴇssɪɴɢ:</b>\n"
                                f"{drama_title}</blockquote>",
                                parse_mode='html'
                            )
                        
                        logger.info(f"Processing result: {drama_title}")
                        
                        episodes = await get_episode_list(drama_url) if drama_url else []
                        if not episodes:
                            logger.warning(f"No episodes found for {drama_title}")
                            continue
                        
                        total_episodes = len(episodes)
                        logger.info(f"Found {total_episodes} episodes for {drama_title}")
                        
                        drama_info = await get_drama_info(drama_title)
                        
                        audio_type = detect_audio_type(drama_title)
                        thumb = await get_fixed_thumbnail()
                        uploaded_msg_ids = []
                        
                        for ep_idx, episode in enumerate(episodes):
                            episode_number = int(episode.get('episode', ep_idx + 1))
                            episode_url = episode.get('url', episode.get('link', ''))
                            
                            try:
                                if progress:
                                    await progress.update(
                                        f"<b><blockquote>✦ 𝗥𝗘𝗤𝗨𝗘𝗦𝗧 𝗣𝗥𝗢𝗖𝗘𝗦𝗦𝗜𝗡𝗚 ✦</blockquote>\n"
                                        f"──────────────────\n"
                                        f"<blockquote>・ Dʀᴀᴍᴀ: {drama_title}\n"
                                        f"・ Eᴘɪsᴏᴅᴇ: {episode_number} ({ep_idx+1}/{total_episodes})\n"
                                        f"・ Sᴛᴀᴛᴜs: Fᴇᴛᴄʜɪɴɢ ʟɪɴᴋ...</blockquote>\n"
                                        f"──────────────────\n"
                                        f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                                        parse_mode='html'
                                    )
                                
                                # Get HubCloud links for the episode
                                download_links = await get_episode_download_links(episode_url) if episode_url else []
                                if not download_links:
                                    logger.warning(f"No download links for {drama_title} Episode {episode_number}")
                                    continue
                                
                                hubcloud_url = download_links[0] if isinstance(download_links[0], str) else download_links[0].get('url', '')
                                
                                # Bypass HubCloud
                                direct_link = await bypass_hubcloud(hubcloud_url)
                                if not direct_link:
                                    logger.warning(f"HubCloud bypass failed for {drama_title} Episode {episode_number}")
                                    continue
                                
                                # Build filename
                                base_name = format_filename(drama_title, episode_number, audio_type)
                                main_channel_username = CHANNEL_USERNAME if CHANNEL_USERNAME else BOT_USERNAME
                                full_caption = f"**{base_name} {main_channel_username}.mkv**"
                                filename = sanitize_filename(f"{base_name}.mkv")
                                download_path = os.path.join(DOWNLOAD_DIR, filename)
                                
                                if progress:
                                    await progress.update(
                                        f"<b><blockquote>✦ 𝗥𝗘𝗤𝗨𝗘𝗦𝗧 𝗣𝗥𝗢𝗖𝗘𝗦𝗦𝗜𝗡𝗚 ✦</blockquote>\n"
                                        f"──────────────────\n"
                                        f"<blockquote>・ Dʀᴀᴍᴀ: {drama_title}\n"
                                        f"・ Eᴘɪsᴏᴅᴇ: {episode_number} ({ep_idx+1}/{total_episodes})\n"
                                        f"・ Sᴛᴀᴛᴜs: Dᴏᴡɴʟᴏᴀᴅɪɴɢ...</blockquote>\n"
                                        f"──────────────────\n"
                                        f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                                        parse_mode='html'
                                    )
                                
                                ydl_opts = {
                                    'outtmpl': download_path,
                                    'quiet': True,
                                    'no_warnings': True,
                                    'http_headers': YTDLP_HEADERS,
                                    'nocheckcertificate': True,
                                }
                                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                                    ydl.download([direct_link])
                                
                                if not os.path.exists(download_path) or os.path.getsize(download_path) < 1000:
                                    logger.error(f"Downloaded file too small: {download_path}")
                                    continue
                                
                                dump_msg_id = await robust_upload_file(
                                    file_path=download_path,
                                    caption=full_caption,
                                    thumb_path=thumb,
                                    max_retries=3
                                )
                                
                                if dump_msg_id:
                                    uploaded_msg_ids.append(dump_msg_id)
                                    logger.info(f"Uploaded Episode {episode_number} - msg_id: {dump_msg_id}")
                                else:
                                    logger.error(f"Upload FAILED: Episode {episode_number} [{drama_title}]")
                                
                                try:
                                    os.remove(download_path)
                                except:
                                    pass
                            
                            except Exception as e:
                                logger.error(f"Error processing Episode {episode_number} [{drama_title}]: {e}")
                                import traceback
                                logger.error(traceback.format_exc())
                        
                        if uploaded_msg_ids and drama_info:
                            logger.info(f"Creating final channel post for {drama_title}")
                            logger.info(f"Uploaded episodes: {len(uploaded_msg_ids)}")
                            
                            if progress:
                                await progress.update(
                                    f"<b><blockquote>✦ 𝗙𝗜𝗡𝗔𝗟𝗜𝗭𝗜𝗡𝗚 ✦</blockquote>\n"
                                    f"──────────────────\n"
                                    f"<blockquote>・ Dʀᴀᴍᴀ: {drama_title}\n"
                                    f"・ Eᴘɪsᴏᴅᴇs: {total_episodes}\n"
                                    f"・ Sᴛᴀᴛᴜs: Cʀᴇᴀᴛɪɴɢ ғɪɴᴀʟ ᴘᴏsᴛ...</blockquote>\n"
                                    f"──────────────────\n"
                                    f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                                    parse_mode='html'
                                )
                            
                            await post_drama_batch_with_buttons(
                                client, drama_title, drama_info, {}, total_episodes, audio_type
                            )
                            
                            await add_processed_request_result(request_text, drama_title)
                            processed_any = True
                            logger.info(f"Successfully processed ALL {total_episodes} episodes of '{drama_title}'")
                        else:
                            logger.warning(f"No files uploaded for {drama_title}")
                        
                    except Exception as e:
                        logger.error(f"Error processing result: {e}")
                        import traceback
                        logger.error(traceback.format_exc())
                
                if processed_any:
                    mark_request_processed(request_id)
                    
                    if progress:
                        await progress.update(
                            f"<b><blockquote>✦ ᴄᴏᴍᴘʟᴇᴛᴇᴅ ✦</blockquote>\n"
                            f"──────────────────\n"
                            f"<blockquote>・ Rᴇǫᴜᴇsᴛ: {request_text}\n"
                            f"・ Sᴛᴀᴛᴜs: ᴄᴏᴍᴘʟᴇᴛᴇᴅ</blockquote>\n"
                            f"──────────────────\n"
                            f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                            parse_mode='html'
                        )
                else:
                    logger.warning(f"Failed to process any results for request '{request_text}'")
                    if progress:
                        await progress.update(
                            f"<b><blockquote>ғᴀɪʟᴇᴅ ᴛᴏ ᴘʀᴏᴄᴇss ʀᴇǫᴜᴇsᴛ: {request_text}</b></blockquote>",
                            parse_mode='html'
                        )
                
            except Exception as e:
                logger.error(f"Error processing request {idx}: {e}")
                import traceback
                logger.error(traceback.format_exc())
        
        logger.info("Daily request processing completed")
        
    except Exception as e:
        logger.error(f"Error in process_daily_requests: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        _currently_processing = False
        logger.info("Request processing finished - auto-processing RESUMED")


# IST Timezone (UTC+5:30)
IST = ZoneInfo("Asia/Kolkata")
UTC = ZoneInfo("UTC")


def convert_ist_to_utc(ist_time_str: str) -> str:
    try:
        hour, minute = map(int, ist_time_str.split(':'))
        
        today = datetime.now(IST).date()
        ist_datetime = datetime(today.year, today.month, today.day, hour, minute, tzinfo=IST)
        
        utc_datetime = ist_datetime.astimezone(UTC)
        
        return utc_datetime.strftime('%H:%M')
    except Exception as e:
        logger.error(f"Error converting IST to UTC: {e}")
        return ist_time_str


def get_current_ist_time() -> str:
    return datetime.now(IST).strftime('%H:%M')


_request_time_job_tag = "daily_request_processing"


def setup_scheduler(client):
    def schedule_check():
        asyncio.create_task(check_for_new_episodes(client))
    
    def schedule_queue_check():
        asyncio.create_task(process_pending_queue())
    
    def schedule_daily_requests():
        asyncio.create_task(process_daily_requests(client))
        logger.info(f"Triggered daily request processing at {get_current_utc_time()} UTC / {get_current_ist_time()} IST")
    
    async def setup_daily_request_scheduler():
        from core.database import get_request_process_time
        
        try:
            ist_time_str = await get_request_process_time()
            
            if ist_time_str and ist_time_str != "00:00":
                utc_time_str = convert_ist_to_utc(ist_time_str)
                
                schedule.clear(_request_time_job_tag)
                
                schedule.every().day.at(utc_time_str).do(schedule_daily_requests).tag(_request_time_job_tag)
                
                logger.info(f"𝗗𝗮𝗶𝗹𝘆 𝗿𝗲𝗾𝘂𝗲𝘀𝘁 𝗽𝗿𝗼𝗰𝗲𝘀𝘀𝗶𝗻𝗴 𝘀𝗰𝗵𝗲𝗱𝘂𝗹𝗲𝗱 𝗮𝘁 {ist_time_str} IST ({utc_time_str} UTC)")
            else:
                logger.info("No daily request processing time configured")
        except Exception as e:
            logger.error(f"Error setting up daily request scheduler: {e}")
    
    def reschedule():
        for job in schedule.get_jobs():
            if _request_time_job_tag not in job.tags:
                schedule.cancel_job(job)
        
        interval = auto_download_state.interval
        schedule.every(interval).seconds.do(schedule_check)
        logger.info(f"𝙎𝙩𝙖𝙧𝙩𝙞𝙣𝙜 𝙎𝙘𝙝𝙚𝙙𝙪𝙡𝙚𝙧")
    
    reschedule()
    
    asyncio.create_task(setup_daily_request_scheduler())
    
    orig_setter = auto_download_state.__class__.interval.fset
    def interval_setter(self, seconds):
        orig_setter(self, seconds)
        reschedule()
    
    auto_download_state.__class__.interval = auto_download_state.__class__.interval.setter(interval_setter)
    
    async def scheduler_loop():
        while True:
            schedule.run_pending()
            await asyncio.sleep(1)
    
    asyncio.create_task(scheduler_loop())


async def reschedule_daily_requests(ist_time_str: str):
    try:
        utc_time_str = convert_ist_to_utc(ist_time_str)
        
        schedule.clear(_request_time_job_tag)
        
        def schedule_daily_requests_job():
            from core.client import client
            asyncio.create_task(process_daily_requests(client))
            logger.info(f"Triggered daily request processing at {get_current_utc_time()} UTC / {get_current_ist_time()} IST")
        
        schedule.every().day.at(utc_time_str).do(schedule_daily_requests_job).tag(_request_time_job_tag)
        
        logger.info(f"𝗥𝗲𝘀𝗰𝗵𝗲𝗱𝘂𝗹𝗲𝗱 𝗱𝗮𝗶𝗹𝘆 𝗿𝗲𝗾𝘂𝗲𝘀𝘁 𝗽𝗿𝗼𝗰𝗲𝘀𝘀𝗶𝗻𝗴 𝘁𝗼 {ist_time_str} IST ({utc_time_str} UTC)")
        return True
    except Exception as e:
        logger.error(f"Error rescheduling daily requests: {e}")
        return False
