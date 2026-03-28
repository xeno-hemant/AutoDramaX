from __future__ import annotations
import re
import os
import time
import random
import logging
import asyncio
import xml.etree.ElementTree as ET
from typing import Optional, List, Dict, Any
from urllib.parse import quote, urljoin

import aiohttp
import requests
import cloudscraper
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

KDRAMAMAZA_BASE = "https://kdramamaza.net"
KDRAMAMAZA_FEED = "https://kdramamaza.net/feed/"
TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500"

DRAMA_REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def detect_audio_type(text: str) -> str:
    """Detect audio type from title or text."""
    text_lower = text.lower()
    if any(kw in text_lower for kw in ['hindi dubbed', 'hindi dub', 'urdu dubbed', 'urdu dub']):
        return "Hindi Dubbed"
    if any(kw in text_lower for kw in ['eng sub', 'english sub', 'subbed', 'eng dub']):
        return "Eng Subbed"
    return "Hindi Dubbed"  # default


def extract_episode_number(text: str) -> Optional[int]:
    """Extract episode number from a title string."""
    patterns = [
        r'episode[s]?\s*(\d+)',
        r'\bep\.?\s*(\d+)',
        r'\beps\.?\s*(\d+)',
        r'(?<!\d)(\d{1,3})(?!\d)(?=\s*(?:hindi|eng|sub|dub|added|$))',
    ]
    text_lower = text.lower()
    for pattern in patterns:
        match = re.search(pattern, text_lower)
        if match:
            return int(match.group(1))
    return None


def extract_drama_title(raw_title: str) -> str:
    """Strip episode number and audio type suffixes from a raw post title."""
    title = raw_title
    # Remove common suffixes
    title = re.sub(r'\s*[\-–|]\s*episode[s]?\s*\d+.*$', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s*episode[s]?\s*\d+.*$', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s*\bep\.?\s*\d+.*$', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s*(hindi dubbed|eng subbed|english subbed|subbed|dubbed|urdu dubbed|added).*$', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s*(korean drama|chinese drama|thai drama|japanese drama|in urdu|in hindi).*$', '', title, flags=re.IGNORECASE)
    return title.strip(' -–|')


# ─────────────────────────────────────────────
# kdramamaza.net RSS Feed
# ─────────────────────────────────────────────

async def get_latest_dramas(page: int = 1) -> List[Dict[str, Any]]:
    """Fetch latest drama episodes from kdramamaza.net RSS feed."""
    try:
        feed_url = KDRAMAMAZA_FEED
        if page > 1:
            feed_url = f"{KDRAMAMAZA_FEED}?paged={page}"

        async with aiohttp.ClientSession() as session:
            async with session.get(
                feed_url,
                headers=DRAMA_REQUEST_HEADERS,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                response.raise_for_status()
                content = await response.text()

        root = ET.fromstring(content)
        channel = root.find('channel')
        if not channel:
            logger.error("RSS feed has no channel element")
            return []

        items = []
        for item in channel.findall('item'):
            title_el = item.find('title')
            link_el = item.find('link')
            pub_date_el = item.find('pubDate')

            if title_el is None or link_el is None:
                continue

            raw_title = (title_el.text or "").strip()
            # link is sometimes a text node after the element
            link = (link_el.text or "").strip()
            if not link:
                # CDATA or next sibling text
                link = link_el.tail or ""
                link = link.strip()

            pub_date = (pub_date_el.text or "") if pub_date_el is not None else ""

            drama_title = extract_drama_title(raw_title)
            episode_number = extract_episode_number(raw_title)
            audio_type = detect_audio_type(raw_title)

            if not drama_title:
                continue

            items.append({
                'drama_title': drama_title,
                'raw_title': raw_title,
                'episode': episode_number or 1,
                'audio_type': audio_type,
                'url': link,
                'pub_date': pub_date,
            })

        logger.info(f"Fetched {len(items)} dramas from RSS feed (page {page})")
        return items

    except ET.ParseError as e:
        logger.error(f"RSS XML parse error: {e}")
        return []
    except Exception as e:
        logger.error(f"Error fetching latest dramas from RSS: {e}")
        return []


# ─────────────────────────────────────────────
# kdramamaza.net Search
# ─────────────────────────────────────────────

async def search_drama(query: str) -> List[Dict[str, Any]]:
    """Search for a drama on kdramamaza.net (WordPress search)."""
    try:
        search_url = f"{KDRAMAMAZA_BASE}/?s={quote(query)}"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                search_url,
                headers=DRAMA_REQUEST_HEADERS,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                response.raise_for_status()
                content = await response.text()

        soup = BeautifulSoup(content, 'html.parser')
        results = []

        # WordPress standard: articles in main content
        for article in soup.find_all('article'):
            # Try to get title
            title_el = (
                article.find(class_=re.compile(r'(entry|post)-?title', re.I))
                or article.find(['h1', 'h2', 'h3'])
            )
            link_el = article.find('a', href=True)

            if not title_el or not link_el:
                continue

            title = title_el.get_text(strip=True)
            link = link_el.get('href', '')

            results.append({
                'drama_title': extract_drama_title(title),
                'raw_title': title,
                'url': link,
                'slug': link.rstrip('/').split('/')[-1],
            })

        logger.info(f"Found {len(results)} results for query: {query}")
        return results
    except Exception as e:
        logger.error(f"Error searching drama '{query}': {e}")
        return []


# ─────────────────────────────────────────────
# Episode List from Drama Page
# ─────────────────────────────────────────────

async def get_episode_list(drama_url: str) -> List[Dict[str, Any]]:
    """Scrape episode list from a drama's WordPress page."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                drama_url,
                headers=DRAMA_REQUEST_HEADERS,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                response.raise_for_status()
                content = await response.text()

        soup = BeautifulSoup(content, 'html.parser')
        episodes = {}

        for link_tag in soup.find_all('a', href=True):
            href = link_tag.get('href', '')
            text = link_tag.get_text(strip=True)

            if not href or KDRAMAMAZA_BASE not in href:
                continue

            ep_num = extract_episode_number(text) or extract_episode_number(href)
            if ep_num:
                audio = detect_audio_type(text)
                if ep_num not in episodes:
                    episodes[ep_num] = {
                        'episode': ep_num,
                        'url': href,
                        'audio_type': audio,
                    }

        result = sorted(episodes.values(), key=lambda x: x['episode'])
        logger.info(f"Found {len(result)} episodes for {drama_url}")
        return result
    except Exception as e:
        logger.error(f"Error getting episode list from {drama_url}: {e}")
        return []


# ─────────────────────────────────────────────
# HubCloud Download Links from Episode Page
# ─────────────────────────────────────────────

def _get_hubcloud_links_from_soup(soup: BeautifulSoup, episode_url: str) -> List[Dict[str, Any]]:
    """Extract HubCloud links from parsed page soup."""
    links = []
    seen = set()

    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '').strip()
        if not href:
            continue
        if any(kw in href.lower() for kw in ['hubcloud', 'hub.foo', 'hub-cloud']):
            if href not in seen:
                seen.add(href)
                text = a_tag.get_text(strip=True)
                links.append({
                    'href': href,
                    'text': text or 'Download',
                    'audio_type': detect_audio_type(text + " " + episode_url),
                })

    # Check iframes
    for iframe in soup.find_all('iframe', src=True):
        src = iframe.get('src', '').strip()
        if any(kw in src.lower() for kw in ['hubcloud', 'hub.foo']):
            if src not in seen:
                seen.add(src)
                links.append({
                    'href': src,
                    'text': 'Stream',
                    'audio_type': detect_audio_type(episode_url),
                })

    return links


async def get_episode_download_links(episode_url: str) -> List[Dict[str, Any]]:
    """Get HubCloud download links from an episode page."""
    try:
        scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
        )
        time.sleep(random.uniform(1, 2))
        response = scraper.get(episode_url, headers=DRAMA_REQUEST_HEADERS, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        links = _get_hubcloud_links_from_soup(soup, episode_url)
        logger.info(f"Found {len(links)} HubCloud link(s) on {episode_url}")
        return links

    except Exception as e:
        logger.error(f"Error getting download links from {episode_url}: {e}")
        return []


# ─────────────────────────────────────────────
# HubCloud Bypass
# ─────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
def bypass_hubcloud(hubcloud_url: str) -> Optional[str]:
    """
    Bypass HubCloud (hubcloud.foo) and return a direct download/stream URL.
    Tries multiple strategies in order.
    """
    logger.info(f"Bypassing HubCloud: {hubcloud_url}")

    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False},
        interpreter='nodejs'
    )

    headers = {
        'User-Agent': DRAMA_REQUEST_HEADERS['User-Agent'],
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

    # Step 1: Fetch the HubCloud page
    time.sleep(random.uniform(1.5, 3))
    try:
        response = scraper.get(hubcloud_url, headers=headers, timeout=30, allow_redirects=True)
    except Exception as e:
        logger.error(f"HubCloud initial fetch failed: {e}")
        raise

    # Check if already redirected to a direct file
    final_url = response.url
    if final_url != hubcloud_url:
        if any(final_url.lower().endswith(ext) for ext in ['.mp4', '.mkv', '.avi', '.m3u8']):
            logger.info(f"HubCloud directly redirected to: {final_url}")
            return final_url

    soup = BeautifulSoup(response.content, 'html.parser')

    # Strategy 1: Direct file link in anchor tags
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '').strip()
        if any(href.lower().endswith(ext) for ext in ['.mp4', '.mkv', '.avi', '.m3u8']):
            logger.info(f"HubCloud Strategy 1 - direct file link: {href}")
            return href

    # Strategy 2: Find a form (POST-based download flow)
    form = soup.find('form')
    if form and form.get('method', '').lower() == 'post':
        action = form.get('action', hubcloud_url)
        if not action.startswith('http'):
            from urllib.parse import urljoin
            action = urljoin(hubcloud_url, action)

        form_data = {}
        for inp in form.find_all('input'):
            name = inp.get('name')
            value = inp.get('value', '')
            if name:
                form_data[name] = value

        try:
            post_resp = scraper.post(
                action,
                data=form_data,
                headers={**headers, 'Referer': hubcloud_url},
                timeout=30,
                allow_redirects=True
            )
            # Check direct redirect
            if post_resp.url != action:
                redirect_url = post_resp.url
                if redirect_url.startswith('http') and 'hubcloud' not in redirect_url.lower():
                    logger.info(f"HubCloud Strategy 2 - POST redirect: {redirect_url}")
                    return redirect_url

            # Parse post response
            post_soup = BeautifulSoup(post_resp.content, 'html.parser')
            for a_tag in post_soup.find_all('a', href=True):
                href = a_tag.get('href', '')
                if any(href.lower().endswith(ext) for ext in ['.mp4', '.mkv', '.m3u8']):
                    logger.info(f"HubCloud Strategy 2 - POST page link: {href}")
                    return href
        except Exception as e:
            logger.warning(f"HubCloud Strategy 2 failed: {e}")

    # Strategy 3: JavaScript source parsing
    for script in soup.find_all('script'):
        script_text = script.string or ""
        if not script_text:
            continue

        # File URL patterns
        for pattern in [
            r'["\']?(https?://[^"\']+\.(?:mp4|mkv|m3u8)(?:[^"\']*)?)["\']?',
            r'(?:file|src|url)\s*[=:]\s*["\']?(https?://[^"\']+)["\']?',
        ]:
            matches = re.findall(pattern, script_text, re.IGNORECASE)
            for url in matches:
                url = url.strip().rstrip(',;')
                if url.startswith('http') and 'hubcloud' not in url.lower():
                    logger.info(f"HubCloud Strategy 3 - JS file URL: {url}")
                    return url

        # Window redirect
        redirect_matches = re.findall(
            r'(?:location\.href|window\.location(?:\.href)?)\s*=\s*["\']([^"\']+)["\']',
            script_text
        )
        for redir_url in redirect_matches:
            if redir_url.startswith('http') and 'hubcloud' not in redir_url.lower():
                logger.info(f"HubCloud Strategy 3 - JS redirect: {redir_url}")
                return redir_url

    # Strategy 4: Follow any non-hubcloud anchor links on page
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '').strip()
        text = a_tag.get_text(strip=True).lower()
        if not href.startswith('http'):
            continue
        if 'hubcloud' in href.lower():
            continue
        if any(kw in text for kw in ['download', 'direct', 'stream', 'watch', 'play']):
            try:
                follow_resp = scraper.get(
                    href,
                    headers={**headers, 'Referer': hubcloud_url},
                    timeout=30,
                    allow_redirects=True
                )
                furl = follow_resp.url
                if any(furl.lower().endswith(ext) for ext in ['.mp4', '.mkv', '.avi', '.m3u8']):
                    logger.info(f"HubCloud Strategy 4 - followed link: {furl}")
                    return furl
                # Check page for file links
                fsoup = BeautifulSoup(follow_resp.content, 'html.parser')
                for fa in fsoup.find_all('a', href=True):
                    fhref = fa.get('href', '')
                    if any(fhref.lower().endswith(ext) for ext in ['.mp4', '.mkv', '.m3u8']):
                        logger.info(f"HubCloud Strategy 4 - followed page link: {fhref}")
                        return fhref
            except Exception as e:
                logger.warning(f"HubCloud Strategy 4 link follow failed: {e}")

    # Strategy 5: Try the /download or /dl variant of the URL
    if '/drive/' in hubcloud_url:
        drive_id = hubcloud_url.split('/drive/')[-1].strip('/')
        for alt_path in [f'/download/{drive_id}', f'/dl/{drive_id}', f'/file/{drive_id}']:
            alt_url = urljoin(hubcloud_url, alt_path)
            try:
                alt_resp = scraper.get(
                    alt_url,
                    headers={**headers, 'Referer': hubcloud_url},
                    timeout=20,
                    allow_redirects=True
                )
                if alt_resp.url != alt_url:
                    furl = alt_resp.url
                    if any(furl.lower().endswith(ext) for ext in ['.mp4', '.mkv', '.avi', '.m3u8']):
                        logger.info(f"HubCloud Strategy 5 - alt URL: {furl}")
                        return furl
                alt_soup = BeautifulSoup(alt_resp.content, 'html.parser')
                for a_tag in alt_soup.find_all('a', href=True):
                    href = a_tag.get('href', '')
                    if any(href.lower().endswith(ext) for ext in ['.mp4', '.mkv', '.m3u8']):
                        logger.info(f"HubCloud Strategy 5 - alt URL page: {href}")
                        return href
            except Exception:
                pass

    logger.error(f"All HubCloud bypass strategies failed for: {hubcloud_url}")
    return None


# ─────────────────────────────────────────────
# TMDB Drama Info
# ─────────────────────────────────────────────

async def get_drama_info(title: str, tmdb_api_key: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Fetch drama metadata from TMDB API."""
    try:
        from core.config import TMDB_API_KEY as CONFIG_API_KEY
        api_key = tmdb_api_key or CONFIG_API_KEY
    except ImportError:
        api_key = tmdb_api_key

    if not api_key:
        logger.warning("TMDB_API_KEY not configured — skipping drama info")
        return None

    try:
        search_url = f"{TMDB_BASE}/search/tv"
        params = {
            'api_key': api_key,
            'query': title,
            'language': 'en-US',
            'page': 1,
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(
                search_url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as response:
                if response.status != 200:
                    logger.warning(f"TMDB search returned {response.status} for '{title}'")
                    return None
                data = await response.json()

            results = data.get('results', [])
            if not results:
                logger.info(f"TMDB: no results for '{title}'")
                return None

            best = results[0]
            show_id = best.get('id')
            if not show_id:
                return best

            # Get full details
            detail_url = f"{TMDB_BASE}/tv/{show_id}"
            detail_params = {'api_key': api_key, 'language': 'en-US'}
            async with session.get(
                detail_url,
                params=detail_params,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as detail_resp:
                if detail_resp.status == 200:
                    return await detail_resp.json()
                return best

    except Exception as e:
        logger.error(f"Error fetching TMDB info for '{title}': {e}")
        return None


async def download_drama_poster(drama_title: str, drama_info: Dict[str, Any]) -> Optional[str]:
    """Download and cache a drama poster from TMDB."""
    try:
        from core.config import THUMBNAIL_DIR
        from core.utils import sanitize_filename
    except ImportError:
        logger.error("Cannot import THUMBNAIL_DIR or sanitize_filename")
        return None

    try:
        poster_path_tmdb = drama_info.get('poster_path')
        if not poster_path_tmdb:
            # Fallback: backdrop
            poster_path_tmdb = drama_info.get('backdrop_path')
        if not poster_path_tmdb:
            return None

        poster_url = f"{TMDB_IMAGE_BASE}{poster_path_tmdb}"
        poster_local = os.path.join(str(THUMBNAIL_DIR), f"{sanitize_filename(drama_title)}_poster.jpg")

        async with aiohttp.ClientSession() as session:
            async with session.get(
                poster_url,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status == 200:
                    with open(poster_local, 'wb') as f:
                        f.write(await response.read())
                    logger.info(f"Downloaded poster for '{drama_title}' -> {poster_local}")
                    return poster_local
                else:
                    logger.warning(f"Poster download returned {response.status} for '{drama_title}'")

    except Exception as e:
        logger.error(f"Error downloading drama poster for '{drama_title}': {e}")

    return None
