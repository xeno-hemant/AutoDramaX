import os
import re

# The "Nuclear Purge" Mapping
# This handles everything: standard text, known unicode variations, and links.
PURGE_MAP = {
    # BRANDING - BASE
    "DramaX": "DramaX",
    "Dramax": "Dramax",
    "dramax": "dramax",
    "DRAMAX": "DRAMAX",
    
    # UNICODE BRANDING (Mathematical/SmallCaps/Styled)
    "Dramax": "Dramax",
    "Dramax Community": "Dramax Community",
    "Community": "Community",
    "Drama": "Drama",
    "drama": "drama",
    "latest airing drama": "latest airing drama",
    "currently airing drama": "currently airing drama",
    
    # GENERIC TERMS
    "Drama": "Drama",
    "drama": "drama",
    "DRAMA": "DRAMA",
    "AutoDrama": "AutoDrama",
    "Auto Drama": "Auto Drama",
    "AutoDrama": "AutoDrama",
    
    # LINKS & SOURCES
    "kdramamaza.net": "kdramamaza.net",
    "https://image.tmdb.org/t/p/original/": "https://image.tmdb.org/t/p/original/",
    "tmdb.org": "tmdb.org",
    "t.me/DramaxCommunity": "t.me/DramaxCommunity",
}

# Regex for more complex patterns
REGEX_PURGE = [
    (r't\.me/[a-zA-Z0-9_]*drama[a-zA-Z0-9_]*', 't.me/DramaxCommunity'),
    (r'add_drama_channel', 'add_drama_channel'),
    (r'remove_drama_channel', 'remove_drama_channel'),
    (r'get_drama_channel', 'get_drama_channel'),
    (r'list_drama_channels', 'list_drama_channels'),
    (r'drama_queue', 'drama_queue'),
    (r'drama_id', 'drama_id'),
    (r'drama_list', 'drama_list'),
    (r'drama_index', 'drama_index'),
    (r'selected_drama', 'selected_drama'),
    (r'drama_session', 'drama_session'),
    (r'latest_drama_text', 'latest_drama_text'),
    (r'airing_drama_text', 'airing_drama_text'),
    (r'post_drama_to_dedicated_channel', 'post_drama_to_dedicated_channel'),
]

def nuclear_purge(content):
    # 1. Direct string replacements (ordered by length to avoid partial overwrites)
    sorted_keys = sorted(PURGE_MAP.keys(), key=len, reverse=True)
    for key in sorted_keys:
        content = content.replace(key, PURGE_MAP[key])
    
    # 2. Regex replacements
    for pattern, replacement in REGEX_PURGE:
        content = re.sub(pattern, replacement, content, flags=re.IGNORECASE)
        
    return content

def process_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            original = f.read()
        
        purged = nuclear_purge(original)
        
        if purged != original:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(purged)
            return True
    except Exception as e:
        print(f"FAILED: {file_path} - {e}")
    return False

def main():
    exclude = {'.git', '__pycache__', '.gemini'}
    modified = 0
    for root, dirs, files in os.walk('.'):
        dirs[:] = [d for d in dirs if d not in exclude]
        for f in files:
            if f.endswith(('.py', '.txt', '.env', '.md', 'Procfile', 'yml', 'Dockerfile')):
                if process_file(os.path.join(root, f)):
                    print(f"PURGED: {os.path.join(root, f)}")
                    modified += 1
    
    print(f"\nNuclear Purge complete. Modified {modified} files.")

if __name__ == "__main__":
    main()
