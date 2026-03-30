import os
import re

# The "Absolute Final Nuclear Purge" Mapping
# Includes all standard text, case variations, mixed cases, and specific unicode styles.
REPLACEMENTS = {
    # BRANDING - Styled Unicode (Mathematics/SmallCaps)
    "DramaX": "DramaX",
    "Community": "Community",
    "Drama": "Drama",
    "drama": "drama",
    "D": "D", # Special 'A' used in branding
    "ramax": "ramax",
    "x": "x",
    
    # CASE VARIATIONS
    "DramaX": "DramaX",
    "Dramax": "Dramax",
    "dramax": "dramax",
    "DRAMAX": "DRAMAX",
    "AutoDrama": "AutoDrama",
    "Auto Drama": "Auto Drama",
    "AutoDrama": "AutoDrama",
    "Drama": "Drama",
    "drama": "drama",
    "DRAMA": "DRAMA",
    
    # CAPTION HEADERS (Mixed styled unicode)
    "Lᴀᴛᴇsᴛ Aɪʀɪɴɢ Dʀᴀᴍᴀ": "Lᴀᴛᴇsᴛ Aɪʀɪɴɢ Dʀᴀᴍᴀ", # Already fixed in some, just in case
    "Cᴜʀʀᴇɴᴛʟʏ Aɪʀɪɴɢ Dʀᴀᴍᴀ": "Cᴜʀʀᴇɴᴛʟʏ Aɪʀɪɴɢ Dʀᴀᴍᴀ",
    
    # LINKS & SOURCES
    "kdramamaza.net": "kdramamaza.net",
    "https://image.tmdb.org/t/p/original/": "https://image.tmdb.org/t/p/original/",
    "tmdb.org": "tmdb.org",
    "t.me/DramaxCommunity": "t.me/DramaxCommunity",
    
    # FUNCTION & VARIABLE NAMES (Regex is better for these, but including common ones)
    "post_drama_to_dedicated_channel": "post_drama_to_dedicated_channel",
    "get_drama_channel": "get_drama_channel",
    "add_drama_channel": "add_drama_channel",
    "remove_drama_channel": "remove_drama_channel",
    "list_drama_channels": "list_drama_channels",
    "drama_queue": "drama_queue",
}

def final_purge(content):
    # 1. Direct string replacements (ordered by length to avoid partial overwrites)
    # We sort by length descending to catch longer phrases first.
    sorted_keys = sorted(REPLACEMENTS.keys(), key=len, reverse=True)
    for key in sorted_keys:
        content = content.replace(key, REPLACEMENTS[key])
    
    # 2. Case-insensitive Regex for generic words 'drama' and 'dramax' (word boundaries)
    content = re.sub(r'\bdrama\b', 'drama', content, flags=re.IGNORECASE)
    content = re.sub(r'\bdramax\b', 'dramax', content, flags=re.IGNORECASE)
    content = re.sub(r'\bshogunate\b', 'community', content, flags=re.IGNORECASE)
    
    return content

def main():
    exclude = {'.git', '__pycache__', '.gemini'}
    modified_files = []
    
    for root, dirs, files in os.walk('.'):
        dirs[:] = [d for d in dirs if d not in exclude]
        for f in files:
            # Target all relevant source/config files
            if f.endswith(('.py', '.txt', '.env', '.md', 'Procfile', 'yml', 'Dockerfile')):
                fpath = os.path.join(root, f)
                try:
                    with open(fpath, 'r', encoding='utf-8', errors='ignore') as file:
                        original = file.read()
                    
                    purged = final_purge(original)
                    
                    if purged != original:
                        with open(fpath, 'w', encoding='utf-8') as file:
                            file.write(purged)
                        modified_files.append(fpath)
                except Exception as e:
                    print(f"Error in {fpath}: {e}")
    
    if modified_files:
        print(f"Purge successful! Modified {len(modified_files)} files:")
        for f in modified_files:
            print(f"- {f}")
    else:
        print("No drama/dramax references found.")

if __name__ == "__main__":
    main()
