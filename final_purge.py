import os
import re

# Comprehensive list of replacements for standard and styled characters
MAPPINGS = [
    # Telegram Styled / Unicode Variations (SmallCaps and Mathematical)
    (r'[AD][\u0274\u1d0f][\u026a\u1d0d][\u1d0d\u1d21\u1d07\u0299\u0280]+', 'Dramax'), # Catches variations of Dramax/Drama
    (r'\b[Aᴀ][ɴn][ɪi][ᴍm][ᴇe]\b', 'Drama'),
    (r'\b[Aᴀ][ɴn][ɪi][wᴡ][ᴇe][ʙb]\b', 'Dramax'),
    (r'[𝖲][ʜ][ᴏ][ɢ][ᴜ][ɴ][ᴀ][ᴛ][ᴇ]', 'Community'), # Unicode styling Community
    (r'Community', 'Community'),
    
    # Links/URLs
    (r't\.me/[a-zA-Z0-9_]*Drama[a-zA-Z0-9_]*', 't.me/DramaxCommunity'), # Generic replacement for telegram links with 'Drama'
    (r't\.me/dramax_Community', 't.me/DramaxCommunity'),
    (r'dramapahe\.si', 'kdramamaza.net'),
    (r'https://img\.anili\.st/media/', 'https://image.tmdb.org/t/p/original/'),
    (r'ani\.li', 'tmdb.org'),
    
    # Generic replacements (again, for redundancy)
    (r'\bDrama\b', 'Drama'),
    (r'\bdrama\b', 'drama'),
    (r'\bDRAMA\b', 'DRAMA'),
    (r'AutoDrama', 'AutoDrama'),
    (r'AutoDrama', 'AutoDrama'),
    
    # Specific branding
    (r'Dramax', 'DramaX'),
    (r'Dramax', 'Dramax'),
]

def purge_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            original = f.read()
            
        new_text = original
        
        # Standard replacements
        for pattern, replacement in MAPPINGS:
            new_text = re.sub(pattern, replacement, new_text, flags=re.IGNORECASE | re.MULTILINE)
            
        # Hardcodes for specific styling
        # "Dramax" -> "Dʀᴀᴍᴀx" (example)
        # Using a direct string mapping for common branding I see
        unicodes = [
            ('Dramax', 'Dramax'),
            ('Community', 'Community'),
            ('Dramax', 'Drama'),
        ]
        for old, new in unicodes:
            new_text = new_text.replace(old, new)
            
        if new_text != original:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_text)
            return True
            
    except Exception as e:
        print(f"Error purging {file_path}: {e}")
    return False

def main():
    skip_dirs = {'.git', '__pycache__', '.gemini'}
    file_count = 0
    total_files = 0
    
    for root, dirs, files in os.walk('.'):
        dirs[:] = [d for d in dirs if d not in skip_dirs]
        for file in files:
            if file.endswith(('.py', '.txt', '.env', '.md', 'Procfile', 'yml')):
                total_files += 1
                if purge_file(os.path.join(root, file)):
                    file_count += 1
                    print(f"Completely Purged: {os.path.join(root, file)}")

    print(f"Total files checked: {total_files}")
    print(f"Files modified: {file_count}")

if __name__ == "__main__":
    main()
