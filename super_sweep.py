import os
import re

# Comprehensive replacement map for all case variations and specific strings
MAPPINGS = [
    # Branding
    ('Dramax', 'DramaX'),
    ('Dramax', 'Dramax'),
    ('Dramax', 'dramax'),
    ('Dramax', 'DRAMAX'),
    ('Dramax', 'DʀᴀᴍᴀX'),
    ('AutoDrama', 'AutoDrama'),
    ('Auto Drama', 'Auto Drama'),
    ('Auto Drama', 'Auto drama'),

    # Core Terms
    ('Drama', 'Drama'),
    ('Drama', 'drama'),
    ('Drama', 'DRAMA'),
    ('Dramax', 'Dʀᴀᴍᴀ'),

    # Banners/Sources
    ('kdramamaza.net', 'kdramamaza.net'),
    ('https://image.tmdb.org/t/p/original/', 'https://image.tmdb.org/t/p/original/'), # Better mapping for TMDB
    ('tmdb.org', 'tmdb.org'),
    
    # Specific branding suffixes
    ('Community', '𝗖𝗼𝗺𝗺𝘂𝗻𝗶𝘁𝘆'),
]

def replace_all(text):
    for old, new in MAPPINGS:
        # Use simple string replace to preserve case and partials as much as possible
        text = text.replace(old, new)
    return text

def fix_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            original = f.read()
        
        new_text = replace_all(original)
        
        if new_text != original:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_text)
            return True
    except Exception as e:
        print(f"Error reading/writing {file_path}: {e}")
    return False

def main():
    exclude_dirs = {'.git', '__pycache__', '.gemini'}
    exclude_files = {'super_sweep.py', 'sweep.py', 'fix_handlers.py', 'fix_scheduler.py', 'sweep_output.txt'}
    
    count = 0
    for root, dirs, files in os.walk('.'):
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        for file in files:
            if file in exclude_files:
                continue
            if file.endswith(('.py', '.txt', '.env', '.md', 'Procfile', 'Dockerfile', 'yml')):
                if fix_file(os.path.join(root, file)):
                    print(f"Purged: {os.path.join(root, file)}")
                    count += 1
    
    print(f"Successfully cleaned {count} files.")

if __name__ == "__main__":
    main()
