import yaml
import re
from pathlib import Path

# Paths
project_root = Path("I:/ComAPIs")
data_dir = project_root / "data" / "utilities"
backup_path = data_dir / "translations_data_backup.yaml"
sources_config_path = project_root / "translation_sources.yaml"

# 1. Build key -> en mapping
with open(backup_path, "r", encoding="utf-8") as f:
    backup_yaml = yaml.safe_load(f)
key_to_en = {entry["key"]: entry["en"] for entry in backup_yaml.get("data", []) if "key" in entry and "en" in entry}

# 2. Load list of code files to process
with open(sources_config_path, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)
files_to_scan = config["program_files"]

# 3. Regex to find get_translation("KEY", ...)
pattern = re.compile(r'get_translation\(\s*[\'"]([^\'"]+)[\'"]')

for rel_path in files_to_scan:
    file_path = project_root / rel_path
    if not file_path.exists():
        print(f"WARNING: File not found: {file_path}")
        continue

    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Function to replace keys with English text
    def replacer(match):
        key = match.group(1)
        if key in key_to_en:
            en_text = key_to_en[key].replace('"', '\\"')  # Escape quotes for safety
            print(f"  Replacing: {key} -> {en_text}")
            return f'get_translation("{en_text}"'
        else:
            print(f"  WARNING: Key '{key}' not found in backup, leaving unchanged.")
            return match.group(0)

    new_content = pattern.sub(replacer, content)

    # Write back only if changed
    if new_content != content:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(new_content)
        print(f"Updated {file_path}")
    else:
        print(f"No changes in {file_path}")

print("Migration complete. Now run extract_translation_keys.py to extract new keys.")
