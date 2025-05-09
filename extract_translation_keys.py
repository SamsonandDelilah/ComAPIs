import yaml
import re
from pathlib import Path
from libretranslatepy import LibreTranslateAPI

# ---------- CONFIGURATION ----------
PROJECT_ROOT = Path.cwd()
SOURCES_CONFIG_PATH = PROJECT_ROOT / "translation_sources.yaml"
TRANSLATIONS_PATH = PROJECT_ROOT / "data" / "utilities" / "translations_data.yaml"
LANGUAGES_PATH = PROJECT_ROOT / "data" / "utilities" / "languages_data.yaml"
LT_URL = "http://localhost:5000/"  # Use your local LibreTranslate instance!
lt = LibreTranslateAPI(LT_URL)

# ---------- 1. Read target languages from languages_data.yaml ----------
with open(LANGUAGES_PATH, "r", encoding="utf-8") as f:
    lang_yaml = yaml.safe_load(f)
target_langs = [entry["code"] for entry in lang_yaml.get("data", []) if entry.get("code") != "en"]
print(f"Target languages: {target_langs}")

# ---------- 2. Extract translation keys from source files ----------
with open(SOURCES_CONFIG_PATH, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)
files_to_scan = config["program_files"]

pattern = re.compile(r'get_translation\(\s*[fF]?[\'"](.+?)[\'"]')
all_keys = set()
file_key_map = {}

for rel_path in files_to_scan:
    file_path = PROJECT_ROOT / rel_path
    if not file_path.exists():
        print(f"WARNING: File not found: {file_path}")
        continue
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
        matches = pattern.findall(content)
        all_keys.update(matches)
        file_key_map[rel_path] = matches

# Logging: Show per-file results
for rel_path, keys in file_key_map.items():
    if keys:
        print(f"\n[{rel_path}] found {len(keys)} translation keys:")
        for key in sorted(keys):
            print(f"   {key}")
    else:
        print(f"\n[{rel_path}] found 0 translation keys.")

print(f"\nTotal unique translation keys: {len(all_keys)}")

# ---------- 3. Load and update translations_data.yaml ----------
if TRANSLATIONS_PATH.exists():
    with open(TRANSLATIONS_PATH, "r", encoding="utf-8") as f:
        translations_yaml = yaml.safe_load(f)
    if not translations_yaml:
        translations_yaml = {"metadata": {}, "data": []}
else:
    translations_yaml = {"metadata": {}, "data": []}

existing_data = translations_yaml.get("data")
if not isinstance(existing_data, list):
    existing_data = []

existing_keys = {entry.get("en") for entry in existing_data if "en" in entry}

# Add any missing keys with 'en' as the key itself
new_entries = []
for key in sorted(all_keys):
    if key not in existing_keys:
        new_entry = {"en": key}
        new_entries.append(new_entry)
        existing_data.append(new_entry)

if new_entries:
    print(f"Added {len(new_entries)} new translation entries.")
else:
    print("No new translation entries to add.")

# ---------- 4. Auto-translate missing languages ----------
def translate_with_tokens(text, target_lang):
    placeholder_pattern = re.compile(r"\{[^}]+\}")
    placeholders = placeholder_pattern.findall(text)
    masked_text = text
    token_map = {}
    for i, ph in enumerate(placeholders):
        token = f"xx{i}x"
        masked_text = masked_text.replace(ph, token)
        token_map[token] = ph
    try:
        translated_masked = lt.translate(masked_text, "en", target_lang)
        for token, ph in token_map.items():
            translated_masked = translated_masked.replace(token, ph)
        return translated_masked
    except Exception as e:
        print(f"[ERROR] {e}")
        return ""

updated = False
for entry in existing_data:
    en_text = entry.get("en")
    if not en_text:
        continue
    for lang in target_langs:
        if lang not in entry or not entry[lang]:
            translated = translate_with_tokens(en_text, lang)
            if translated:
                entry[lang] = translated
                print(f"Translated to {lang}: {en_text} -> {translated}")
                updated = True

# ---------- 5. Write back to YAML ----------
translations_yaml["data"] = existing_data
with open(TRANSLATIONS_PATH, "w", encoding="utf-8") as f:
    yaml.dump(translations_yaml, f, allow_unicode=True, sort_keys=False)

if updated:
    print("translations_data.yaml updated with new translations.")
else:
    print("No missing translations found.")

