import yaml
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).parent
yaml_path = BASE_DIR / "data" / "languages_data.yaml"
db_path = BASE_DIR / "data" / "languages.db"

with open(yaml_path, "r", encoding="utf-8") as f:
    data = yaml.safe_load(f)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

for item in data["languages"]:
    cursor.execute(
        "INSERT OR IGNORE INTO languages (code, name_en, name_native, script, direction) VALUES (?, ?, ?, ?, ?)",
        (item["code"], item["name_en"], item["name_native"], item["script"], item["direction"])
    )

conn.commit()
conn.close()
print("✅ Sprachdaten erfolgreich importiert!")
