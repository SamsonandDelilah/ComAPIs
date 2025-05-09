import yaml
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).parent
yaml_path = BASE_DIR / "data" / "translations_data.yaml"
db_path = BASE_DIR / "data" / "translations.db"

with open(yaml_path, "r", encoding="utf-8") as f:
    data = yaml.safe_load(f)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

for item in data["translations"]:
    cursor.execute(
        "INSERT OR IGNORE INTO translations (entity_type, entity_id, lang, value, version, source, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            item["entity_type"],
            item["entity_id"],
            item["lang"],
            item["value"],
            item.get("version", "1.0.0"),
            item.get("source", "manual"),
            item.get("timestamp", None)
        )
    )

conn.commit()
conn.close()
print("✅ Übersetzungsdaten erfolgreich importiert!")
