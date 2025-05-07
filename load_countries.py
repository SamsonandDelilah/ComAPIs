import yaml
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).parent
yaml_path = BASE_DIR / "data" / "countries_data.yaml"
db_path = BASE_DIR / "data" / "countries.db"

with open(yaml_path, "r", encoding="utf-8") as f:
    data = yaml.safe_load(f)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

for item in data["countries"]:
    cursor.execute(
        "INSERT OR IGNORE INTO countries (iso2, iso3, name_en, region, currency, languages) VALUES (?, ?, ?, ?, ?, ?)",
        (item["iso2"], item["iso3"], item["name_en"], item["region"], item["currency"], item["languages"])
    )

conn.commit()
conn.close()
print("✅ Länderdaten erfolgreich importiert!")
