import yaml
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).parent
yaml_path = BASE_DIR / "data" / "prefixes_data.yaml"
db_path = BASE_DIR / "data" / "prefixes.db"

with open(yaml_path, "r", encoding="utf-8") as f:
    data = yaml.safe_load(f)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

for item in data["prefixes"]:
    cursor.execute(
        "INSERT OR IGNORE INTO prefixes (symbol, name_en, factor, unit_symbol) VALUES (?, ?, ?, ?)",
        (item["symbol"], item["name_en"], item["factor"], item["unit_symbol"])
    )

conn.commit()
conn.close()
print("✅ Prefix-Daten erfolgreich importiert!")
