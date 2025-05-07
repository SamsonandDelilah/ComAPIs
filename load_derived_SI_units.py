import yaml
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).parent
yaml_path = BASE_DIR / "data" / "derived_SI_units_data.yaml"
db_path = BASE_DIR / "data" / "derived_SI_units.db"

with open(yaml_path, "r", encoding="utf-8") as f:
    data = yaml.safe_load(f)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

for item in data["derived_SI_units"]:
    cursor.execute(
        "INSERT OR IGNORE INTO derived_SI_units (symbol, name_en, dimension, definition) VALUES (?, ?, ?, ?)",
        (item["symbol"], item["name_en"], item["dimension"], item.get("definition", ""))
    )

conn.commit()
conn.close()
print("✅ Abgeleitete SI-Einheiten importiert!")
