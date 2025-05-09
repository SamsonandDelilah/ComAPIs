import yaml
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).parent
yaml_path = BASE_DIR / "data" / "base_SI_units_data.yaml"
db_path = BASE_DIR / "data" / "base_SI_units.db"

try:
    with open(yaml_path, 'r') as f:
        data = yaml.safe_load(f)
except FileNotFoundError:
    print(f"Fehler: {yaml_path} nicht gefunden!")
    exit(1)
except yaml.YAMLError:
    print(f"Fehler: {yaml_path} ist keine gültige YAML-Datei!")
    exit(1)

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS base_SI_units")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS base_SI_units (
            symbol TEXT PRIMARY KEY,
            name_en TEXT,
            dimension TEXT
        )
    ''')
    for item in data['base_SI_units']:
        # Ersetze Unicode-Code durch tatsächliches Zeichen, falls nötig
        dimension = item['dimension'].replace("U+0398", "Θ")
        cursor.execute(
            'INSERT OR REPLACE INTO base_SI_units (symbol, name_en, dimension) VALUES (?, ?, ?)',
            (item['symbol'], item['name_en'], dimension)
        )
    conn.commit()
    print("✅ SI-Basiseinheiten erfolgreich importiert!")
except sqlite3.Error as e:
    print(f"❌ Datenbankfehler: {e}")
finally:
    if conn:
        conn.close()
