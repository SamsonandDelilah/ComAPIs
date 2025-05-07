# init_db.py
import sqlite3
import yaml
from pathlib import Path

BASE_DIR = Path(__file__).parent.resolve()

DB_PATHS = {
    "base_SI_units": BASE_DIR / "data" / "base_SI_units.db",
    "derived_SI_units": BASE_DIR / "data" / "derived_SI_units.db",
    "countries": BASE_DIR / "data" / "countries.db",
    "languages": BASE_DIR / "data" / "languages.db",
    "translations": BASE_DIR / "data" / "translations.db",
    "prefixes": BASE_DIR / "data" / "prefixes.db"
}

def init_db():
    for name, db_path in DB_PATHS.items():
        schema_path = BASE_DIR / "schemas" / f"{name}_schema.yaml"
        if not schema_path.exists():
            continue

        with open(schema_path, 'r') as f:
            config = yaml.safe_load(f)

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        columns = []
        for field in config['fields']:
            column_def = f"{field['name']} {field['type']}"
            if field.get('unique'):
                column_def += " UNIQUE"
            columns.append(column_def)

        create_sql = f"CREATE TABLE IF NOT EXISTS {name} ({', '.join(columns)})"
        cursor.execute(create_sql)

        conn.commit()
        conn.close()

if __name__ == "__main__":
    init_db()
