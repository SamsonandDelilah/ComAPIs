from fastapi import FastAPI, HTTPException
import sqlite3
import yaml
from pathlib import Path
from contextlib import closing
import logging

# Logging konfigurieren
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="CommonsAPI", version="1.0.0")

BASE_DIR = Path(__file__).parent.resolve()

DB_PATHS = {
    "base_SI_units": BASE_DIR / "data" / "base_SI_units.db",
    "derived_SI_units": BASE_DIR / "data" / "derived_SI_units.db",
    "countries": BASE_DIR / "data" / "countries.db",
    "languages": BASE_DIR / "data" / "languages.db",
    "translations": BASE_DIR / "data" / "translations.db",
    "prefixes": BASE_DIR / "data" / "prefixes.db"
}

@app.on_event("startup")
def init_db():
    """Initialisiert alle Datenbanken beim Serverstart"""
    for name, db_path in DB_PATHS.items():
        schema_path = BASE_DIR / "schemas" / f"{name}_schema.yaml"
        if not schema_path.exists():
            logger.warning(f"Schema {schema_path} nicht gefunden, überspringe Tabelle {name}")
            continue
        with open(schema_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        with closing(sqlite3.connect(db_path)) as conn:
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
            logger.info(f"Tabelle {name} in {db_path} initialisiert")

def create_get_all_endpoint(table: str, db_path: Path):
    @app.get(f"/{table}", tags=[table])
    def get_all():
        try:
            with closing(sqlite3.connect(db_path)) as conn:
                cursor = conn.execute(f"SELECT * FROM {table}")
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                logger.info(f"{table}: {len(rows)} Einträge gefunden")
                return [dict(zip(columns, row)) for row in rows]
        except sqlite3.Error as e:
            logger.error(f"Datenbankfehler in {table}: {str(e)}")
            raise HTTPException(500, detail=f"Datenbankfehler: {str(e)}")

# Dynamische Endpunkte für alle Tabellen
for schema_file in (BASE_DIR / "schemas").glob("*_schema.yaml"):
    with open(schema_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    table = config.get("table")
    db_path = DB_PATHS.get(table)
    if table and db_path:
        create_get_all_endpoint(table, db_path)
        logger.info(f"Endpunkt für Tabelle {table} registriert")

# Beispiel für einen manuellen Endpunkt
@app.get("/")
def root():
    return {
        "message": "CommonsAPI läuft!",
        "version": app.version,
        "endpoints": [route.path for route in app.routes]
    }
