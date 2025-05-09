from fastapi import FastAPI, APIRouter, HTTPException, Query
from pathlib import Path
import sqlite3
import yaml
import numpy as np
from contextlib import asynccontextmanager

# ---------- Configuration ----------
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DB_DIR = BASE_DIR / "db"

# ---------- Helper functions ----------
def get_translation(key: str, lang: str = "en", **kwargs) -> str:
    translations_db = DB_DIR / "utilities" / "translations.db"
    try:
        with sqlite3.connect(translations_db) as conn:
            cursor = conn.execute(
                f"SELECT {lang} FROM translations WHERE key = ?", 
                (key,)
            )
            row = cursor.fetchone()
            if row and row[0]:
                return row[0].format(**kwargs)
            else:
                return key  # fallback to key if no translation found
    except Exception:
        return key  # fallback on any error

def deserialize(value):
    """Deserialize BLOBs to Python-object"""
    if isinstance(value, bytes):
        try:
            return np.frombuffer(value, dtype=np.float64).tolist()
        except Exception:
            return "<BLOB>"
    return value

def get_db_path(data_file: Path) -> Path:
    """DB path from YAML-file (like autoschema.py)"""
    rel_path = data_file.relative_to(DATA_DIR)
    db_name = data_file.stem.replace("_data", "") + ".db"
    return DB_DIR / rel_path.parent / db_name

# ---------- Router-generation ----------
def create_router_for_file(data_file: Path) -> APIRouter:
    router = APIRouter()
    db_path = get_db_path(data_file)
    table_name = data_file.stem.replace("_data", "")

    @router.get("/")
    def get_all(lang: str = Query("en")):
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute(f"SELECT * FROM {table_name}")
                columns = [col[0] for col in cursor.description]
                return [
                    {col: deserialize(val) for col, val in zip(columns, row)}
                    for row in cursor.fetchall()
                ]
        except sqlite3.OperationalError as e:
            detail = get_translation("DATABASE_ERROR", lang, error=str(e))
            raise HTTPException(500, detail=detail)

    return router

# ---------- Lifespan event for router registration ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Registration of endpoints for all YAML-files"""
    for data_file in DATA_DIR.rglob("*_data.yaml"):
        # Create API-path and replace backslashes by normal slashes
        endpoint_path = str(data_file.relative_to(DATA_DIR).with_name(data_file.stem.replace("_data", "")))
        endpoint_path = endpoint_path.replace("\\", "/")  

        # Create Router and register
        router = create_router_for_file(data_file)
        app.include_router(
            router,
            prefix=f"/{endpoint_path}",
            tags=[endpoint_path.split("/")[0]]  # category (e.g. 'utilities')
        )
        print(f"Registered route: /{endpoint_path}")

    yield  # FastAPI requires yield in lifespan context

app = FastAPI(title="ComAPIs", version="1.0.0", lifespan=lifespan)

# ---------- Root-Endpoint ----------
@app.get("/")
def root():
    return {
        "message": "ComAPIs is up and running!",
        "endpoints": [route.path for route in app.routes if route.path != "/"]
    }
