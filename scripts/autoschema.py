import sys
import hashlib
import json
import logging
import sqlite3
import yaml
import csv
from pathlib import Path
from typing import Dict, List, Any, Optional, Union


# Logging-configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# Internationalization (i18n) - translations

def get_translation(key: str, lang: str = "en", **kwargs) -> str:
    """Get translations-DB."""
    translations_db = Path(__file__).parent / "db" / "utilities" / "translations.db"
    try:
        with sqlite3.connect(translations_db) as conn:
            cursor = conn.execute(
                f"SELECT {lang} FROM translations WHERE key = ?", 
                (key,)
            )
            row = cursor.fetchone()
            return row[0].format(**kwargs) if row else key
    except Exception:
        return key  # Fallback to key in cae of errors


# check numpy installed
try:
    import numpy as np
except ImportError:
    logging.error(get_translation("NumPy is not installed! Install with 'pip install numpy'"))
    sys.exit(1)

class DataProcessor:
    """Base class for data processing."""
    SUPPORTED_SUFFIXES = ["_data.yaml", "_data.yml", "_data.json", "_data.csv"]

    def __init__(self, data_dir: str = r"I:\ComAPIs\data", schema_dir: str = "schemas", db_dir: str = "db"):
        self.data_dir = Path(data_dir)
        self.schema_dir = Path(schema_dir)
        self.db_dir = Path(db_dir)
        self.version_file = Path(".version_control.yaml")
        self.version_data = self._load_version_data()

        # Ordner erstellen
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.schema_dir.mkdir(parents=True, exist_ok=True)
        self.db_dir.mkdir(parents=True, exist_ok=True)

    def _load_version_data(self) -> Dict:
        if self.version_file.exists():
            try:
                with open(self.version_file, "r", encoding="utf-8") as f:
                    return yaml.safe_load(f) or {}
            except Exception as e:
                logging.error(get_translation("Error reading version file: {e}", e=str(e)))
        return {}

    def _save_version_data(self):
        with open(self.version_file, "w", encoding="utf-8") as f:
            yaml.dump(self.version_data, f, allow_unicode=True)

    def find_data_files(self) -> List[Path]:
        """Find all supported data files."""
        files = []
        for suffix in self.SUPPORTED_SUFFIXES:
            files.extend(self.data_dir.rglob(f"*{suffix}"))
        return files

    def process_all(self):
        """Process all data fiels"""
        files = self.find_data_files()
        print(get_translation("Files found:") + f": {files}")
        for data_path in self.find_data_files():
            if self._needs_processing(data_path):
                self._process_file(data_path)
        self._save_version_data()

    def _needs_processing(self, data_path: Path) -> bool:
        """Check, if file has to be processed."""
        data_hash = self._file_hash(data_path)
        schema_path = self._data_to_schema_path(data_path)
        schema_exists = schema_path.exists()
        schema_hash = self._file_hash(schema_path) if schema_exists else None

        key = str(data_path.relative_to(self.data_dir)).replace("\\", "/")
        version_entry = self.version_data.get(key, {})

        # Debugging-output
        print(f"Checking {data_path.name}: Schema exists: {schema_exists}, hash match: {version_entry.get('data_hash') == data_hash}")

        # Force processing if schema is missing or hashes differ
        if not schema_exists or version_entry.get('data_hash') != data_hash or version_entry.get('schema_hash') != schema_hash:
            self.version_data[key] = {'data_hash': data_hash, 'schema_hash': schema_hash}
            return True
        return False


    def _file_hash(self, path: Path) -> Optional[str]:
        """Calculate SHA-256-Hash for file."""
        if not path.exists():
            return None
        hasher = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(8192):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _process_file(self, data_path: Path):
        """Processing single file (abstract method)."""
        raise NotImplementedError(get_translation("Subclasses must implement this"))

class SchemaHandler(DataProcessor):
    def _infer_type(self, value: Any) -> str:
        """Recognises complex type from data structures."""
        def get_nesting_depth(obj, depth=0):
            if isinstance(obj, list) and len(obj) > 0:
                return get_nesting_depth(obj[0], depth + 1)
            return depth

        if isinstance(value, list):
            depth = get_nesting_depth(value)
            
            if depth == 1:
                if len(value) == 4:
                    return "QUATERNION"
                return "VEC"
            elif depth == 2:
                return "MATRIX"
            elif depth >= 3:
                return "TENSOR"
            return "VEC"

        # Standard types
        if isinstance(value, bool):
            return "BOOLEAN"
        elif isinstance(value, int):
            return "INTEGER"
        elif isinstance(value, float):
            return "REAL"
        elif isinstance(value, dict):
            return "JSON"
        return "TEXT"

    def _infer_type_params(self, value: Any, field_type: str) -> list:
        """Defines parameters for complex types."""
        def get_shape(obj):
            if isinstance(obj, list):
                return [len(obj)] + get_shape(obj[0]) if obj else []
            return []

        if field_type == "VEC":
            return [len(value)]
        elif field_type == "MATRIX":
            row_lengths = [len(row) for row in value]
            if len(set(row_lengths)) != 1:
                raise ValueError(get_translation("unequal lengths in matrix"))
            return [len(value), row_lengths[0]]
        elif field_type == "TENSOR":
            return get_shape(value)
        elif field_type == "QUATERNION":
            return [4]  # Immer Länge 4
        return []

    def generate_schema(self, data: List[Dict], data_path: Path) -> Dict:
        """Generates schema from data."""
        if not data:
            raise ValueError(get_translation("no data to create schemas"))
            
        sample = data[0] if isinstance(data, list) else data
        fields = []
        for key, value in sample.items():
            field_type = self._infer_type(value)
            fields.append({
                "name": key,
                "type": field_type,
                "type_params": self._infer_type_params(value, field_type)
            })
        
        return {
            "table": data_path.stem.replace("_data", ""),
            "fields": fields,
            "metadata": {"private": False}
        }


class DatabaseHandler(DataProcessor):
    """Handling of databases."""
    def create_table(self, schema: Dict, db_path: Path):
        """Defines a table according schema."""
        # Ordner für die DB-Datei erstellen
        db_path.parent.mkdir(parents=True, exist_ok=True) 

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            columns = []
            for field in schema["fields"]:
                col_def = f"{field['name']} {field['type']}"
                if field.get("primary_key"):
                    col_def += " PRIMARY KEY"
                columns.append(col_def)

            create_sql = f"CREATE TABLE IF NOT EXISTS {schema['table']} ({', '.join(columns)})"
            cursor.execute(create_sql)
            conn.commit()

    def insert_data(self, data: List[Dict], schema: Dict, db_path: Path):
        """Insert data in table."""
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Konvertiere Tensoren zu NumPy-Arrays
            processed_data = []
            for entry in data:
                processed_entry = {}
                for field in schema["fields"]:
                    value = entry.get(field["name"])
                    if field["type"] in ["VEC", "MATRIX", "TENSOR", "QUATERNION"]:
                        # Validierung vor Serialisierung
                        processed_entry[field["name"]] = np.array(value).tobytes()
                    else:
                        processed_entry[field["name"]] = value
                processed_data.append(processed_entry)
                
            field_names = [f["name"] for f in schema["fields"]]
            placeholders = ", ".join(["?"] * len(field_names))
            insert_sql = f"INSERT OR REPLACE INTO {schema['table']} ({', '.join(field_names)}) VALUES ({placeholders})"

            # Batch-Insert für Performance
            entries = []
            for processed_entry in processed_data:
                entries.append([processed_entry.get(name) for name in field_names])
            
            cursor.executemany(insert_sql, entries)
            conn.commit()

class Validator:
    """Validates data against schemas."""
    @staticmethod
    def validate_entry(entry: Dict, schema: Dict):
        for field in schema["fields"]:
            value = entry.get(field["name"])
            field_type = field.get("type")
            type_params = field.get("type_params", [])
            
            if field_type == "VEC":
                Validator._validate_vector(value, type_params)
            elif field_type == "MATRIX":
                Validator._validate_matrix(value, type_params)
            elif field_type == "TENSOR":
                Validator._validate_tensor(value, type_params)
            elif field_type == "QUATERNION":
                Validator._validate_quaternion(value, type_params)      

    @staticmethod
    def _validate_vector(value: list, params: list):
        expected_length = params[0] if params else 0
        if len(value) != expected_length:
            raise ValueError(get_translation("Vectore expects {expected_length} elements, got {len(value)}", 
                expected_length=expected_length, 
                actual_length=len(value)))

    @staticmethod
    def _validate_matrix(value: list, params: list):
        rows, cols = params
        if len(value) != rows:
            raise ValueError(get_translation("Matrix expects {rows} rows", rows=rows))
        for row in value:
            if len(row) != cols:
                raise ValueError(get_translation("Each row must have {cols} columns", cols=cols))

    @staticmethod
    def _validate_tensor(value: list, params: list):
        def check_shape(data, shape, dim_level=0):
            if len(shape) == 0:
                return
            if len(data) != shape[0]:
                expected = shape[0]
                actual = len(data)
                raise ValueError(get_translation("Dimension {dim_level} expects {expected}, got {actual}",
                    dim_level=dim_level,
                    expected=expected,
                    actual=actual
                ))
            for item in data:
                check_shape(item, shape[1:], dim_level + 1)
        
        check_shape(value, params)


    @staticmethod
    def _validate_quaternion(value: list, type_params: list = []):
        if len(value) != 4:
            raise ValueError(get_translation("Quaternion expects exactly 4 elements"))
        # Optional: Normprüfung
        # norm = sum(x**2 for x in value)
        # if not abs(norm - 1.0) < 1e-6:
        #     raise ValueError("Quaternion muss Einheitsnorm haben")
                    
class AutoSchemaDB(SchemaHandler, DatabaseHandler):
    """Main class for automatic schema and DB generation."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.validator = Validator()

    def _process_file(self, data_path: Path):
        """Precessing single file."""
        logging.info(get_translation("Processing file: {path}", path=data_path))
        
        # Load data
        try:
            data = self._load_data(data_path)
            
            # Generate schema or load
            schema = self._get_or_create_schema(data_path, data)
            
            # Validation
            for entry in data:
                self.validator.validate_entry(entry, schema)
            
            # update database
            db_path = self._data_to_db_path(data_path)
            self.create_table(schema, db_path)
            self.insert_data(data, schema, db_path)
            
        except Exception as e:
            logging.error(get_translation("Error processing {path}: {error}", 
                path=data_path, 
                error=str(e)))
            raise
    
    def _load_data(self, data_path: Path) -> List[Dict]:
        """Loads data from various file formats."""
        suffix = data_path.suffix.lower()
        loader = {
            '.yaml': self._load_yaml,
            '.yml': self._load_yaml,
            '.json': self._load_json,
            '.csv': self._load_csv
        }.get(suffix, lambda x: [])
        return loader(data_path)

    def _load_yaml(self, path: Path) -> List[Dict]:
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            return self._unwrap_nested_data(data)

    def _load_json(self, path: Path) -> List[Dict]:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _load_csv(self, path: Path) -> List[Dict]:
        with open(path, 'r', encoding='utf-8') as f:
            return list(csv.DictReader(f))

    def _unwrap_nested_data(self, data: Any) -> List[Dict]:
        """Extracting nested YAML/JSON-structure."""
        if isinstance(data, dict):
            for v in data.values():
                if isinstance(v, list):
                    return v
        return data if isinstance(data, list) else []

    def _get_or_create_schema(self, data_path: Path, data: List[Dict]) -> Dict:
        """Loads existing schema or generates new one."""
        schema_path = self._data_to_schema_path(data_path)
        if schema_path.exists():
            return self._load_schema(schema_path)
        
        schema = self.generate_schema(data, data_path)
        self._save_schema(schema, schema_path)
        return schema

    def _data_to_schema_path(self, data_path: Path) -> Path:
        return self.schema_dir / data_path.relative_to(self.data_dir).with_name(
            data_path.name.replace("_data", "_schema")
        )

    def _data_to_db_path(self, data_path: Path) -> Path:
        # keep full folder structure
        stem = data_path.stem.replace("_data", "")
        db_name = f"{stem}.db"
        
        # Calculate realtive path, without using .parent
        rel_path = data_path.relative_to(self.data_dir)
        target_dir = self.db_dir / rel_path.parent
        
        return target_dir / db_name
    

    def _load_schema(self, schema_path: Path) -> Dict:
        with open(schema_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) if schema_path.suffix in ['.yaml', '.yml'] else json.load(f)

    def _save_schema(self, schema: Dict, schema_path: Path):
        schema_path.parent.mkdir(parents=True, exist_ok=True)
        with open(schema_path, 'w', encoding='utf-8') as f:
            yaml.dump(schema, f, allow_unicode=True)

if __name__ == "__main__":
    processor = AutoSchemaDB()
    processor.process_all()
    logging.info("Processing finished!")