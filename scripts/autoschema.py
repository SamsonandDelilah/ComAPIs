import os, sys
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

logging.basicConfig(level=logging.DEBUG)
        
# Internationalization (i18n) - translations

# Load translations from YAML file
def load_translations_from_yaml():
    """
    Load translations from a YAML file.
    Expected file location: ../data/utilities/translations_data.yaml
    """
    yaml_path = Path(__file__).parent.parent / "data" / "utilities" / "translations_data.yaml"
    try:
        with open(yaml_path, "r", encoding="utf-8") as file:
            logging.debug(f"Successfully loaded translations from YAML: {yaml_path}")
            return yaml.safe_load(file)  # Load YAML content as a dictionary
    except FileNotFoundError:
        logging.error(f"YAML translation file not found at {yaml_path}.")
        return {}
    except Exception as e:
        logging.error(f"Error loading YAML file: {e}")
        return {}

# Cache YAML translations globally for performance
yaml_translations = load_translations_from_yaml()

def get_translation(key: str, lang: str = "en", **kwargs) -> str:
    """
    Get translations from the database or fall back to YAML.
    :param key: Translation key to look up
    :param lang: Language to translate to (default: 'en')
    :param kwargs: Formatting arguments for the translation string
    :return: Translated string or fallback
    """
    # Resolve the database path to an absolute path
    translations_db = (Path(__file__).parent.parent / "db" / "utilities" / "translations.db").resolve()

    # Debugging: Log the resolved paths
    logging.debug(f"Resolved translations_db path: {translations_db}")
    
    # Attempt to fetch from SQLite database
    try:
        with sqlite3.connect(translations_db) as conn:
            logging.debug("Successfully connected to the translations database.")
            cursor = conn.execute(
                f"SELECT {lang} FROM translations WHERE key = ?", 
                (key,)
            )
            row = cursor.fetchone()
            if row:
                logging.debug(f"Translation found for key '{key}' in database: {row[0]}")
                return row[0].format(**kwargs)
    except sqlite3.OperationalError as e:
        logging.error(f"OperationalError: Could not open database file: {e}")
    except Exception as e:
        logging.error(f"Unexpected error for key '{key}': {e}")

    # Fallback to YAML if database lookup fails
    if key in yaml_translations:
        lang_translation = yaml_translations[key].get(lang)
        if lang_translation:
            logging.debug(f"Translation found for key '{key}' in YAML: {lang_translation}")
            return lang_translation.format(**kwargs)
        else:
            logging.warning(f"No translation found for key '{key}' in language '{lang}'. Using fallback.")
            return key.format(**kwargs)
    else:
        logging.warning(f"No translation found for key '{key}'. Using fallback.")
        return key.format(**kwargs)
    

# === Check if NumPy is installed and install dynamically if missing ===
try:
    import numpy as np
except ImportError:
    logging.error(get_translation("NumPy is not installed! Install with 'pip install numpy'"))
    if input("Do you want to install NumPy now? (y/n): ").strip().lower() == "y":
        import subprocess
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "numpy"])
            import numpy as np
            logging.info(get_translation("NumPy installed successfully."))
        except Exception as e:
            logging.error(get_translation("Failed to install NumPy: {error}", error=str(e)))
            sys.exit(1)
    else:
        sys.exit(1)

class DataProcessor:
    """Base class for data processing."""
    SUPPORTED_SUFFIXES = ["_data.yaml", "_data.yml", "_data.json", "_data.csv"]

    def __init__(self, 
    data_dir: str = os.getenv("DATA_DIR", "data"), 
    schema_dir: str = os.getenv("SCHEMA_DIR", "schemas"),
    db_dir: str = os.getenv("DB_DIR", "db"), 
    version_file: str = os.getenv("VERSION_FILE", ".version_control.yaml")):
        self.data_dir = Path(data_dir)
        self.schema_dir = Path(schema_dir)
        self.db_dir = Path(db_dir)
        self.version_file = Path(version_file)
        self.version_data = self._load_version_data()

        # Ordner erstellen
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.schema_dir.mkdir(parents=True, exist_ok=True)
        self.db_dir.mkdir(parents=True, exist_ok=True)

    def _load_version_data(self) -> Dict:
        """Load version control data."""
        if self.version_file.exists():
            try:
                with open(self.version_file, "r", encoding="utf-8") as f:
                    return yaml.safe_load(f) or {}
            except FileNotFoundError:
                logging.error(get_translation("Version control file not found: {file}", file=self.version_file))
            except yaml.YAMLError as e:
                logging.error(get_translation("Error parsing version file: {e}", e=str(e)))
            except Exception as e:
                logging.error(get_translation("Unexpected error reading version file: {e}", e=str(e)))
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
        """Process a single file."""
        logging.info(get_translation("Processing file: {path}", path=data_path))
        try:
            # Load data
            data = self._load_data(data_path)

            # Generate schema or load existing
            schema = self._get_or_create_schema(data_path, data)

            # Validation
            for entry in data:
                self.validator.validate_entry(entry, schema)

            # Update database
            db_path = self._data_to_db_path(data_path)
            self.create_table(schema, db_path)
            self.insert_data(data, schema, db_path)
        except (FileNotFoundError, ValueError) as e:
            logging.error(get_translation("Error processing {path}: {error}", path=data_path, error=str(e)))
        except sqlite3.DatabaseError as e:
            logging.error(get_translation("Database error processing {path}: {error}", path=data_path, error=str(e)))
        except Exception as e:
            logging.error(get_translation("Unexpected error processing {path}: {error}", path=data_path, error=str(e)))
            raise

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
        """
        Validates a single entry against the schema provided.
        """
        for field in schema["fields"]:
            value = entry.get(field["name"])
            field_type = field.get("type")
            type_params = field.get("type_params", [])

            # --- TEXT validation ---
            if field_type == "TEXT":
                if value is not None:
                    # Check if the value is a string
                    if not isinstance(value, str):
                        logging.error(get_translation(
                            "Field '{field}' expected TEXT but got {vtype}: {value}",
                            field=field["name"],
                            vtype=type(value).__name__,
                            value=repr(value)
                        ))
                        raise ValueError(get_translation(
                            "Invalid TEXT value in field '{field}': {value}",
                            field=field["name"],
                            value=repr(value)
                        ))

                    # Check for malicious inputs (control/non-printable characters)
                    if any(c in value for c in ("\x00", "\x1a", "\x1b")):
                        logging.error(get_translation(
                            "Field '{field}' contains potentially malicious characters: {value}",
                            field=field["name"],
                            value=repr(value)
                        ))
                        raise ValueError(get_translation(
                            "Invalid TEXT value in field '{field}': {value}",
                            field=field["name"],
                            value=repr(value)
                        ))

                    # Check printable characters while allowing valid escaped sequences
                    try:
                        decoded_value = value.encode('utf-8').decode('unicode_escape')
                        if not decoded_value.isprintable():
                            logging.error(get_translation(
                                "Field '{field}' contains non-printable characters: {value}",
                                field=field["name"],
                                value=repr(value)
                            ))
                            raise ValueError(get_translation(
                                "Invalid TEXT value in field '{field}': {value}",
                                field=field["name"],
                                value=repr(value)
                            ))
                    except UnicodeDecodeError as e:
                        logging.error(get_translation(
                            "Field '{field}' contains invalid Unicode: {value}, Error: {error}",
                            field=field["name"],
                            value=repr(value),
                            error=str(e)
                        ))
                        raise ValueError(get_translation(
                            "Invalid TEXT value in field '{field}': {value}",
                            field=field["name"],
                            value=repr(value)
                        ))

                continue  # Skip to next field if valid

            # --- INT/REAL validation ---
            if field_type in ("INT", "REAL", "INTEGER"):
                if value is not None and not isinstance(value, (int, float)):
                    logging.error(get_translation(
                        "Field '{field}' expected {ftype}, got {vtype}: {value}",
                        field=field["name"],
                        ftype=field_type,
                        vtype=type(value).__name__,
                        value=repr(value)
                    ))
                    raise ValueError(get_translation(
                        "Invalid {ftype} value in field '{field}': {value}",
                        ftype=field_type,
                        field=field["name"],
                        value=repr(value)
                    ))

            # Handle other types (VECTOR, MATRIX, etc.)
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
        """
        Validate a vector entry.
        Raises a ValueError if the length of the vector does not match the expected length.

        :param value: The vector to validate.
        :param params: The expected parameters (e.g., [expected_length]).
        """
        expected_length = params[0] if params else 0
        actual_length = len(value)

        if actual_length != expected_length:
            raise ValueError(get_translation(
                "Vectore expects {expected_length} elements, got {actual_length}",
                expected_length=expected_length,
                actual_length=actual_length  # Pass actual_length explicitly
            ))

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
        """Processing single file."""
        logging.info(get_translation("Processing file: {path}", path=data_path))

        try:
            data = self._load_data(data_path)
            schema = self._get_or_create_schema(data_path, data)

            validation_errors = []
            valid_entries = []
            for entry in data:
                try:
                    self.validator.validate_entry(entry, schema)
                    valid_entries.append(entry)
                except ValueError as ve:
                    logging.error(get_translation(
                        "Validation error in file {path}, entry {entry}: {error}",
                        path=data_path,
                        entry=entry,
                        error=str(ve)
                    ))
                    validation_errors.append((entry, str(ve)))
                    # Continue to next entry

            if validation_errors:
                print(f"\nValidation errors found in {data_path}:")
                for entry, error in validation_errors:
                    print(f"  Entry: {entry}\n    Error: {error}")
                # Optionally, skip DB update if there are errors:
                # return

            # Proceed with valid entries only
            if valid_entries:
                db_path = self._data_to_db_path(data_path)
                self.create_table(schema, db_path)
                self.insert_data(valid_entries, schema, db_path)
            else:
                logging.warning(get_translation(
                    "No valid entries to insert for {path}", path=data_path
                ))

        except Exception as e:
            logging.error(get_translation(
                "Error processing {path}: {error}",
                path=data_path,
                error=str(e)
            ))
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