r""" 
Make sure your virtual environment (venv) is active with command '.\venv\Scripts\Activate.ps1' or "activate", and pytest is installed.
"""

# === test_autoschema.py ===
from pathlib import Path
import os
import unittest
import logging
import sqlite3
from scripts.autoschema import SchemaHandler, AutoSchemaDB, Validator

# === General setting for logging ===

class BaseTest(unittest.TestCase):
    """Base test class for shared setup logic."""

    def setUp(self):
        """Common setup for all tests."""
        # Suppress warnings during tests
        logging.getLogger().setLevel(logging.ERROR)
        
        # Set the base directory for the project
        self.base_dir = Path(__file__).resolve().parent.parent
        print(f"Base directory set to: {self.base_dir}")

        
        
# === Test Schema Handler ===
class TestSchemaHandler(BaseTest):
    def setUp(self):
        """Set up a SchemaHandler instance for testing."""
        super().setUp()  # Call the shared setup logic from BaseTest
        self.handler = SchemaHandler()

    def test_infer_type_vector(self):
        """Test if '_infer_type' correctly identifies a vector."""
        result = self.handler._infer_type([1, 2, 3])
        self.assertEqual(result, "VEC")

    def test_infer_type_matrix(self):
        """Test if '_infer_type' correctly identifies a matrix."""
        result = self.handler._infer_type([[1, 2], [3, 4]])
        self.assertEqual(result, "MATRIX")

    def test_infer_type_text(self):
        """Test if '_infer_type' correctly identifies a text field."""
        result = self.handler._infer_type(r"hello \u+389")
        self.assertEqual(result, "TEXT")

    
# === Test Validation ===
class TestValidator(BaseTest):
    def setUp(self):
        """Set up a Validator instance for testing."""
        super().setUp()  # Call the shared setup logic from BaseTest
        self.validator  = Validator()

    def test_validate_vector(self):
        """Test if 'validate_entry' correctly validates a vector."""
        schema = {
            "fields": [{"name": "vector_field", "type": "VEC", "type_params": [3]}]
        }
        entry = {"vector_field": [1, 2, 3]}
        # Should pass without raising an error
        self.assertIsNone(self.validator.validate_entry(entry, schema))

        # Test invalid vector length
        invalid_entry = {"vector_field": [1, 2]}
        with self.assertRaises(ValueError):
            self.validator.validate_entry(invalid_entry, schema)

    def test_validate_matrix(self):
        """Test if 'validate_entry' correctly validates a matrix."""
        schema = {
            "fields": [{"name": "matrix_field", "type": "MATRIX", "type_params": [2, 2]}]
        }
        entry = {"matrix_field": [[1, 2], [3, 4]]}
        # Should pass without raising an error
        self.assertIsNone(self.validator.validate_entry(entry, schema))

        # Test invalid matrix dimensions
        invalid_entry = {"matrix_field": [[1, 2], [3]]}  # Unequal row lengths
        with self.assertRaises(ValueError):
            self.validator.validate_entry(invalid_entry, schema)

    def test_validate_quaternion(self):
        """Test if 'validate_entry' correctly validates a quaternion."""
        schema = {
            "fields": [{"name": "quat_field", "type": "QUATERNION", "type_params": []}]
        }
        entry = {"quat_field": [1.0, 0.0, 0.0, 0.0]}
        # Should pass without raising an error
        self.assertIsNone(self.validator.validate_entry(entry, schema))

        # Test invalid quaternion length
        invalid_entry = {"quat_field": [1.0, 0.0, 0.0]}
        with self.assertRaises(ValueError):
            self.validator.validate_entry(invalid_entry, schema)

# === Test Unwrap Nested Data ===
class TestUnwrapNestedData(BaseTest):
    """Unit tests for the '_unwrap_nested_data' method in AutoSchemaDB."""

    def setUp(self):
        """Set up an instance of AutoSchemaDB for testing."""
        super().setUp()  # Call the shared setup logic from BaseTest
        self.processor = AutoSchemaDB()

    def test_extract_data_key(self):
        """Test if '_unwrap_nested_data' correctly extracts the 'data' key."""
        nested_data = {
            "metadata": {
                "filename": "base_SI_units_data.yaml",
                "version": "1.0.0",
            },
            "data": [
                {"symbol": "m", "name_en": "metre", "dimension": "L"},
                {"symbol": "kg", "name_en": "kilogram", "dimension": "M"},
            ],
        }
        expected = [
            {"symbol": "m", "name_en": "metre", "dimension": "L"},
            {"symbol": "kg", "name_en": "kilogram", "dimension": "M"},
        ]
        result = self.processor._unwrap_nested_data(nested_data)
        self.assertEqual(result, expected)

    def test_no_data_key(self):
        """Test if '_unwrap_nested_data' returns an empty list when no 'data' key is present."""
        nested_data = {
            "metadata": {
                "filename": "base_SI_units_data.yaml",
                "version": "1.0.0",
            }
        }
        result = self.processor._unwrap_nested_data(nested_data)
        self.assertEqual(result, [])

    def test_input_is_list(self):
        """Test if '_unwrap_nested_data' directly returns the input when it's already a list."""
        input_data = [
            {"symbol": "m", "name_en": "metre", "dimension": "L"},
            {"symbol": "kg", "name_en": "kilogram", "dimension": "M"},
        ]
        result = self.processor._unwrap_nested_data(input_data)
        self.assertEqual(result, input_data)

    def test_unsupported_structure(self):
        """Test if '_unwrap_nested_data' returns an empty list for unsupported structures."""
        input_data = "This is not a valid input"
        result = self.processor._unwrap_nested_data(input_data)
        self.assertEqual(result, [])
        
class TestAutoSchema(BaseTest):
    """Test cases for the AutoSchemaDB functionality."""

    def setUp(self):
        """Custom setup logic for TestAutoSchema."""
        super().setUp()  # Call shared setup from BaseTest

        # Initialize the processor for schema and DB generation
        self.processor = AutoSchemaDB(
            data_dir=self.base_dir / "data",        # Absolute path to data
            schema_dir=self.base_dir / "schemas",  # Absolute path to schemas
            db_dir=self.base_dir / "db"            # Absolute path to db
        )

        # Ensure the translations table exists in the test database
        db_path = self.processor._data_to_db_path(
            self.base_dir / "data/utilities/translations_data.yaml"
        )
        db_path.parent.mkdir(parents=True, exist_ok=True)  # Create parent dirs
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS translations (
                id INTEGER PRIMARY KEY,
                key TEXT NOT NULL,
                value TEXT NOT NULL
            )
        """)
        conn.commit()
        conn.close()

    def test_data_to_schema_path(self):
        """Test the _data_to_schema_path method."""
        data_path = self.base_dir / "data/physics/units/base_SI_units_data.yaml"
        expected_schema_path = self.base_dir / "schemas/physics/units/base_SI_units_schema.yaml"
        schema_path = self.processor._data_to_schema_path(data_path)
        self.assertEqual(schema_path, expected_schema_path)

    def test_data_to_db_path(self):
        """Test the _data_to_db_path method."""
        data_path = self.base_dir / "data/physics/units/base_SI_units_data.yaml"
        expected_db_path = self.base_dir / "db/physics/units/base_SI_units.db"
        db_path = self.processor._data_to_db_path(data_path)
        self.assertEqual(db_path, expected_db_path)
        
                    
# === Run Tests ===
if __name__ == "__main__":
    unittest.main()