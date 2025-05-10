""" 
Make sure your virtual environment (venv) is active with command ".\venv\Scripts\Activate.ps1" or "activate", and pytest is installed.
"""

# === test_autoschema.py ===
import unittest
from scripts.autoschema import SchemaHandler, Validator

# === Test Schema Generation ===
class TestSchemaHandler(unittest.TestCase):
    def setUp(self):
        """Set up a SchemaHandler instance for testing."""
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
class TestValidator(unittest.TestCase):
    def setUp(self):
        """Set up a Validator instance for testing."""
        self.validator = Validator()

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

# === Run Tests ===
if __name__ == "__main__":
    unittest.main()