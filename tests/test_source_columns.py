"""
Tests for source column detection feature.

Tests that the DBTGenerator can read columns from various file formats
when column information is not available from the Alteryx workflow.
"""
import os
import sys
import tempfile
import json

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dbt_generator import DBTGenerator


def test_csv_column_reading():
    """Test reading columns from a CSV file."""
    generator = DBTGenerator(tempfile.mkdtemp(), interactive=False)

    test_file = os.path.join(os.path.dirname(__file__), 'test_data', 'sample_customers.csv')
    columns = generator._read_csv_columns(test_file)

    expected = ['customer_id', 'customer_name', 'email', 'region', 'created_date', 'is_active']
    assert columns == expected, f"Expected {expected}, got {columns}"
    print("[PASS] CSV column reading works correctly")


def test_json_column_reading():
    """Test reading columns from a JSON file."""
    generator = DBTGenerator(tempfile.mkdtemp(), interactive=False)

    test_file = os.path.join(os.path.dirname(__file__), 'test_data', 'sample_orders.json')
    columns = generator._read_json_columns(test_file)

    expected = ['order_id', 'customer_id', 'product', 'quantity', 'price', 'order_date']
    assert columns == expected, f"Expected {expected}, got {columns}"
    print("[PASS] JSON column reading works correctly")


def test_read_file_columns_dispatcher():
    """Test that _read_file_columns dispatches to correct reader based on extension."""
    generator = DBTGenerator(tempfile.mkdtemp(), interactive=False)

    csv_file = os.path.join(os.path.dirname(__file__), 'test_data', 'sample_customers.csv')
    json_file = os.path.join(os.path.dirname(__file__), 'test_data', 'sample_orders.json')

    csv_cols = generator._read_file_columns(csv_file)
    json_cols = generator._read_file_columns(json_file)

    assert len(csv_cols) == 6, f"Expected 6 CSV columns, got {len(csv_cols)}"
    assert len(json_cols) == 6, f"Expected 6 JSON columns, got {len(json_cols)}"
    print("[PASS] File type dispatcher works correctly")


def test_nonexistent_file():
    """Test that nonexistent files return empty list."""
    generator = DBTGenerator(tempfile.mkdtemp(), interactive=False)

    columns = generator._read_file_columns('/nonexistent/path/file.csv')
    assert columns == [], f"Expected empty list for nonexistent file, got {columns}"
    print("[PASS] Nonexistent file handling works correctly")


def test_json_nested_data_structure():
    """Test reading columns from JSON with nested 'data' key."""
    generator = DBTGenerator(tempfile.mkdtemp(), interactive=False)

    # Create temp file with nested structure
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump({
            "meta": {"total": 2},
            "data": [
                {"id": 1, "name": "Test", "value": 100},
                {"id": 2, "name": "Test2", "value": 200}
            ]
        }, f)
        temp_path = f.name

    try:
        columns = generator._read_json_columns(temp_path)
        expected = ['id', 'name', 'value']
        assert columns == expected, f"Expected {expected}, got {columns}"
        print("[PASS] Nested JSON structure reading works correctly")
    finally:
        os.unlink(temp_path)


def test_csv_with_different_delimiters():
    """Test CSV reading with semicolon delimiter."""
    generator = DBTGenerator(tempfile.mkdtemp(), interactive=False)

    # Create temp file with semicolon delimiter
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("col_a;col_b;col_c\n")
        f.write("1;2;3\n")
        temp_path = f.name

    try:
        columns = generator._read_csv_columns(temp_path)
        expected = ['col_a', 'col_b', 'col_c']
        assert columns == expected, f"Expected {expected}, got {columns}"
        print("[PASS] CSV with semicolon delimiter works correctly")
    finally:
        os.unlink(temp_path)


def run_all_tests():
    """Run all tests."""
    print("Testing source column detection feature...\n")

    test_csv_column_reading()
    test_json_column_reading()
    test_read_file_columns_dispatcher()
    test_nonexistent_file()
    test_json_nested_data_structure()
    test_csv_with_different_delimiters()

    print("\n" + "=" * 50)
    print("All tests passed!")
    print("=" * 50)


if __name__ == '__main__':
    run_all_tests()
