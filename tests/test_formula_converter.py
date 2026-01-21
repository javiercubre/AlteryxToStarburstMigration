"""
Tests for the Alteryx formula to Trino SQL converter.

Tests that Alteryx formulas are correctly converted to their
Trino SQL equivalents.
"""
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from formula_converter import (
    FormulaConverter,
    convert_alteryx_expression,
    convert_aggregation
)


def test_field_references():
    """Test that Alteryx field references [FieldName] are converted to SQL."""
    converter = FormulaConverter()

    # Basic field reference
    result = converter.convert('[CustomerName]')
    assert '"CustomerName"' in result, f"Expected quoted field, got {result}"

    # Multiple field references
    result = converter.convert('[Field1] + [Field2]')
    assert '"Field1"' in result and '"Field2"' in result, f"Got {result}"

    print("[PASS] Field references conversion works correctly")


def test_operator_conversion():
    """Test that Alteryx operators are converted to SQL."""
    converter = FormulaConverter()

    # Equality operator
    result = converter.convert('[Status] == "Active"')
    assert '=' in result and '==' not in result, f"Got {result}"

    # AND/OR operators
    result = converter.convert('[A] && [B]')
    assert ' AND ' in result, f"Got {result}"

    result = converter.convert('[A] || [B]')
    assert ' OR ' in result, f"Got {result}"

    print("[PASS] Operator conversion works correctly")


def test_iif_conversion():
    """Test IIF to CASE WHEN conversion."""
    converter = FormulaConverter()

    # Simple IIF
    result = converter.convert('IIF([Status]="Active", 1, 0)')
    assert 'CASE WHEN' in result and 'THEN' in result and 'ELSE' in result and 'END' in result, \
        f"Expected CASE WHEN, got {result}"

    # Nested IIF
    result = converter.convert('IIF([A]>10, IIF([B]>5, "High", "Med"), "Low")')
    assert result.count('CASE WHEN') == 2, f"Expected 2 CASE WHEN, got {result}"

    print("[PASS] IIF to CASE WHEN conversion works correctly")


def test_isnull_conversion():
    """Test IsNull function conversion."""
    converter = FormulaConverter()

    result = converter.convert('IsNull([Field])')
    assert 'IS NULL' in result, f"Expected IS NULL, got {result}"

    print("[PASS] IsNull conversion works correctly")


def test_isempty_conversion():
    """Test IsEmpty function conversion."""
    converter = FormulaConverter()

    result = converter.convert('IsEmpty([Field])')
    assert "= ''" in result, f"Expected empty string check, got {result}"

    print("[PASS] IsEmpty conversion works correctly")


def test_string_functions():
    """Test string function conversions."""
    converter = FormulaConverter()

    # Trim
    result = converter.convert('Trim([Name])')
    assert 'TRIM' in result, f"Expected TRIM, got {result}"

    # UpperCase
    result = converter.convert('UpperCase([Name])')
    assert 'UPPER' in result, f"Expected UPPER, got {result}"

    # LowerCase
    result = converter.convert('LowerCase([Name])')
    assert 'LOWER' in result, f"Expected LOWER, got {result}"

    # Length
    result = converter.convert('Length([Name])')
    assert 'LENGTH' in result, f"Expected LENGTH, got {result}"

    # Replace
    result = converter.convert('Replace([Text], "old", "new")')
    assert 'REPLACE' in result, f"Expected REPLACE, got {result}"

    # Contains
    result = converter.convert('Contains([Text], "search")')
    assert 'STRPOS' in result and '> 0' in result, f"Expected STRPOS > 0, got {result}"

    print("[PASS] String function conversions work correctly")


def test_math_functions():
    """Test math function conversions."""
    converter = FormulaConverter()

    # Abs
    result = converter.convert('Abs([Value])')
    assert 'ABS' in result, f"Expected ABS, got {result}"

    # Ceil
    result = converter.convert('Ceil([Value])')
    assert 'CEIL' in result, f"Expected CEIL, got {result}"

    # Floor
    result = converter.convert('Floor([Value])')
    assert 'FLOOR' in result, f"Expected FLOOR, got {result}"

    # Round
    result = converter.convert('Round([Value], 2)')
    assert 'ROUND' in result, f"Expected ROUND, got {result}"

    # Sqrt
    result = converter.convert('Sqrt([Value])')
    assert 'SQRT' in result, f"Expected SQRT, got {result}"

    print("[PASS] Math function conversions work correctly")


def test_date_functions():
    """Test date/time function conversions."""
    converter = FormulaConverter()

    # DateTimeNow
    result = converter.convert('DateTimeNow()')
    assert 'CURRENT_TIMESTAMP' in result, f"Expected CURRENT_TIMESTAMP, got {result}"

    # DateTimeYear
    result = converter.convert('DateTimeYear([Date])')
    assert 'YEAR' in result, f"Expected YEAR, got {result}"

    # DateTimeMonth
    result = converter.convert('DateTimeMonth([Date])')
    assert 'MONTH' in result, f"Expected MONTH, got {result}"

    # DateTimeDay
    result = converter.convert('DateTimeDay([Date])')
    assert 'DAY' in result, f"Expected DAY, got {result}"

    print("[PASS] Date function conversions work correctly")


def test_conversion_functions():
    """Test type conversion function conversions."""
    converter = FormulaConverter()

    # ToNumber
    result = converter.convert('ToNumber([StringField])')
    assert 'CAST' in result and 'DOUBLE' in result, f"Expected CAST AS DOUBLE, got {result}"

    # ToString
    result = converter.convert('ToString([NumField])')
    assert 'CAST' in result and 'VARCHAR' in result, f"Expected CAST AS VARCHAR, got {result}"

    # ToInteger
    result = converter.convert('ToInteger([StringField])')
    assert 'CAST' in result and 'BIGINT' in result, f"Expected CAST AS BIGINT, got {result}"

    print("[PASS] Conversion function conversions work correctly")


def test_min_max_functions():
    """Test Min/Max function conversions."""
    converter = FormulaConverter()

    # Min
    result = converter.convert('Min([A], [B], [C])')
    assert 'LEAST' in result, f"Expected LEAST, got {result}"

    # Max
    result = converter.convert('Max([A], [B], [C])')
    assert 'GREATEST' in result, f"Expected GREATEST, got {result}"

    print("[PASS] Min/Max function conversions work correctly")


def test_coalesce():
    """Test Coalesce function conversion."""
    converter = FormulaConverter()

    result = converter.convert('Coalesce([A], [B], "default")')
    assert 'COALESCE' in result, f"Expected COALESCE, got {result}"

    print("[PASS] Coalesce conversion works correctly")


def test_complex_expression():
    """Test conversion of complex nested expressions."""
    converter = FormulaConverter()

    # Complex expression with multiple functions
    expr = 'IIF(IsNull([Amount]), 0, Round([Amount] * 1.1, 2))'
    result = converter.convert(expr)

    assert 'CASE WHEN' in result, f"Expected CASE WHEN, got {result}"
    assert 'IS NULL' in result, f"Expected IS NULL, got {result}"
    assert 'ROUND' in result, f"Expected ROUND, got {result}"

    print("[PASS] Complex expression conversion works correctly")


def test_aggregation_conversion():
    """Test aggregation function conversions."""
    # Sum
    result = convert_aggregation('Sum', '"Amount"')
    assert 'SUM("Amount")' == result, f"Expected SUM, got {result}"

    # Count
    result = convert_aggregation('Count', '"ID"')
    assert 'COUNT("ID")' == result, f"Expected COUNT, got {result}"

    # CountDistinct
    result = convert_aggregation('CountDistinct', '"CustomerID"')
    assert 'COUNT(DISTINCT "CustomerID")' == result, f"Expected COUNT DISTINCT, got {result}"

    # Avg
    result = convert_aggregation('Avg', '"Price"')
    assert 'AVG("Price")' == result, f"Expected AVG, got {result}"

    print("[PASS] Aggregation conversions work correctly")


def test_convenience_function():
    """Test the convenience convert_alteryx_expression function."""
    result = convert_alteryx_expression('[Field1] + [Field2]')
    assert '"Field1"' in result and '"Field2"' in result, f"Got {result}"

    print("[PASS] Convenience function works correctly")


def run_all_tests():
    """Run all tests."""
    print("Testing Alteryx formula converter...\n")

    test_field_references()
    test_operator_conversion()
    test_iif_conversion()
    test_isnull_conversion()
    test_isempty_conversion()
    test_string_functions()
    test_math_functions()
    test_date_functions()
    test_conversion_functions()
    test_min_max_functions()
    test_coalesce()
    test_complex_expression()
    test_aggregation_conversion()
    test_convenience_function()

    print("\n" + "=" * 50)
    print("All formula converter tests passed!")
    print("=" * 50)


if __name__ == '__main__':
    run_all_tests()
