"""
Quality validation module for parallel testing between Alteryx and DBT outputs.

Provides validation mechanisms to compare outputs during migration:
- Record count comparison
- Data point quantities
- Null value completeness per field
- Layer-to-layer validation (bronze vs raw, silver vs staging, gold vs fed)

Target Platform: Starburst (Trino-based)
"""
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime
from pathlib import Path


@dataclass
class ColumnValidation:
    """Validation results for a single column."""
    column_name: str
    alteryx_null_count: int = 0
    dbt_null_count: int = 0
    alteryx_distinct_count: int = 0
    dbt_distinct_count: int = 0
    null_count_match: bool = True
    distinct_count_match: bool = True
    data_type_match: bool = True
    notes: str = ""


@dataclass
class TableValidation:
    """Validation results for a table/model comparison."""
    table_name: str
    alteryx_source: str  # Path to Alteryx output or YXDB file
    dbt_model: str       # DBT model name
    layer: str           # bronze, silver, gold

    # Record counts
    alteryx_record_count: int = 0
    dbt_record_count: int = 0
    record_count_match: bool = True
    record_count_diff: int = 0
    record_count_diff_pct: float = 0.0

    # Column validations
    column_validations: List[ColumnValidation] = field(default_factory=list)

    # Overall status
    validation_passed: bool = True
    validation_timestamp: str = ""
    notes: str = ""


@dataclass
class ValidationReport:
    """Complete validation report for a migration."""
    report_name: str
    generated_at: str = ""
    total_tables_validated: int = 0
    tables_passed: int = 0
    tables_failed: int = 0

    # Layer summaries
    bronze_validations: List[TableValidation] = field(default_factory=list)
    silver_validations: List[TableValidation] = field(default_factory=list)
    gold_validations: List[TableValidation] = field(default_factory=list)

    # Discrepancies found
    discrepancies: List[str] = field(default_factory=list)


class QualityValidator:
    """
    Generates validation SQL and tests for parallel comparison
    between Alteryx and DBT outputs.
    """

    def __init__(self, output_dir: str, catalog: str = "hive",
                 raw_schema: str = "raw", staging_schema: str = "staging"):
        """
        Initialize the quality validator.

        Args:
            output_dir: Directory for validation outputs
            catalog: Trino catalog name
            raw_schema: Schema for raw/bronze data
            staging_schema: Schema for staged data
        """
        self.output_dir = Path(output_dir)
        self.catalog = catalog
        self.raw_schema = raw_schema
        self.staging_schema = staging_schema
        self.validations: List[TableValidation] = []

    def generate_validation_tests(self, models_info: Dict[str, Any]) -> List[str]:
        """
        Generate DBT test files for validation.

        Args:
            models_info: Dictionary of model information from DBTGenerator

        Returns:
            List of generated test file paths
        """
        test_files = []
        tests_dir = self.output_dir / "tests" / "validation"
        tests_dir.mkdir(parents=True, exist_ok=True)

        for model_name, info in models_info.items():
            layer = info.layer if hasattr(info, 'layer') else 'silver'

            # Generate record count test
            count_test = self._generate_record_count_test(model_name, layer)
            count_test_path = tests_dir / f"validate_count_{model_name}.sql"
            count_test_path.write_text(count_test)
            test_files.append(str(count_test_path))

            # Generate null completeness test
            if hasattr(info, 'columns') and info.columns:
                null_test = self._generate_null_completeness_test(
                    model_name, info.columns, layer
                )
                null_test_path = tests_dir / f"validate_nulls_{model_name}.sql"
                null_test_path.write_text(null_test)
                test_files.append(str(null_test_path))

        return test_files

    def _generate_record_count_test(self, model_name: str, layer: str) -> str:
        """Generate a DBT test to validate record counts."""
        # Determine the comparison source based on layer
        if layer == "bronze":
            comparison_note = "raw Nasuni files"
        elif layer == "silver":
            comparison_note = "staging/bronze layer"
        else:
            comparison_note = "fed/production layer"

        return f'''-- Validation test: Record count for {model_name}
-- Compare DBT output against {comparison_note}
-- Layer: {layer}

-- This test returns rows when there's a count mismatch
-- A passing test returns 0 rows

WITH dbt_counts AS (
    SELECT
        '{model_name}' AS model_name,
        COUNT(*) AS record_count,
        CURRENT_TIMESTAMP AS validation_timestamp
    FROM {{{{ ref('{model_name}') }}}}
),

-- TODO: Replace this CTE with actual Alteryx output comparison
-- Option 1: External table pointing to Alteryx YXDB output
-- Option 2: Staging table with Alteryx results
-- Option 3: Direct comparison to source files
alteryx_counts AS (
    SELECT
        '{model_name}' AS model_name,
        -- TODO: Replace with actual Alteryx output source
        -- Example: COUNT(*) FROM {{{{ source('alteryx_outputs', '{model_name}') }}}}
        0 AS record_count,  -- Placeholder
        CURRENT_TIMESTAMP AS validation_timestamp
),

comparison AS (
    SELECT
        d.model_name,
        d.record_count AS dbt_count,
        a.record_count AS alteryx_count,
        ABS(d.record_count - a.record_count) AS count_difference,
        CASE
            WHEN a.record_count = 0 THEN 0
            ELSE ROUND(100.0 * ABS(d.record_count - a.record_count) / a.record_count, 2)
        END AS difference_pct,
        d.validation_timestamp
    FROM dbt_counts d
    CROSS JOIN alteryx_counts a
)

-- Return rows only if counts don't match (test fails)
SELECT *
FROM comparison
WHERE count_difference > 0
'''

    def _generate_null_completeness_test(self, model_name: str,
                                          columns: List[str], layer: str) -> str:
        """Generate a DBT test to validate null completeness per column."""
        # Build column null count expressions
        null_counts = []
        for col in columns:
            safe_col = f'"{col}"' if not col.startswith('"') else col
            null_counts.append(
                f"SUM(CASE WHEN {safe_col} IS NULL THEN 1 ELSE 0 END) AS null_count_{col.replace(' ', '_').lower()}"
            )

        null_counts_sql = ",\n        ".join(null_counts)

        return f'''-- Validation test: Null completeness for {model_name}
-- Compare null counts between DBT and Alteryx outputs
-- Layer: {layer}

-- This test returns rows when there's a null count mismatch
-- A passing test returns 0 rows

WITH dbt_null_counts AS (
    SELECT
        '{model_name}' AS model_name,
        COUNT(*) AS total_records,
        {null_counts_sql}
    FROM {{{{ ref('{model_name}') }}}}
)

-- TODO: Add comparison CTE with Alteryx output null counts
-- Then compare and return mismatches

SELECT
    model_name,
    total_records,
    'Review null counts and compare with Alteryx output' AS validation_note
FROM dbt_null_counts
WHERE 1=0  -- Placeholder: modify to actual comparison logic
'''

    def generate_validation_macro(self) -> str:
        """Generate a reusable DBT macro for validation comparisons."""
        return '''{% macro validate_record_counts(dbt_model, alteryx_source, tolerance_pct=0) %}
{#
    Macro to compare record counts between DBT model and Alteryx source.

    Args:
        dbt_model: Name of the DBT model to validate
        alteryx_source: Source reference for Alteryx output
        tolerance_pct: Acceptable percentage difference (default 0 = exact match)

    Returns:
        Query that returns mismatches (empty = validation passed)
#}

WITH dbt_counts AS (
    SELECT COUNT(*) AS record_count
    FROM {{ ref(dbt_model) }}
),

alteryx_counts AS (
    SELECT COUNT(*) AS record_count
    FROM {{ alteryx_source }}
),

comparison AS (
    SELECT
        '{{ dbt_model }}' AS model_name,
        d.record_count AS dbt_count,
        a.record_count AS alteryx_count,
        ABS(d.record_count - a.record_count) AS count_diff,
        CASE
            WHEN a.record_count = 0 THEN 100.0
            ELSE 100.0 * ABS(d.record_count - a.record_count) / a.record_count
        END AS diff_pct
    FROM dbt_counts d
    CROSS JOIN alteryx_counts a
)

SELECT *
FROM comparison
WHERE diff_pct > {{ tolerance_pct }}

{% endmacro %}


{% macro validate_null_completeness(dbt_model, column_name, alteryx_source) %}
{#
    Macro to compare null counts for a specific column.

    Args:
        dbt_model: Name of the DBT model to validate
        column_name: Column to check for null completeness
        alteryx_source: Source reference for Alteryx output

    Returns:
        Query that returns mismatches (empty = validation passed)
#}

WITH dbt_nulls AS (
    SELECT
        COUNT(*) AS total_records,
        SUM(CASE WHEN {{ column_name }} IS NULL THEN 1 ELSE 0 END) AS null_count
    FROM {{ ref(dbt_model) }}
),

alteryx_nulls AS (
    SELECT
        COUNT(*) AS total_records,
        SUM(CASE WHEN {{ column_name }} IS NULL THEN 1 ELSE 0 END) AS null_count
    FROM {{ alteryx_source }}
),

comparison AS (
    SELECT
        '{{ dbt_model }}' AS model_name,
        '{{ column_name }}' AS column_name,
        d.null_count AS dbt_null_count,
        a.null_count AS alteryx_null_count,
        ABS(d.null_count - a.null_count) AS null_diff
    FROM dbt_nulls d
    CROSS JOIN alteryx_nulls a
)

SELECT *
FROM comparison
WHERE null_diff > 0

{% endmacro %}


{% macro generate_validation_report(models) %}
{#
    Macro to generate a comprehensive validation report.

    Args:
        models: List of model names to validate

    Returns:
        Combined validation results
#}

{% for model in models %}
SELECT
    '{{ model }}' AS model_name,
    'record_count' AS validation_type,
    CASE WHEN count_diff = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    count_diff AS difference,
    CURRENT_TIMESTAMP AS validated_at
FROM (
    SELECT ABS(
        (SELECT COUNT(*) FROM {{ ref(model) }}) -
        -- TODO: Replace with Alteryx source count
        0
    ) AS count_diff
)
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}

{% endmacro %}
'''

    def generate_validation_documentation(self, report: ValidationReport) -> str:
        """Generate markdown documentation for validation results."""
        doc = [
            "# Migration Validation Report",
            "",
            f"**Generated:** {report.generated_at or datetime.now().isoformat()}",
            f"**Report Name:** {report.report_name}",
            "",
            "## Summary",
            "",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Total Tables Validated | {report.total_tables_validated} |",
            f"| Tables Passed | {report.tables_passed} |",
            f"| Tables Failed | {report.tables_failed} |",
            f"| Pass Rate | {100 * report.tables_passed / max(report.total_tables_validated, 1):.1f}% |",
            "",
            "## Validation Approach",
            "",
            "This validation follows a parallel testing strategy comparing:",
            "",
            "1. **Bronze Layer** (dbt external tables) vs **Raw Layer** (Nasuni raw files)",
            "2. **Silver Layer** (dbt intermediate) vs **Staging** (Alteryx staging outputs)",
            "3. **Gold Layer** (dbt marts) vs **Fed Layer** (Alteryx final outputs)",
            "",
            "### Metrics Compared",
            "",
            "- **Record Counts**: Total number of rows in each dataset",
            "- **Data Point Quantities**: Sum/count of key numeric fields",
            "- **Null Completeness**: Null count per output field",
            "",
        ]

        # Add layer-specific results
        for layer_name, validations in [
            ("Bronze", report.bronze_validations),
            ("Silver", report.silver_validations),
            ("Gold", report.gold_validations)
        ]:
            if validations:
                doc.extend([
                    f"## {layer_name} Layer Validations",
                    "",
                    "| Table | DBT Count | Alteryx Count | Diff | Status |",
                    "|-------|-----------|---------------|------|--------|",
                ])
                for v in validations:
                    status = "PASS" if v.validation_passed else "FAIL"
                    doc.append(
                        f"| {v.table_name} | {v.dbt_record_count:,} | "
                        f"{v.alteryx_record_count:,} | {v.record_count_diff:,} | {status} |"
                    )
                doc.append("")

        # Add discrepancies section
        if report.discrepancies:
            doc.extend([
                "## Discrepancies Found",
                "",
                "The following issues need resolution before cut-over:",
                "",
            ])
            for i, disc in enumerate(report.discrepancies, 1):
                doc.append(f"{i}. {disc}")
            doc.append("")

        # Add next steps
        doc.extend([
            "## Next Steps",
            "",
            "1. Review any FAIL status validations above",
            "2. Investigate record count discrepancies",
            "3. Check null completeness differences",
            "4. Update DBT models or Alteryx workflows as needed",
            "5. Re-run validation until all tests pass",
            "",
            "## Running Validations",
            "",
            "```bash",
            "# Run all validation tests",
            "dbt test --select tag:validation",
            "",
            "# Run specific layer validations",
            "dbt test --select tag:validation_bronze",
            "dbt test --select tag:validation_silver",
            "dbt test --select tag:validation_gold",
            "```",
            "",
        ])

        return "\n".join(doc)

    def write_validation_outputs(self, dbt_output_dir: Path,
                                  models_info: Dict[str, Any]) -> List[str]:
        """
        Write all validation artifacts to the DBT project.

        Args:
            dbt_output_dir: Root of DBT project
            models_info: Model information from DBTGenerator

        Returns:
            List of created file paths
        """
        created_files = []

        # Create tests directory
        tests_dir = dbt_output_dir / "tests" / "validation"
        tests_dir.mkdir(parents=True, exist_ok=True)

        # Generate validation tests for each model
        test_files = self.generate_validation_tests(models_info)
        created_files.extend(test_files)

        # Generate validation macros
        macros_dir = dbt_output_dir / "macros" / "validation"
        macros_dir.mkdir(parents=True, exist_ok=True)

        macro_content = self.generate_validation_macro()
        macro_path = macros_dir / "validation_macros.sql"
        macro_path.write_text(macro_content)
        created_files.append(str(macro_path))

        # Generate validation schema
        schema_content = self._generate_validation_schema()
        schema_path = tests_dir / "_validation_schema.yml"
        schema_path.write_text(schema_content)
        created_files.append(str(schema_path))

        return created_files

    def _generate_validation_schema(self) -> str:
        """Generate schema.yml for validation tests."""
        return '''version: 2

# Validation test configuration
# These tests compare DBT outputs against Alteryx outputs
# during the parallel validation period

# Tag all validation tests for easy selection
# Run with: dbt test --select tag:validation

seeds:
  - name: alteryx_expected_counts
    description: "Expected record counts from Alteryx outputs for validation"
    config:
      tags: ['validation']
    columns:
      - name: model_name
        description: "Name of the DBT model being validated"
      - name: expected_count
        description: "Expected record count from Alteryx"
      - name: layer
        description: "Medallion layer (bronze, silver, gold)"
      - name: validation_date
        description: "Date of the Alteryx run being compared"

# Example seed data file: seeds/alteryx_expected_counts.csv
# model_name,expected_count,layer,validation_date
# stg_customers,10000,bronze,2024-01-15
# int_customer_orders,8500,silver,2024-01-15
# fct_daily_sales,365,gold,2024-01-15
'''


def create_validation_seed_template(output_dir: Path) -> str:
    """Create a template CSV for Alteryx expected counts."""
    seeds_dir = output_dir / "seeds"
    seeds_dir.mkdir(parents=True, exist_ok=True)

    content = '''model_name,expected_count,layer,validation_date,notes
# Add your Alteryx output counts below
# stg_customers,10000,bronze,2024-01-15,Customer master data
# int_customer_orders,8500,silver,2024-01-15,Joined customer orders
# fct_daily_sales,365,gold,2024-01-15,Daily aggregated sales
'''

    seed_path = seeds_dir / "alteryx_expected_counts.csv"
    seed_path.write_text(content)
    return str(seed_path)
