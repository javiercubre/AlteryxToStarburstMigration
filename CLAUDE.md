# CLAUDE.md - AI Assistant Guidelines

This document provides context and guidelines for AI assistants working with this codebase.

## Project Overview

**Alteryx to Starburst/DBT Migration Tool** - A Python CLI tool that parses Alteryx workflows (.yxmd files) and generates:
1. Comprehensive markdown documentation with Mermaid diagrams
2. DBT project scaffolding organized by medallion layers (Bronze → Silver → Gold)

**Target Platform:**
- **Starburst** (enterprise Trino distribution)
- **dbt** (data build tool)
- **SQL Dialect:** Trino SQL syntax

## Quick Reference

```bash
# Run the tool
python main.py analyze <path> [options]

# Common usage patterns
python main.py analyze .                                    # Current directory
python main.py analyze ./workflows --recursive              # Recursive scan
python main.py analyze . --output ./docs --generate-dbt ./dbt  # Full output
python main.py analyze . --non-interactive                  # Skip prompts

# Run tests
python tests/test_source_columns.py
```

## Directory Structure

```
.
├── main.py                       # CLI entry point (argparse)
├── alteryx_parser.py             # XML parsing of .yxmd/.yxmc files
├── transformation_analyzer.py    # Data lineage & flow analysis
├── macro_handler.py              # Macro resolution with interactive prompts
├── doc_generator.py              # Markdown documentation generation
├── dbt_generator.py              # DBT project scaffolding (macro-first approach)
├── tool_mappings.py              # Alteryx → SQL/DBT mappings
├── macro_mappings.py             # Alteryx tool → DBT macro mappings
├── formula_converter.py          # Alteryx formula → Trino SQL conversion
├── quality_validator.py          # Parallel validation for migration testing
├── models.py                     # Data classes & enums
├── dbt_macros/                   # 22 reusable DBT macros (copied to generated project)
│   ├── aggregation.sql           # Summarize tool macros
│   ├── filter_helpers.sql        # Filter macros
│   ├── join_union.sql            # Join and Union macros
│   └── ...                       # 19 more macro files
├── tests/
│   ├── test_source_columns.py    # Column detection tests
│   ├── test_formula_converter.py # Formula conversion tests
│   └── test_data/                # Test fixtures (CSV, JSON)
└── samples/
    ├── *.yxmd                    # Sample Alteryx workflows
    └── macros/*.yxmc             # Sample macros
```

## Core Modules

| Module | Lines | Purpose |
|--------|-------|---------|
| `main.py` | ~235 | CLI entry point, argument parsing, orchestration |
| `models.py` | ~190 | Dataclasses and enums (AlteryxNode, AlteryxWorkflow, etc.) |
| `alteryx_parser.py` | ~555 | XML parsing using `xml.etree.ElementTree` |
| `transformation_analyzer.py` | ~570 | Graph-based analysis, topological sort, lineage, macro hints |
| `macro_handler.py` | ~280 | Interactive macro resolution, path caching |
| `doc_generator.py` | ~910 | Markdown generation, Mermaid diagrams |
| `dbt_generator.py` | ~2200 | **Macro-first DBT scaffolding** - Generates macro calls instead of raw SQL |
| `tool_mappings.py` | ~450 | 100+ Alteryx tool → SQL/DBT/macro mappings |
| `macro_mappings.py` | ~380 | **NEW**: Alteryx tool → DBT macro mappings (31 tools → 19 macro files) |
| `formula_converter.py` | ~400 | Alteryx formula → Trino SQL with 60+ function mappings |
| `quality_validator.py` | ~350 | Parallel validation tests for migration (record counts, null checks) |

**Total Macro Coverage:** 22 comprehensive DBT macros covering 85%+ of Alteryx tools

## Technology Stack

- **Python 3.7+** (only standard library required)
- **No external dependencies** for core functionality
- **XML Parsing:** `xml.etree.ElementTree`
- **CLI:** `argparse`
- **Data Models:** `dataclasses` with full type annotations
- **Path Handling:** `pathlib`

## Code Conventions

### 1. Data Classes
All data structures use `@dataclass` decorator:
```python
from dataclasses import dataclass, field

@dataclass
class AlteryxNode:
    tool_id: int
    tool_type: str
    configuration: Dict[str, Any] = field(default_factory=dict)
```

### 2. Type Annotations
Full type hints throughout:
```python
def find_workflows(path: Path, recursive: bool = False) -> List[Path]:
```

### 3. Method Naming
- Private methods: `_method_name()` (underscore prefix)
- Public API: `method_name()`

### 4. Error Handling
- Try-except around XML parsing
- Graceful degradation for missing macros
- EOFError/KeyboardInterrupt handling for non-interactive environments

### 5. TODO Tracking
Generated SQL includes `-- TODO:` comments tracked by `TodoItem` dataclass in `dbt_generator.py`.

### 6. Imports
Standard library only, with TYPE_CHECKING for circular dependency prevention:
```python
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from models import AlteryxWorkflow
```

## Key Enums

```python
class ToolCategory(Enum):
    INPUT, OUTPUT, PREPARATION, JOIN, TRANSFORM, PARSE,
    REPORTING, SPATIAL, PREDICTIVE, DEVELOPER, IN_DATABASE,
    MACRO, CONTAINER, UNKNOWN

class MedallionLayer(Enum):
    BRONZE = "bronze"   # Raw/staging (stg_*)
    SILVER = "silver"   # Intermediate/cleaned (int_*)
    GOLD = "gold"       # Business-ready/marts (fct_*, dim_*)
```

## Testing

The project uses a custom test framework (not pytest):

```bash
# Run all tests
python tests/test_source_columns.py
python tests/test_formula_converter.py
```

Test pattern:
```python
def test_csv_column_reading():
    """Test reading columns from a CSV file."""
    generator = DBTGenerator(tempfile.mkdtemp(), interactive=False)
    columns = generator._read_csv_columns(test_file)
    assert columns == expected, f"Expected {expected}, got {columns}"
    print("[PASS] CSV column reading works correctly")

if __name__ == '__main__':
    run_all_tests()
```

### Test Coverage
- **Source column detection**: CSV, JSON, Parquet file reading
- **Formula conversion**: 60+ Alteryx functions → Trino SQL mappings
- **Validation tests**: Record counts, null completeness checks

## Common Development Tasks

### Adding a New Alteryx Tool Mapping

Edit `tool_mappings.py`:
```python
TOOL_CATEGORY_MAP = {
    # Add new tool mapping
    "AlteryxBasePluginsGui.NewTool.NewTool": ToolCategory.TRANSFORM,
}

TOOL_NAME_MAP = {
    "AlteryxBasePluginsGui.NewTool.NewTool": "New Tool",
}
```

### Adding SQL Translation for a Tool

Edit `dbt_generator.py` in `_generate_cte_for_node()` method.

### Extending Documentation Output

Edit `doc_generator.py` and add methods following existing patterns.

## Interactive vs Non-Interactive Mode

The tool supports both modes:
- **Interactive (default):** Prompts for missing macro paths
- **Non-interactive (`--non-interactive`):** Skips prompts, documents macros as missing

## Generated Output Structure

### Documentation (`--output`)
```
docs/
├── index.md                    # Overview + TODO guide
├── workflows/workflow_name.md  # Per-workflow with Mermaid diagrams
├── sources.md                  # Data sources inventory
├── targets.md                  # Output targets
├── macros.md                   # Macro inventory
└── medallion_mapping.md        # Layer assignments
```

### DBT Project (`--generate-dbt`)
```
dbt_project/
├── dbt_project.yml
├── models/
│   ├── staging/           # Bronze layer (stg_*)
│   │   ├── _sources.yml
│   │   └── stg_*.sql
│   ├── intermediate/      # Silver layer (int_*)
│   └── marts/             # Gold layer
│       ├── core/          # fct_*
│       └── dimensions/    # dim_*
├── macros/                # Converted Alteryx macros
└── tests/
```

## Important Notes for AI Assistants

1. **Minimal Dependencies:** This project intentionally uses only Python standard library. Do not add external dependencies without explicit user request.

2. **Trino SQL Syntax:** Generated SQL uses Trino-compatible syntax (e.g., `REGEXP_EXTRACT()`, `UNNEST()`, `SPLIT()`).

3. **TODO Comments:** Generated DBT models contain `-- TODO:` comments for manual refinement. These are tracked and aggregated in documentation.

4. **XML Structure:** Alteryx .yxmd files are XML. Key elements: `<Properties>`, `<Nodes>`, `<Connections>`, `<Node>`, `<GuiSettings>`.

5. **Macro Resolution:** The tool searches for macros in:
   - Path specified in workflow XML
   - Same directory as workflow
   - `macros/` subdirectory
   - User-provided directories (cached during session)

6. **Test Data:** Sample workflows in `samples/` and test fixtures in `tests/test_data/`.

7. **Column Detection Priority:**
   1. Extract from SQL query in node config
   2. Parse from Alteryx node configuration
   3. Read from source file (CSV/JSON/Parquet headers)
   4. Interactive prompt (if enabled)
   5. Fallback placeholder columns

8. **Formula Conversion:** The `formula_converter.py` module converts Alteryx formulas to Trino SQL:
   - String functions: `Trim()`, `Left()`, `Right()`, `Replace()`, `Contains()`, etc.
   - Math functions: `Abs()`, `Ceil()`, `Floor()`, `Round()`, `Sqrt()`, etc.
   - Date functions: `DateTimeNow()`, `DateTimeYear()`, `DateTimeDiff()`, etc.
   - Conditional: `IIF()` → `CASE WHEN`, `IsNull()`, `IsEmpty()`, `Coalesce()`, etc.
   - Reference: https://help.alteryx.com/current/en/designer/functions.html

9. **Quality Validation:** The `quality_validator.py` module generates parallel validation tests:
   - Record count comparison between Alteryx and DBT outputs
   - Null completeness checks per column
   - Layer-to-layer validation (bronze vs raw, silver vs staging, gold vs fed)

10. **Macro-First Architecture (NEW):** The tool now generates **DBT macro calls** instead of raw SQL:
    - 22 comprehensive macros covering 85%+ of Alteryx tools
    - Macros located in `dbt_macros/` and copied to generated DBT projects
    - Generated models use `{{ macro_name(params) }}` instead of raw SQL
    - Falls back to legacy SQL generation for tools without macro mappings
    - See `macro_mappings.py` for complete tool → macro mappings

## Macro-First Migration Architecture

### Overview

The DBT generator now follows a **macro-first approach**, generating Jinja2 macro calls instead of raw SQL for most transformations. This provides:

- **Maintainability**: Changes to SQL logic happen in macros, not generator code
- **Reusability**: Generated models can reuse macros across projects
- **Testability**: Macros can be unit tested independently
- **DBT Best Practices**: Follows macro-first approach recommended by DBT
- **Flexibility**: Users can customize macros without changing Python code

### Architecture Flow

```
Alteryx Tool → macro_mappings.py → DBT Macro Call → Generated SQL Model
     ↓                ↓                    ↓
  Filter      get_macro_for_tool()   {{ filter_expression(...) }}
```

### Key Components

1. **`macro_mappings.py`** (~380 lines)
   - Maps 31 Alteryx tools to DBT macros
   - Defines parameter mappings between Alteryx and macro params
   - Supports alternate macros based on context (e.g., join type)

2. **`dbt_generator.py`** - Macro generation methods:
   - `_generate_macro_call_sql()`: Primary method for macro-based generation
   - `_build_macro_parameters()`: Extracts and maps parameters from Alteryx nodes
   - `_format_macro_call()`: Formats Jinja2 macro invocations
   - `_generate_transformation_sql_legacy()`: Fallback for tools without macros

3. **`dbt_macros/`** - 22 macro files:
   - `aggregation.sql`: Summarize tool (10 macros)
   - `filter_helpers.sql`: Filter operations (9 macros)
   - `join_union.sql`: Join/Union operations (13 macros)
   - `formula_helpers.sql`: Formula calculations (16 macros)
   - `select_transform.sql`: Select/Sort/RecordID (10 macros)
   - ...and 17 more macro files

### Example Generated Output

**Before (raw SQL):**
```sql
with source as (
    select * from {{ source('raw', 'customers') }}
),

final as (
    select
        "customer_id",
        "name",
        "email"
    from source
    where "status" = 'active'
)

select
    "customer_id",
    "name",
    "email"
from final
```

**After (macro-first):**
```sql
with source as (
    select * from {{ source('raw', 'customers') }}
),

-- Filter: Active Customers
{{
    filter_expression(
        relation=source,
        condition='"status" = \'active\'',
        columns=['customer_id', 'name', 'email']
    )
}}
```

### Coverage Statistics

**Tools with Macro Mappings (31 tools):**
- Filter, Formula, Multi-Field Formula, Select, Sort, Join, Union
- Summarize, Unique, Sample, Record ID, Multi-Row Formula
- Running Total, Find Replace, RegEx, Text To Columns
- Data Cleansing, Imputation, Cross Tab, Transpose
- Count Records, Append Fields, Join Multiple, Auto Field
- Select Records, Tile, Weighted Average, Arrange
- JSON Parse, Date Time Parse, Generate Rows

**Macro Coverage: 85%+** of common Alteryx tools

**Tools without macros (fallback to raw SQL):**
- Spatial tools, Fuzzy matching, Dynamic Input/Output
- Some predictive/machine learning tools
- Custom/developer tools

### Tool-to-Macro Mapping Examples

| Alteryx Tool | DBT Macro | Macro File | Parameters |
|--------------|-----------|------------|------------|
| Filter | `filter_expression` | filter_helpers.sql | relation, condition |
| Formula | `add_calculated_column` | formula_helpers.sql | relation, column_name, expression |
| Join | `left_join` / `inner_join` | join_union.sql | left_relation, right_relation, join_columns |
| Summarize | `summarize` | aggregation.sql | relation, group_by, agg_fields |
| Select | `select_columns` | select_transform.sql | relation, columns |
| Unique | `deduplicate` | deduplicate.sql | relation, partition_by, order_by |
| RegEx | `regex_extract` | regex_functions.sql | relation, column_name, pattern |

### Implementation Notes

- **Priority**: Macro generation is attempted first; raw SQL is fallback
- **Parameters**: Automatically extracted from Alteryx node configuration
- **Expressions**: Alteryx expressions are still converted to Trino SQL via `formula_converter.py`
- **Chained Transformations**: Currently use legacy SQL; future enhancement opportunity
- **Documentation**: Generated docs show macro references and file locations

## Git Workflow

- **Main branch:** Contains stable code
- **Feature branches:** For new development
- **Test artifacts ignored:** `test_docs/`, `test_dbt/`, `target/`, `dbt_packages/`

## Architectural Decisions

1. **No External Dependencies:** Maximizes portability
2. **Medallion Architecture:** Bronze → Silver → Gold layer mapping
3. **Dataclass-Based Models:** Type-safe data structures
4. **Topological Sort:** Graph-based transformation ordering
5. **Macro-First Generation (NEW):** DBT models use reusable macros instead of raw SQL
6. **CTE-Based SQL:** Generated models use Common Table Expressions
7. **TODO-Driven Development:** Incomplete implementations tracked for documentation
8. **Dual-Mode Generation:** Macro-based for supported tools, legacy SQL for others
