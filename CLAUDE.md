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
├── dbt_generator.py              # DBT project scaffolding (largest module)
├── tool_mappings.py              # Alteryx → SQL/DBT mappings
├── models.py                     # Data classes & enums
├── tests/
│   ├── test_source_columns.py    # Column detection tests
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
| `transformation_analyzer.py` | ~570 | Graph-based analysis, topological sort, lineage |
| `macro_handler.py` | ~280 | Interactive macro resolution, path caching |
| `doc_generator.py` | ~910 | Markdown generation, Mermaid diagrams |
| `dbt_generator.py` | ~2385 | DBT scaffolding, SQL generation, column detection |
| `tool_mappings.py` | ~440 | 100+ Alteryx tool → SQL/DBT mappings |

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

## Git Workflow

- **Main branch:** Contains stable code
- **Feature branches:** For new development
- **Test artifacts ignored:** `test_docs/`, `test_dbt/`, `target/`, `dbt_packages/`

## Architectural Decisions

1. **No External Dependencies:** Maximizes portability
2. **Medallion Architecture:** Bronze → Silver → Gold layer mapping
3. **Dataclass-Based Models:** Type-safe data structures
4. **Topological Sort:** Graph-based transformation ordering
5. **CTE-Based SQL:** Generated models use Common Table Expressions
6. **TODO-Driven Development:** Incomplete implementations tracked for documentation
