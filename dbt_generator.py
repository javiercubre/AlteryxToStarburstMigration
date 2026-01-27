"""
DBT project scaffolding generator for Starburst (Trino).
Generates starter DBT models based on Alteryx workflow analysis.

Target Platform: Starburst (Trino-based)
SQL Dialect: Trino SQL
"""
import os
import re
import csv
import json
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass, field

from models import (
    AlteryxWorkflow, AlteryxNode, MedallionLayer, ToolCategory
)
from transformation_analyzer import TransformationAnalyzer
from tool_mappings import get_dbt_prefix, AGGREGATION_MAP
from quality_validator import QualityValidator, create_validation_seed_template
from formula_converter import FormulaConverter, convert_aggregation


@dataclass
class SourceInfo:
    """Information about a data source including columns."""
    schema: str
    table: str
    columns: List[str] = field(default_factory=list)
    description: str = ""
    source_path: str = ""


@dataclass
class ModelInfo:
    """Information about a generated model."""
    name: str
    layer: str  # bronze, silver, gold
    columns: List[str] = field(default_factory=list)
    description: str = ""
    source_tool_id: int = 0


@dataclass
class TodoItem:
    """Represents a TODO instruction in the generated DBT scaffold."""
    file_path: str          # Path to the file containing the TODO
    model_name: str         # Name of the model/macro
    layer: str              # bronze, silver, gold, macro
    todo_type: str          # Type of TODO (columns, transformation, expression, etc.)
    description: str        # Human-readable description of what needs to be done
    context: str = ""       # Additional context (tool name, expression, etc.)
    priority: str = "medium"  # high, medium, low


class DBTGenerator:
    """Generates DBT project structure from Alteryx workflows."""

    def __init__(self, output_dir: str, project_name: str = "alteryx_migration",
                 interactive: bool = True, generate_validation: bool = True):
        self.output_dir = Path(output_dir)
        self.project_name = project_name
        self.interactive = interactive  # Whether to prompt user for missing info
        self.generate_validation = generate_validation  # Generate validation tests
        self.sources: Dict[str, Dict[str, SourceInfo]] = {}  # schema -> {table -> SourceInfo}
        self.models_info: Dict[str, ModelInfo] = {}  # model_name -> ModelInfo
        self.models_generated: List[str] = []
        self.macros_generated: List[str] = []  # Track generated macros
        self.validation_tests_generated: List[str] = []  # Track validation tests
        self.todos: List[TodoItem] = []  # Track all TODO items for documentation
        self._node_columns: Dict[int, List[str]] = {}  # tool_id -> columns (cache)
        self._current_workflow: Optional[AlteryxWorkflow] = None
        self._macro_name_map: Dict[str, str] = {}  # original macro path -> dbt macro name
        self._resolved_source_files: Dict[str, str] = {}  # source key -> resolved file path
        self._current_model_name: str = ""  # Track current model being generated
        self._current_layer: str = ""  # Track current layer
        self._formula_converter = FormulaConverter()  # Alteryx to Trino formula converter

    def generate(self, workflows: List[AlteryxWorkflow], macro_inventory=None) -> None:
        """Generate complete DBT project from workflows.

        Args:
            workflows: List of parsed Alteryx workflows
            macro_inventory: Optional MacroInventory with resolved macro information
        """
        # Create directory structure
        self._create_structure()

        # Generate reusable macros from Alteryx macros
        if macro_inventory:
            self._generate_macros(macro_inventory)

        # Collect all sources with column info
        self._collect_sources(workflows)

        # Generate source definitions
        self._generate_sources_yml()

        # Generate models for each workflow
        for workflow in workflows:
            self._generate_workflow_models(workflow)

        # Generate schema.yml with proper columns
        self._generate_schema_yml()

        # Generate dbt tests
        self._generate_tests()

        # Generate validation tests for parallel testing (Issue #5)
        if self.generate_validation:
            self._generate_validation_tests()

        # Generate dbt_project.yml
        self._generate_project_yml()

        print(f"DBT project generated at: {self.output_dir}")
        print(f"Models generated: {len(self.models_generated)}")
        if self.macros_generated:
            print(f"Macros generated: {len(self.macros_generated)}")
        if self.validation_tests_generated:
            print(f"Validation tests generated: {len(self.validation_tests_generated)}")
        if self.todos:
            print(f"TODOs requiring attention: {len(self.todos)}")

    def _add_todo(self, todo_type: str, description: str, context: str = "",
                  priority: str = "medium") -> None:
        """Track a TODO item that was generated in the scaffold."""
        file_path = ""
        if self._current_layer and self._current_model_name:
            file_path = f"models/{self._current_layer}/{self._current_model_name}.sql"
        elif self._current_model_name:
            file_path = f"macros/{self._current_model_name}.sql"

        self.todos.append(TodoItem(
            file_path=file_path,
            model_name=self._current_model_name,
            layer=self._current_layer or "unknown",
            todo_type=todo_type,
            description=description,
            context=context,
            priority=priority,
        ))

    def get_todos_summary(self) -> Dict:
        """Get a summary of all TODOs for documentation."""
        summary = {
            "total": len(self.todos),
            "by_priority": {"high": 0, "medium": 0, "low": 0},
            "by_layer": {},
            "by_type": {},
            "items": self.todos,
        }

        for todo in self.todos:
            summary["by_priority"][todo.priority] = summary["by_priority"].get(todo.priority, 0) + 1
            summary["by_layer"][todo.layer] = summary["by_layer"].get(todo.layer, 0) + 1
            summary["by_type"][todo.todo_type] = summary["by_type"].get(todo.todo_type, 0) + 1

        return summary

    def _create_structure(self) -> None:
        """Create DBT project directory structure with bronze/silver/gold naming."""
        dirs = [
            self.output_dir,
            self.output_dir / "models",
            self.output_dir / "models" / "bronze",      # Renamed from staging
            self.output_dir / "models" / "silver",      # Renamed from intermediate
            self.output_dir / "models" / "gold",        # Renamed from marts
            self.output_dir / "macros",
            self.output_dir / "macros" / "migration",   # Reusable migration macros
            self.output_dir / "tests",
            self.output_dir / "tests" / "generic",
            self.output_dir / "tests" / "singular",
        ]

        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)

        # Copy reusable migration macros
        self._copy_migration_macros()

    def _copy_migration_macros(self) -> None:
        """Copy reusable DBT macros for common Alteryx migration patterns.

        These macros provide Trino-compatible implementations of common
        Alteryx tool patterns like deduplication, window functions, pivoting, etc.
        """
        import shutil

        # Source directory containing migration macros (relative to this file)
        source_macros_dir = Path(__file__).parent / "dbt_macros"
        target_macros_dir = self.output_dir / "macros" / "migration"

        if not source_macros_dir.exists():
            print(f"Warning: Migration macros directory not found at {source_macros_dir}")
            return

        # Copy all .sql files from dbt_macros to the generated project
        macro_files = list(source_macros_dir.glob("*.sql"))
        for macro_file in macro_files:
            target_file = target_macros_dir / macro_file.name
            shutil.copy2(macro_file, target_file)

        if macro_files:
            print(f"Copied {len(macro_files)} reusable migration macros")

            # Generate an index file documenting the migration macros
            self._generate_migration_macros_index(macro_files)

    def _generate_migration_macros_index(self, macro_files: List[Path]) -> None:
        """Generate documentation index for migration macros."""
        content = [
            "{#",
            "    Migration Macros Index",
            "    =======================",
            "    These macros provide Trino/Starburst compatible implementations",
            "    of common Alteryx transformation patterns.",
            "",
            "    Macros included:",
        ]

        macro_descriptions = {
            "deduplicate.sql": "Remove duplicate rows using ROW_NUMBER (Alteryx: Unique tool)",
            "running_total.sql": "Calculate running/cumulative totals (Alteryx: Multi-Row Formula, Running Total)",
            "window_rank.sql": "Add ranking columns with ROW_NUMBER/RANK/DENSE_RANK (Alteryx: RecordID)",
            "safe_cast.sql": "Safe type casting with TRY_CAST and fallback values (Alteryx: Formula)",
            "split_unnest.sql": "Split delimited strings into rows (Alteryx: Text to Columns, Transpose)",
            "generate_surrogate_key.sql": "Generate hash-based surrogate keys (Alteryx: Formula with MD5)",
            "pivot.sql": "Pivot/CrossTab operations (Alteryx: CrossTab tool)",
            "null_if_empty.sql": "Clean empty strings and standardize nulls (Alteryx: Data Cleansing)",
            "date_spine.sql": "Generate date sequences (Alteryx: Generate Rows)",
            "string_normalize.sql": "String normalization and cleaning (Alteryx: Data Cleansing, Formula)",
        }

        for macro_file in sorted(macro_files):
            desc = macro_descriptions.get(macro_file.name, "Reusable transformation macro")
            content.append(f"    - {macro_file.stem}: {desc}")

        content.extend([
            "",
            "    Usage Example:",
            "        {{ deduplicate(",
            "            relation=ref('stg_customers'),",
            "            partition_by=['customer_id'],",
            "            order_by=['updated_at'],",
            "            order_direction='desc'",
            "        ) }}",
            "#}",
            "",
            "-- This file serves as documentation only.",
            "-- Each macro is defined in its own .sql file in this directory.",
        ])

        index_file = self.output_dir / "macros" / "migration" / "_index.sql"
        self._write_file(index_file, "\n".join(content))

    def _generate_macros(self, macro_inventory) -> None:
        """Generate DBT macros from Alteryx macros for reusability.

        Converts Alteryx macros (.yxmc) into reusable DBT/Jinja macros that can
        be called from multiple models, maintaining the same reusability pattern
        as the original Alteryx macros.
        """
        from models import MacroInfo

        for macro_name, macro_info in macro_inventory.macros.items():
            if not macro_info.found or not macro_info.workflow:
                continue

            # Generate DBT macro name (sanitized)
            dbt_macro_name = self._sanitize_name(Path(macro_name).stem)

            # Store mapping for later reference
            self._macro_name_map[macro_info.file_path] = dbt_macro_name

            # Generate the macro content
            macro_content = self._generate_macro_content(macro_info, dbt_macro_name)

            # Write macro file
            macro_file = self.output_dir / "macros" / f"{dbt_macro_name}.sql"
            self._write_file(macro_file, macro_content)
            self.macros_generated.append(dbt_macro_name)

        # Generate a macros index file documenting all macros
        if self.macros_generated:
            self._generate_macros_yml(macro_inventory)

    def _generate_macro_content(self, macro_info, dbt_macro_name: str) -> str:
        """Generate DBT macro content from an Alteryx macro."""
        workflow = macro_info.workflow
        if not workflow:
            return f"-- Macro: {macro_info.name}\n-- Could not parse macro content\n"

        # Find macro inputs and outputs
        inputs = []
        outputs = []
        transform_nodes = []

        for node in workflow.nodes:
            if node.plugin_name == "Macro Input":
                input_name = node.annotation or node.configuration.get('Name', f'input_{node.tool_id}')
                inputs.append({'name': self._sanitize_name(input_name), 'node': node})
            elif node.plugin_name == "Macro Output":
                output_name = node.annotation or node.configuration.get('Name', f'output_{node.tool_id}')
                outputs.append({'name': self._sanitize_name(output_name), 'node': node})
            elif node.category not in [ToolCategory.INPUT, ToolCategory.OUTPUT]:
                transform_nodes.append(node)

        # Build macro parameters from inputs
        params = [inp['name'] for inp in inputs]
        params_str = ", ".join(params) if params else "source_relation"

        # Generate macro header
        content = [
            f"{{#",
            f"    Macro: {dbt_macro_name}",
            f"    Converted from Alteryx macro: {macro_info.name}",
            f"    Description: {workflow.metadata.description or 'Reusable transformation macro'}",
            f"",
            f"    Inputs: {', '.join(params) if params else 'source_relation'}",
            f"    Outputs: {', '.join([o['name'] for o in outputs]) if outputs else 'transformed data'}",
            f"",
            f"    Usage:",
            f"        {{{{ {dbt_macro_name}(ref('your_source_model')) }}}}",
            f"#}}",
            "",
            f"{{% macro {dbt_macro_name}({params_str}) %}}",
            "",
        ]

        # Generate transformation CTEs
        source_ref = params[0] if params else "source_relation"
        cte_parts = [f"with source as (\n    select * from {{{{ {source_ref} }}}}\n)"]

        # Sort nodes by dependency order
        ordered_nodes = self._get_ordered_transform_nodes(workflow, transform_nodes)

        prev_cte = "source"
        for i, node in enumerate(ordered_nodes):
            cte_name = f"step_{i + 1}" if i < len(ordered_nodes) - 1 else "final"
            cte_sql = self._generate_macro_cte(node, prev_cte, cte_name)
            cte_parts.append(cte_sql)
            prev_cte = cte_name

        # Combine CTEs
        content.append(",\n\n".join(cte_parts))
        content.append("")
        content.append("select * from final")
        content.append("")
        content.append("{% endmacro %}")

        # If there are multiple outputs, generate additional macros for each output
        if len(outputs) > 1:
            content.append("")
            content.append(f"{{# Additional output macros for {dbt_macro_name} #}}")
            for output in outputs:
                output_macro_name = f"{dbt_macro_name}_{output['name']}"
                content.append("")
                content.append(f"{{% macro {output_macro_name}({params_str}) %}}")
                content.append(f"    {{{{ {dbt_macro_name}({source_ref}) }}}}")
                content.append(f"    -- Filter for {output['name']} output")
                content.append("{% endmacro %}")

        return "\n".join(content)

    def _get_ordered_transform_nodes(self, workflow, transform_nodes: List[AlteryxNode]) -> List[AlteryxNode]:
        """Order transformation nodes by dependency."""
        if not transform_nodes:
            return []

        # Build dependency graph
        node_ids = {n.tool_id for n in transform_nodes}
        ordered = []
        visited = set()

        def visit(node):
            if node.tool_id in visited:
                return
            visited.add(node.tool_id)

            # Visit upstream nodes first
            for conn in workflow.connections:
                if conn.destination_id == node.tool_id and conn.origin_id in node_ids:
                    origin_node = workflow.get_node_by_id(conn.origin_id)
                    if origin_node:
                        visit(origin_node)

            ordered.append(node)

        for node in transform_nodes:
            visit(node)

        return ordered

    def _generate_macro_cte(self, node: AlteryxNode, source_cte: str, cte_name: str) -> str:
        """Generate a CTE for a node within a macro."""
        if node.plugin_name == "Filter":
            condition = self._convert_expression(node.expression or "1=1")
            return f"""{cte_name} as (
    -- {node.plugin_name}: {node.annotation or ''}
    select *
    from {source_cte}
    where {condition}
)"""

        elif node.plugin_name in ["Formula", "Multi-Field Formula"]:
            formulas = node.configuration.get('formulas', [])
            if formulas:
                select_parts = ["*"]
                for f in formulas:
                    field = self._quote_column(f.get('field', 'new_field'))
                    expr = self._convert_expression(f.get('expression', 'NULL'))
                    select_parts.append(f"{expr} as {field}")

                return f"""{cte_name} as (
    -- {node.plugin_name}: {node.annotation or ''}
    select
        {','.join(chr(10) + '        ' + p for p in select_parts)}
    from {source_cte}
)"""

        elif node.plugin_name == "Select":
            if node.selected_fields:
                quoted_fields = [self._quote_column(f.split(' AS ')[0].strip()) +
                                (' as ' + self._quote_column(f.split(' AS ')[1].strip()) if ' AS ' in f.upper() else '')
                                for f in node.selected_fields]
                fields = ",\n        ".join(quoted_fields)
                return f"""{cte_name} as (
    -- {node.plugin_name}: {node.annotation or ''}
    select
        {fields}
    from {source_cte}
)"""

        elif node.plugin_name == "Sort":
            sort_fields = node.configuration.get('sort_fields', [])
            if sort_fields:
                order_parts = []
                for sf in sort_fields:
                    direction = "asc" if sf.get('order', 'Ascending') == 'Ascending' else "desc"
                    field = self._quote_column(sf['field'])
                    order_parts.append(f"{field} {direction}")
                order_clause = ", ".join(order_parts)
                return f"""{cte_name} as (
    -- {node.plugin_name}: {node.annotation or ''}
    select *
    from {source_cte}
    order by {order_clause}
)"""

        elif node.plugin_name == "Summarize":
            group_by_quoted = [self._quote_column(f) for f in node.group_by_fields] if node.group_by_fields else []
            group_cols = ", ".join(group_by_quoted) if group_by_quoted else "1"

            agg_parts = []
            for agg in node.aggregations:
                action = agg.get('action', 'COUNT')
                field = agg.get('field', '*')
                output = self._quote_column(agg.get('output_name', field))
                sql_func = AGGREGATION_MAP.get(action, action.upper())
                field_ref = self._quote_column(field) if field != '*' else field

                if sql_func.endswith('(DISTINCT'):
                    agg_parts.append(f"{sql_func} {field_ref}) as {output}")
                else:
                    agg_parts.append(f"{sql_func}({field_ref}) as {output}")

            select_parts = group_by_quoted + agg_parts if group_by_quoted else agg_parts
            select_clause = ",\n        ".join(select_parts) if select_parts else "count(*) as record_count"

            return f"""{cte_name} as (
    -- {node.plugin_name}: {node.annotation or ''}
    select
        {select_clause}
    from {source_cte}
    group by {group_cols}
)"""

        # Default
        return f"""{cte_name} as (
    -- {node.plugin_name}: {node.annotation or ''} (TODO: implement)
    select *
    from {source_cte}
)"""

    def _generate_macros_yml(self, macro_inventory) -> None:
        """Generate a YAML file documenting all macros."""
        content = [
            "# DBT Macros generated from Alteryx macros",
            "# These macros provide reusable SQL transformations",
            "#",
            "# Usage: {{ macro_name(ref('source_model')) }}",
            "",
            "macros:",
        ]

        for macro_name, macro_info in macro_inventory.macros.items():
            if macro_info.found and macro_info.workflow:
                dbt_name = self._macro_name_map.get(macro_info.file_path, self._sanitize_name(macro_name))
                usage_count = len(macro_inventory.usage.get(macro_name, []))

                content.extend([
                    f"  - name: {dbt_name}",
                    f"    description: \"{macro_info.workflow.metadata.description or 'Converted from Alteryx macro'}\"",
                    f"    original_file: \"{macro_info.file_path}\"",
                    f"    used_by_workflows: {usage_count}",
                ])

                if macro_info.inputs:
                    content.append(f"    inputs: {macro_info.inputs}")
                if macro_info.outputs:
                    content.append(f"    outputs: {macro_info.outputs}")
                content.append("")

        self._write_file(
            self.output_dir / "macros" / "_macros.yml",
            "\n".join(content)
        )

    def _collect_sources(self, workflows: List[AlteryxWorkflow]) -> None:
        """Collect all data sources with column information from workflows."""
        for workflow in workflows:
            for node in workflow.sources:
                schema = self._get_schema_name(node)
                table = self._get_table_name(node)

                if schema not in self.sources:
                    self.sources[schema] = {}

                # Extract columns from node configuration
                columns = self._extract_columns_from_node(node)

                # If no columns found, try to read from the source file
                if not columns:
                    columns = self._try_read_source_columns(node, schema, table)

                source_info = SourceInfo(
                    schema=schema,
                    table=table,
                    columns=columns,
                    description=node.annotation or node.get_display_name(),
                    source_path=node.source_path or node.table_name or "",
                )

                # Merge columns if source already exists
                if table in self.sources[schema]:
                    existing = self.sources[schema][table]
                    # Combine columns, removing duplicates
                    all_cols = list(dict.fromkeys(existing.columns + columns))
                    existing.columns = all_cols
                else:
                    self.sources[schema][table] = source_info

    def _try_read_source_columns(self, node: AlteryxNode, schema: str, table: str) -> List[str]:
        """Try to read columns from the source file, prompting user if needed."""
        columns = []
        source_key = f"{schema}.{table}"

        # First, try the source path from the node
        source_path = node.source_path
        if source_path:
            columns = self._read_file_columns(source_path)
            if columns:
                return columns

        # If interactive mode, prompt user for the file location
        if self.interactive and source_key not in self._resolved_source_files:
            columns = self._prompt_for_source_file(node, schema, table)

        return columns

    def _prompt_for_source_file(self, node: AlteryxNode, schema: str, table: str) -> List[str]:
        """Prompt user for source file location to read column metadata."""
        source_key = f"{schema}.{table}"
        source_display = node.source_path or node.table_name or table

        print("\n" + "=" * 60)
        print(f"Unknown columns for source: {source_display}")
        print(f"Schema: {schema}, Table: {table}")
        if node.annotation:
            print(f"Description: {node.annotation}")
        print("=" * 60)
        print("\nTo generate accurate DBT models, column information is needed.")
        print("Options:")
        print("[1] Enter path to raw data file (CSV, Excel, JSON, Parquet)")
        print("[2] Enter columns manually (comma-separated)")
        print("[3] Skip (will use SELECT * with TODO comment)")
        print()

        while True:
            try:
                choice = input("Your choice (1-3): ").strip()

                if choice == "1":
                    file_path = input("Enter full path to data file: ").strip()
                    file_path = file_path.strip('"\'')  # Remove quotes if present

                    if Path(file_path).exists():
                        columns = self._read_file_columns(file_path)
                        if columns:
                            self._resolved_source_files[source_key] = file_path
                            print(f"Found {len(columns)} columns: {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}")
                            return columns
                        else:
                            print(f"Could not read columns from: {file_path}")
                            print("Supported formats: CSV, Excel (.xlsx/.xls), JSON, Parquet")
                            continue
                    else:
                        print(f"File not found: {file_path}")
                        continue

                elif choice == "2":
                    cols_input = input("Enter column names (comma-separated): ").strip()
                    if cols_input:
                        columns = [c.strip() for c in cols_input.split(',') if c.strip()]
                        if columns:
                            print(f"Using {len(columns)} columns: {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}")
                            return columns
                    print("No valid columns entered. Please try again.")
                    continue

                elif choice == "3":
                    print(f"Skipping column detection for {table}")
                    self._resolved_source_files[source_key] = ""  # Mark as skipped
                    return []

                else:
                    print("Invalid choice. Please enter 1, 2, or 3.")

            except KeyboardInterrupt:
                print("\nSkipping column detection")
                return []
            except EOFError:
                # Non-interactive environment
                return []

    def _read_file_columns(self, file_path: str) -> List[str]:
        """Read column names from a data file.

        Supports: CSV, Excel (.xlsx/.xls), JSON, Parquet
        """
        path = Path(file_path)
        if not path.exists():
            return []

        suffix = path.suffix.lower()
        columns = []

        try:
            if suffix == '.csv':
                columns = self._read_csv_columns(file_path)
            elif suffix in ['.xlsx', '.xls']:
                columns = self._read_excel_columns(file_path)
            elif suffix == '.json':
                columns = self._read_json_columns(file_path)
            elif suffix == '.parquet':
                columns = self._read_parquet_columns(file_path)
            elif suffix in ['.txt', '.tsv']:
                # Try as tab-separated or delimited file
                columns = self._read_csv_columns(file_path, delimiter='\t')
        except Exception as e:
            print(f"Warning: Could not read {file_path}: {e}")

        return columns

    def _read_csv_columns(self, file_path: str, delimiter: str = ',') -> List[str]:
        """Read column headers from a CSV file."""
        columns = []
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                # Try to detect delimiter if comma doesn't work well
                sample = f.read(4096)
                f.seek(0)

                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=',;\t|')
                    delimiter = dialect.delimiter
                except csv.Error:
                    pass  # Use default delimiter

                reader = csv.reader(f, delimiter=delimiter)
                header_row = next(reader, None)
                if header_row:
                    columns = [col.strip() for col in header_row if col.strip()]
        except Exception as e:
            print(f"Warning: Error reading CSV {file_path}: {e}")

        return columns

    def _read_excel_columns(self, file_path: str) -> List[str]:
        """Read column headers from an Excel file."""
        columns = []
        try:
            # Try openpyxl for .xlsx
            import openpyxl
            wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
            ws = wb.active
            if ws:
                first_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
                if first_row:
                    columns = [str(cell).strip() for cell in first_row if cell is not None]
            wb.close()
        except ImportError:
            print("Note: Install openpyxl for Excel support: pip install openpyxl")
        except Exception as e:
            # Try xlrd for .xls
            try:
                import xlrd
                wb = xlrd.open_workbook(file_path)
                ws = wb.sheet_by_index(0)
                if ws.nrows > 0:
                    columns = [str(cell.value).strip() for cell in ws.row(0) if cell.value]
            except ImportError:
                print("Note: Install xlrd for .xls support: pip install xlrd")
            except Exception as e2:
                print(f"Warning: Error reading Excel {file_path}: {e2}")

        return columns

    def _read_json_columns(self, file_path: str) -> List[str]:
        """Read column/field names from a JSON file."""
        columns = []
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Handle different JSON structures
            if isinstance(data, list) and len(data) > 0:
                # Array of objects - get keys from first object
                if isinstance(data[0], dict):
                    columns = list(data[0].keys())
            elif isinstance(data, dict):
                # Single object or nested structure
                if all(isinstance(v, (str, int, float, bool, type(None))) for v in data.values()):
                    # Flat object
                    columns = list(data.keys())
                elif 'data' in data and isinstance(data['data'], list):
                    # Common pattern: {"data": [...]}
                    if len(data['data']) > 0 and isinstance(data['data'][0], dict):
                        columns = list(data['data'][0].keys())
                elif 'records' in data and isinstance(data['records'], list):
                    # Common pattern: {"records": [...]}
                    if len(data['records']) > 0 and isinstance(data['records'][0], dict):
                        columns = list(data['records'][0].keys())
        except Exception as e:
            print(f"Warning: Error reading JSON {file_path}: {e}")

        return columns

    def _read_parquet_columns(self, file_path: str) -> List[str]:
        """Read column names from a Parquet file."""
        columns = []
        try:
            import pyarrow.parquet as pq
            parquet_file = pq.ParquetFile(file_path)
            columns = parquet_file.schema.names
        except ImportError:
            print("Note: Install pyarrow for Parquet support: pip install pyarrow")
        except Exception as e:
            print(f"Warning: Error reading Parquet {file_path}: {e}")

        return columns

    def _extract_columns_from_node(self, node: AlteryxNode) -> List[str]:
        """Extract column names from a node's configuration."""
        columns = []

        # From selected fields
        if node.selected_fields:
            for field in node.selected_fields:
                # Handle "field AS alias" format
                if ' AS ' in field.upper():
                    col = field.split(' AS ')[0].strip().strip('"')
                else:
                    col = field.strip().strip('"')
                if col and col != '*':
                    columns.append(col)

        # From group by fields
        if node.group_by_fields:
            columns.extend([f for f in node.group_by_fields if f not in columns])

        # From aggregations
        if node.aggregations:
            for agg in node.aggregations:
                field = agg.get('field', '')
                output = agg.get('output_name', field)
                if field and field != '*' and field not in columns:
                    columns.append(field)
                if output and output not in columns:
                    columns.append(output)

        # From join keys
        if node.join_keys:
            for key in node.join_keys:
                parts = key.split('=')
                for part in parts:
                    col = part.strip()
                    if col and col not in columns:
                        columns.append(col)

        # From formulas
        formulas = node.configuration.get('formulas', [])
        for f in formulas:
            field = f.get('field', '')
            if field and field not in columns:
                columns.append(field)

        # From SQL query - extract column names
        if node.sql_query:
            # Simple extraction from SELECT clause
            sql_cols = self._extract_columns_from_sql(node.sql_query)
            columns.extend([c for c in sql_cols if c not in columns])

        return columns

    def _extract_columns_from_sql(self, sql: str) -> List[str]:
        """Extract column names from a SQL SELECT statement."""
        columns = []
        sql_upper = sql.upper()

        # Find SELECT ... FROM
        select_match = re.search(r'SELECT\s+(.+?)\s+FROM', sql_upper, re.DOTALL)
        if select_match:
            select_clause = sql[select_match.start(1):select_match.end(1)]

            # Skip if SELECT *
            if select_clause.strip() == '*':
                return columns

            # Split by comma (but not inside parentheses)
            parts = self._split_sql_columns(select_clause)

            for part in parts:
                part = part.strip()
                if not part or part == '*':
                    continue

                # Get the alias or column name
                # Handle "expression AS alias" or just "column_name"
                as_match = re.search(r'\s+AS\s+(\w+)\s*$', part, re.IGNORECASE)
                if as_match:
                    columns.append(as_match.group(1))
                else:
                    # Just get the last word (column name)
                    words = part.split()
                    if words:
                        col = words[-1].strip('",[]')
                        if col and not col.startswith('('):
                            columns.append(col)

        return columns

    def _split_sql_columns(self, select_clause: str) -> List[str]:
        """Split SQL SELECT clause by commas, respecting parentheses."""
        parts = []
        current = ""
        depth = 0

        for char in select_clause:
            if char == '(':
                depth += 1
                current += char
            elif char == ')':
                depth -= 1
                current += char
            elif char == ',' and depth == 0:
                parts.append(current)
                current = ""
            else:
                current += char

        if current:
            parts.append(current)

        return parts

    def _get_node_columns(self, node: AlteryxNode, workflow: AlteryxWorkflow) -> List[str]:
        """Get available columns at a node by tracing data lineage.

        This determines what columns are available after a node's transformation
        by looking at upstream columns and the node's own transformations.
        """
        # Check cache first
        if node.tool_id in self._node_columns:
            return self._node_columns[node.tool_id]

        columns = []

        # Get upstream columns first
        upstream_nodes = workflow.get_upstream_nodes(node.tool_id)
        upstream_columns = []
        for up_node in upstream_nodes:
            upstream_columns.extend(self._get_node_columns(up_node, workflow))
        # Remove duplicates while preserving order
        upstream_columns = list(dict.fromkeys(upstream_columns))

        # Now apply this node's transformation
        if node.category == ToolCategory.INPUT:
            # Input nodes: get columns from source
            columns = self._extract_columns_from_node(node)

        elif node.plugin_name == "Select":
            # Select tool: only keep selected columns, remove deselected
            if node.selected_fields:
                for field in node.selected_fields:
                    # Handle "field AS alias" format
                    if ' AS ' in field.upper():
                        # Use the alias as the output column name
                        parts = field.upper().split(' AS ')
                        alias = field[field.upper().index(' AS ') + 4:].strip()
                        columns.append(alias)
                    else:
                        columns.append(field)
            else:
                # No selected fields specified, pass through all upstream
                columns = upstream_columns

        elif node.plugin_name in ["Formula", "Multi-Field Formula"]:
            # Formula: pass through upstream + add new calculated columns
            columns = list(upstream_columns)
            formulas = node.configuration.get('formulas', [])
            for f in formulas:
                field = f.get('field', '')
                if field and field not in columns:
                    columns.append(field)

        elif node.plugin_name == "Filter":
            # Filter: pass through all upstream columns (just filters rows)
            columns = upstream_columns

        elif node.plugin_name == "Sort":
            # Sort: pass through all upstream columns (just reorders rows)
            columns = upstream_columns

        elif node.plugin_name == "Join":
            # Join: columns from both inputs (upstream nodes)
            columns = upstream_columns

        elif node.plugin_name == "Summarize":
            # Summarize: group by columns + aggregation output columns
            if node.group_by_fields:
                columns.extend(node.group_by_fields)
            for agg in node.aggregations:
                output_name = agg.get('output_name', agg.get('field', ''))
                if output_name and output_name not in columns:
                    columns.append(output_name)

        elif node.plugin_name == "Union":
            # Union: typically uses first input's columns
            columns = upstream_columns

        else:
            # Default: pass through upstream columns
            columns = upstream_columns if upstream_columns else self._extract_columns_from_node(node)

        # Cache the result
        self._node_columns[node.tool_id] = columns
        return columns

    def _get_upstream_columns(self, node: AlteryxNode, workflow: AlteryxWorkflow) -> List[str]:
        """Get columns available from upstream nodes."""
        upstream_nodes = workflow.get_upstream_nodes(node.tool_id)
        all_columns = []
        for up_node in upstream_nodes:
            cols = self._get_node_columns(up_node, workflow)
            all_columns.extend([c for c in cols if c not in all_columns])
        return all_columns

    def _format_column_list(self, columns: List[str], indent: str = "        ") -> str:
        """Format a list of columns for SQL SELECT, with proper quoting."""
        if not columns:
            return f"{indent}*  -- TODO: specify columns"
        quoted = [self._quote_column(c) for c in columns]
        return f",\n{indent}".join(quoted)

    def _get_schema_name(self, node: AlteryxNode) -> str:
        """Determine schema name from source node."""
        if node.connection_string:
            conn_lower = node.connection_string.lower()
            if 'database=' in conn_lower:
                match = re.search(r'database=([^;]+)', conn_lower)
                if match:
                    return self._sanitize_name(match.group(1))

        return "raw"

    def _get_table_name(self, node: AlteryxNode) -> str:
        """Determine table name from source node - prioritize meaningful names."""
        # Priority 1: Explicit table name
        if node.table_name:
            return self._sanitize_name(node.table_name)

        # Priority 2: File name from source path
        if node.source_path:
            return self._sanitize_name(Path(node.source_path).stem)

        # Priority 3: Annotation (user-defined name)
        if node.annotation:
            return self._sanitize_name(node.annotation)

        # Priority 4: Extract from SQL query (e.g., "FROM schema.table_name")
        if node.sql_query:
            table_from_sql = self._extract_table_from_sql(node.sql_query)
            if table_from_sql:
                return self._sanitize_name(table_from_sql)

        # Priority 5: Use display name if meaningful
        display = node.get_display_name()
        if display and display != node.plugin_name:
            # Extract just the annotation part if present
            if ':' in display:
                name = display.split(':', 1)[1].strip()
                return self._sanitize_name(name)

        # Fallback: Use a descriptive name based on tool type
        return f"{self._sanitize_name(node.plugin_name)}_{node.tool_id}"

    def _extract_table_from_sql(self, sql: str) -> Optional[str]:
        """Extract table name from SQL query."""
        if not sql:
            return None

        # Look for "FROM schema.table" or "FROM table"
        match = re.search(r'\bFROM\s+([^\s,()]+)', sql, re.IGNORECASE)
        if match:
            table_ref = match.group(1).strip('[]"\'`')
            # Get last part if qualified (schema.table)
            if '.' in table_ref:
                return table_ref.split('.')[-1]
            return table_ref

        return None

    def _get_descriptive_gold_name(self, node: AlteryxNode, workflow: AlteryxWorkflow) -> str:
        """Generate a descriptive name for gold layer models based on actual output."""
        # Skip containers - use children's info instead
        if node.category == ToolCategory.CONTAINER:
            return self._get_container_descriptive_name(node, workflow)

        # Priority 1: Use target table name or file name
        if node.target_path:
            name = Path(node.target_path).stem
            return self._sanitize_name(name)

        if node.table_name:
            return self._sanitize_name(node.table_name)

        # Priority 2: Use annotation if available
        if node.annotation:
            return self._sanitize_name(node.annotation)

        # Priority 3: For Summarize, describe what it's summarizing
        if node.plugin_name == "Summarize":
            if node.group_by_fields:
                # e.g., "summary_by_customer_region"
                fields = "_".join(node.group_by_fields[:2])
                return f"summary_by_{self._sanitize_name(fields)}"
            if node.aggregations:
                # e.g., "total_revenue_count_orders"
                agg_names = [a.get('output_name', a.get('field', '')) for a in node.aggregations[:2]]
                return self._sanitize_name("_".join(agg_names))

        # Priority 4: For Output, use workflow name + output indicator
        if node.plugin_name in ["Output Data", "Browse"]:
            return f"{self._sanitize_name(workflow.metadata.name)}_output"

        # Fallback - avoid generic "tool_container" names
        display = node.get_display_name()
        if "container" in display.lower() or "tool container" in display.lower():
            # Try to find a more meaningful name from context
            return f"{self._sanitize_name(workflow.metadata.name)}_result"

        return self._sanitize_name(display)

    def _get_container_descriptive_name(self, container: AlteryxNode, workflow: AlteryxWorkflow) -> str:
        """Get a descriptive name for a container by looking at its children."""
        # If container has annotation, use it
        if container.annotation and "container" not in container.annotation.lower():
            return self._sanitize_name(container.annotation)

        # Look at child nodes to determine purpose
        output_children = []
        summarize_children = []

        for child_id in container.child_tool_ids:
            child = workflow.get_node_by_id(child_id)
            if child:
                if child.category == ToolCategory.OUTPUT:
                    output_children.append(child)
                elif child.plugin_name == "Summarize":
                    summarize_children.append(child)

        # Use output child's info if available
        for child in output_children:
            if child.target_path:
                return self._sanitize_name(Path(child.target_path).stem)
            if child.table_name:
                return self._sanitize_name(child.table_name)
            if child.annotation:
                return self._sanitize_name(child.annotation)

        # Use summarize child's info
        for child in summarize_children:
            if child.group_by_fields:
                fields = "_".join(child.group_by_fields[:2])
                return f"summary_by_{self._sanitize_name(fields)}"

        # Fallback to workflow name
        return f"{self._sanitize_name(workflow.metadata.name)}_container_output"

    def _quote_column(self, col: str) -> str:
        """Wrap column name in double quotes for Trino compatibility."""
        # Don't quote if already quoted or if it's a *
        if col.startswith('"') or col == '*':
            return col
        return f'"{col}"'

    def _generate_sources_yml(self) -> None:
        """Generate sources.yml file with actual table and column names."""
        content = [
            "version: 2",
            "",
            "sources:",
        ]

        for schema, tables in sorted(self.sources.items()):
            content.extend([
                f"  - name: {schema}",
                f"    description: \"Source data from {schema}\"",
                "    tables:",
            ])

            for table_name, source_info in sorted(tables.items()):
                content.extend([
                    f"      - name: {table_name}",
                    f"        description: \"{source_info.description}\"",
                ])

                # Add columns if we have them
                if source_info.columns:
                    content.append("        columns:")
                    for col in source_info.columns:
                        content.extend([
                            f"          - name: \"{col}\"",
                            f"            description: \"Column {col} from source\"",
                        ])

        self._write_file(
            self.output_dir / "models" / "bronze" / "_sources.yml",
            "\n".join(content)
        )

    def _generate_workflow_models(self, workflow: AlteryxWorkflow) -> None:
        """Generate DBT models for a single workflow."""
        # Clear column cache for new workflow
        self._node_columns = {}
        self._current_workflow = workflow

        analyzer = TransformationAnalyzer(workflow)
        medallion = analyzer.suggest_medallion_mapping()

        workflow_prefix = self._sanitize_name(workflow.metadata.name)

        # Generate bronze models (staging)
        bronze_nodes = medallion.get(MedallionLayer.BRONZE.value, [])
        for node in bronze_nodes:
            if node.category == ToolCategory.INPUT:
                self._generate_bronze_model(node, workflow_prefix)

        # Generate silver models (intermediate) - aggregate sequential transforms
        silver_nodes = medallion.get(MedallionLayer.SILVER.value, [])
        # Filter out containers
        silver_nodes = [n for n in silver_nodes if n.category != ToolCategory.CONTAINER]
        # Group and aggregate silver models
        self._generate_aggregated_silver_models(silver_nodes, workflow_prefix, workflow)

        # Generate gold models (marts)
        gold_nodes = medallion.get(MedallionLayer.GOLD.value, [])
        for node in gold_nodes:
            # Skip containers - their children should be processed separately
            if node.category == ToolCategory.CONTAINER:
                continue
            self._generate_gold_model(node, workflow_prefix, workflow)

    def _generate_aggregated_silver_models(self, silver_nodes: List[AlteryxNode],
                                            workflow_prefix: str,
                                            workflow: AlteryxWorkflow) -> None:
        """Generate aggregated silver models to minimize the number of intermediate steps."""
        if not silver_nodes:
            return

        # Group nodes by their upstream source
        # Nodes that form a linear chain can be merged into one model
        processed = set()
        chains = []

        # Build chains of sequential transformations
        for node in silver_nodes:
            if node.tool_id in processed:
                continue

            chain = [node]
            processed.add(node.tool_id)

            # Follow downstream until we hit a fork or a gold layer node
            current = node
            while True:
                downstream = workflow.get_downstream_nodes(current.tool_id)
                # Filter to silver nodes only
                downstream_silver = [n for n in downstream
                                    if n in silver_nodes and n.tool_id not in processed]

                # If exactly one downstream silver node and it has only this as upstream
                if len(downstream_silver) == 1:
                    next_node = downstream_silver[0]
                    upstream = workflow.get_upstream_nodes(next_node.tool_id)
                    # Only merge if this node feeds exclusively into next_node
                    if len([u for u in upstream if u in silver_nodes]) == 1:
                        chain.append(next_node)
                        processed.add(next_node.tool_id)
                        current = next_node
                    else:
                        break
                else:
                    break

            chains.append(chain)

        # Generate one model per chain
        for chain in chains:
            if len(chain) == 1:
                # Single node - generate normally
                self._generate_silver_model(chain[0], workflow_prefix, workflow)
            else:
                # Multiple nodes - generate combined model
                self._generate_combined_silver_model(chain, workflow_prefix, workflow)

    def _generate_combined_silver_model(self, chain: List[AlteryxNode],
                                         workflow_prefix: str,
                                         workflow: AlteryxWorkflow) -> None:
        """Generate a single silver model that combines multiple sequential transformations."""
        # Use the last node in the chain for the model name (most descriptive)
        primary_node = chain[-1]

        # Create a name that represents the chain
        chain_names = [n.plugin_name for n in chain]
        if len(chain_names) > 3:
            model_desc = f"{chain_names[0]}_{chain_names[-1]}"
        else:
            model_desc = "_".join(chain_names)

        model_name = f"int_{workflow_prefix}_{self._sanitize_name(model_desc)}"

        # Get upstream dependencies (from the first node in chain)
        first_node = chain[0]
        upstream = workflow.get_upstream_nodes(first_node.tool_id)

        content = [
            f"-- Silver model combining {len(chain)} transformations:",
            f"-- " + " -> ".join([n.plugin_name for n in chain]),
            f"-- Generated from Alteryx workflow tools #{', #'.join([str(n.tool_id) for n in chain])}",
            "",
            "{{",
            "    config(",
            "        materialized='table'",
            "    )",
            "}}",
            "",
        ]

        # Generate CTEs for upstream dependencies
        if upstream:
            for i, up_node in enumerate(upstream):
                up_model = self._get_model_reference(up_node, workflow_prefix)
                cte_name = f"source_{i + 1}" if len(upstream) > 1 else "source"
                # Get columns from upstream node
                up_columns = self._get_node_columns(up_node, workflow)
                if up_columns:
                    col_list = self._format_column_list(up_columns)
                    content.extend([
                        f"with {cte_name} as (",
                        "",
                        f"    select",
                        f"        {col_list}",
                        f"    from {{{{ ref('{up_model}') }}}}",
                        "",
                        ")," if i < len(upstream) - 1 else "),",
                        "",
                    ])
                else:
                    content.extend([
                        f"with {cte_name} as (",
                        "",
                        f"    select * from {{{{ ref('{up_model}') }}}}  -- TODO: specify columns",
                        "",
                        ")," if i < len(upstream) - 1 else "),",
                        "",
                    ])

        # Generate combined transformation logic
        content.append(self._generate_chained_transformation_sql(chain, upstream, workflow))

        self._write_file(
            self.output_dir / "models" / "silver" / f"{model_name}.sql",
            "\n".join(content)
        )
        self.models_generated.append(model_name)

        # Collect columns from all nodes in chain
        all_columns = []
        for node in chain:
            cols = self._extract_columns_from_node(node)
            all_columns.extend([c for c in cols if c not in all_columns])

        self.models_info[model_name] = ModelInfo(
            name=model_name,
            layer="silver",
            columns=all_columns,
            description=f"Combined: {' -> '.join([n.plugin_name for n in chain])}",
            source_tool_id=chain[0].tool_id,
        )

    def _generate_chained_transformation_sql(self, chain: List[AlteryxNode],
                                              upstream: List[AlteryxNode],
                                              workflow: AlteryxWorkflow) -> str:
        """Generate SQL that chains multiple transformations together."""
        source_cte = "source" if len(upstream) <= 1 else "source_1"

        # Build CTEs for each transformation in the chain
        cte_parts = []
        prev_cte = source_cte

        for i, node in enumerate(chain):
            cte_name = f"step_{i + 1}" if i < len(chain) - 1 else "final"
            sql_block = self._generate_single_transform_cte(node, prev_cte, cte_name, workflow)
            cte_parts.append(sql_block)
            prev_cte = cte_name

        # Get final columns from the last node in chain
        final_columns = self._get_node_columns(chain[-1], workflow)
        if final_columns:
            final_col_list = self._format_column_list(final_columns)
            return "\n\n".join(cte_parts) + f"\n\nselect\n    {final_col_list}\nfrom final"
        else:
            return "\n\n".join(cte_parts) + "\n\nselect * from final  -- TODO: specify columns"

    def _generate_single_transform_cte(self, node: AlteryxNode, source_cte: str, cte_name: str,
                                        workflow: AlteryxWorkflow) -> str:
        """Generate a single CTE for a transformation node."""
        # Get upstream columns for this node
        upstream_columns = self._get_upstream_columns(node, workflow)

        if node.plugin_name == "Filter":
            condition = self._convert_expression(node.expression or "1=1")
            # Filter passes through all columns
            if upstream_columns:
                col_list = self._format_column_list(upstream_columns)
                return f"""{cte_name} as (
    -- {node.plugin_name}: {node.annotation or ''}
    select
        {col_list}
    from {source_cte}
    where {condition}
),"""
            else:
                return f"""{cte_name} as (
    -- {node.plugin_name}: {node.annotation or ''}
    select *  -- TODO: specify columns
    from {source_cte}
    where {condition}
),"""

        elif node.plugin_name in ["Formula", "Multi-Field Formula"]:
            formulas = node.configuration.get('formulas', [])
            if formulas:
                # Start with upstream columns, then add formula columns
                if upstream_columns:
                    select_parts = [self._quote_column(c) for c in upstream_columns]
                else:
                    select_parts = ["*  -- TODO: specify columns"]
                for f in formulas:
                    field = self._quote_column(f.get('field', 'new_field'))
                    expr = self._convert_expression(f.get('expression', 'NULL'))
                    select_parts.append(f"{expr} as {field}")

                return f"""{cte_name} as (
    -- {node.plugin_name}: {node.annotation or ''}
    select
        {','.join(chr(10) + '        ' + p for p in select_parts)}
    from {source_cte}
),"""

        elif node.plugin_name == "Select":
            if node.selected_fields:
                quoted_fields = [self._quote_column(f.split(' AS ')[0].strip()) +
                                (' as ' + self._quote_column(f.split(' AS ')[1].strip()) if ' AS ' in f.upper() else '')
                                for f in node.selected_fields]
                fields = ",\n        ".join(quoted_fields)
                return f"""{cte_name} as (
    -- {node.plugin_name}: {node.annotation or ''}
    select
        {fields}
    from {source_cte}
),"""

        elif node.plugin_name == "Sort":
            sort_fields = node.configuration.get('sort_fields', [])
            if sort_fields:
                order_parts = []
                for sf in sort_fields:
                    direction = "asc" if sf.get('order', 'Ascending') == 'Ascending' else "desc"
                    field = self._quote_column(sf['field'])
                    order_parts.append(f"{field} {direction}")
                order_clause = ", ".join(order_parts)
                # Sort passes through all columns
                if upstream_columns:
                    col_list = self._format_column_list(upstream_columns)
                    return f"""{cte_name} as (
    -- {node.plugin_name}: {node.annotation or ''}
    select
        {col_list}
    from {source_cte}
    order by {order_clause}
),"""
                else:
                    return f"""{cte_name} as (
    -- {node.plugin_name}: {node.annotation or ''}
    select *  -- TODO: specify columns
    from {source_cte}
    order by {order_clause}
),"""

        # Default pass-through with explicit columns if available
        if upstream_columns:
            col_list = self._format_column_list(upstream_columns)
            return f"""{cte_name} as (
    -- {node.plugin_name}: {node.annotation or ''} (TODO: implement)
    select
        {col_list}
    from {source_cte}
),"""
        else:
            return f"""{cte_name} as (
    -- {node.plugin_name}: {node.annotation or ''} (TODO: implement)
    select *  -- TODO: specify columns
    from {source_cte}
),"""

    def _generate_bronze_model(self, node: AlteryxNode, workflow_prefix: str) -> None:
        """Generate a bronze (staging) model with table materialization."""
        schema = self._get_schema_name(node)
        table = self._get_table_name(node)
        model_name = f"stg_{workflow_prefix}_{table}"

        # Set context for TODO tracking
        self._current_model_name = model_name
        self._current_layer = "bronze"

        # Get columns for this source
        columns = []
        if schema in self.sources and table in self.sources[schema]:
            columns = self.sources[schema][table].columns

        # Build column list with double quotes
        if columns:
            col_list = ",\n        ".join([self._quote_column(c) for c in columns])
            select_clause = f"        {col_list}"
        else:
            select_clause = "        * -- TODO: Replace with explicit column list"
            self._add_todo(
                todo_type="specify_columns",
                description="Replace SELECT * with explicit column list",
                context=f"Source: {node.source_path or node.table_name or schema + '.' + table}",
                priority="high"
            )

        # Build the source select with explicit columns
        if columns:
            source_col_list = ",\n        ".join([self._quote_column(c) for c in columns])
            source_select = f"    select\n        {source_col_list}\n    from {{{{ source('{schema}', '{table}') }}}}"
            final_select = f"select\n    {','.join(chr(10) + '    ' + self._quote_column(c) for c in columns)}\nfrom renamed"
        else:
            source_select = f"    select * from {{{{ source('{schema}', '{table}') }}}}  -- TODO: specify columns"
            final_select = "select * from renamed  -- TODO: specify columns"

        content = [
            f"-- Bronze model for {node.get_display_name()}",
            f"-- Source: {node.source_path or node.table_name or 'Unknown'}",
            f"-- Generated from Alteryx workflow tool #{node.tool_id}",
            "",
            "{{",
            "    config(",
            "        materialized='table'",  # Bronze = table
            "    )",
            "}}",
            "",
            "with source as (",
            "",
            source_select,
            "",
            "),",
            "",
            "renamed as (",
            "",
            "    select",
            select_clause,
            "    from source",
            "",
            ")",
            "",
            final_select,
        ]

        self._write_file(
            self.output_dir / "models" / "bronze" / f"{model_name}.sql",
            "\n".join(content)
        )
        self.models_generated.append(model_name)

        # Store model info for schema generation
        self.models_info[model_name] = ModelInfo(
            name=model_name,
            layer="bronze",
            columns=columns,
            description=f"Staging model for {node.get_display_name()}",
            source_tool_id=node.tool_id,
        )

    def _generate_silver_model(self, node: AlteryxNode,
                                workflow_prefix: str,
                                workflow: AlteryxWorkflow) -> None:
        """Generate a silver (intermediate) model with table materialization."""
        model_name = f"int_{workflow_prefix}_{self._sanitize_name(node.get_display_name())}"

        # Set context for TODO tracking
        self._current_model_name = model_name
        self._current_layer = "silver"

        # Get upstream dependencies
        upstream = workflow.get_upstream_nodes(node.tool_id)

        content = [
            f"-- Silver model: {node.get_display_name()}",
            f"-- Tool type: {node.plugin_name}",
            f"-- Generated from Alteryx workflow tool #{node.tool_id}",
            "",
            "{{",
            "    config(",
            "        materialized='table'",  # Silver = table
            "    )",
            "}}",
            "",
        ]

        # Generate CTEs for upstream dependencies
        if upstream:
            for i, up_node in enumerate(upstream):
                up_model = self._get_model_reference(up_node, workflow_prefix)
                cte_name = f"source_{i + 1}" if len(upstream) > 1 else "source"
                # Get columns from upstream node
                up_columns = self._get_node_columns(up_node, workflow)
                if up_columns:
                    col_list = self._format_column_list(up_columns)
                    content.extend([
                        f"with {cte_name} as (",
                        "",
                        f"    select",
                        f"        {col_list}",
                        f"    from {{{{ ref('{up_model}') }}}}",
                        "",
                        ")," if i < len(upstream) - 1 else "),",
                        "",
                    ])
                else:
                    content.extend([
                        f"with {cte_name} as (",
                        "",
                        f"    select * from {{{{ ref('{up_model}') }}}}  -- TODO: specify columns",
                        "",
                        ")," if i < len(upstream) - 1 else "),",
                        "",
                    ])
                    self._add_todo(
                        todo_type="specify_columns",
                        description=f"Specify columns from upstream model '{up_model}'",
                        context=f"CTE: {cte_name}",
                        priority="medium"
                    )

        # Generate transformation logic based on tool type
        sql = self._generate_transformation_sql(node, upstream, workflow)
        content.append(sql)

        # Track TODOs in generated transformation SQL
        if "-- TODO:" in sql:
            if "specify columns" in sql.lower():
                self._add_todo(
                    todo_type="specify_columns",
                    description="Specify explicit columns in transformation",
                    context=f"Tool: {node.plugin_name}",
                    priority="medium"
                )
            if "implement" in sql.lower():
                self._add_todo(
                    todo_type="implement_transformation",
                    description=f"Implement {node.plugin_name} transformation logic",
                    context=f"Tool #{node.tool_id}: {node.annotation or node.plugin_name}",
                    priority="high"
                )

        self._write_file(
            self.output_dir / "models" / "silver" / f"{model_name}.sql",
            "\n".join(content)
        )
        self.models_generated.append(model_name)

        # Store model info
        columns = self._extract_columns_from_node(node)
        self.models_info[model_name] = ModelInfo(
            name=model_name,
            layer="silver",
            columns=columns,
            description=f"Intermediate model: {node.plugin_name} - {node.get_display_name()}",
            source_tool_id=node.tool_id,
        )

    def _generate_gold_model(self, node: AlteryxNode,
                              workflow_prefix: str,
                              workflow: AlteryxWorkflow) -> None:
        """Generate a gold (mart) model with view materialization."""
        # Use descriptive name based on actual output
        descriptive_name = self._get_descriptive_gold_name(node, workflow)

        # Determine if it's a fact or dimension
        if node.plugin_name == "Summarize" or node.aggregations:
            prefix = "fct"
        else:
            prefix = "dim"

        model_name = f"{prefix}_{workflow_prefix}_{descriptive_name}"

        # Set context for TODO tracking
        self._current_model_name = model_name
        self._current_layer = "gold"

        # Get upstream dependencies
        upstream = workflow.get_upstream_nodes(node.tool_id)

        content = [
            f"-- Gold model: {node.get_display_name()}",
            f"-- Output: {node.target_path or node.table_name or 'N/A'}",
            f"-- Tool type: {node.plugin_name}",
            f"-- Generated from Alteryx workflow tool #{node.tool_id}",
            "",
            "{{",
            "    config(",
            "        materialized='view'",  # Gold = view
            "    )",
            "}}",
            "",
        ]

        # Generate CTEs
        if upstream:
            for i, up_node in enumerate(upstream):
                up_model = self._get_model_reference(up_node, workflow_prefix)
                cte_name = f"source_{i + 1}" if len(upstream) > 1 else "source"
                # Get columns from upstream node
                up_columns = self._get_node_columns(up_node, workflow)
                if up_columns:
                    col_list = self._format_column_list(up_columns)
                    content.extend([
                        f"with {cte_name} as (",
                        "",
                        f"    select",
                        f"        {col_list}",
                        f"    from {{{{ ref('{up_model}') }}}}",
                        "",
                        ")," if i < len(upstream) - 1 else "),",
                        "",
                    ])
                else:
                    content.extend([
                        f"with {cte_name} as (",
                        "",
                        f"    select * from {{{{ ref('{up_model}') }}}}  -- TODO: specify columns",
                        "",
                        ")," if i < len(upstream) - 1 else "),",
                        "",
                    ])
                    self._add_todo(
                        todo_type="specify_columns",
                        description=f"Specify columns from upstream model '{up_model}'",
                        context=f"CTE: {cte_name}",
                        priority="medium"
                    )

        # Generate transformation logic
        sql = self._generate_transformation_sql(node, upstream, workflow)
        content.append(sql)

        # Track TODOs in generated transformation SQL
        if "-- TODO:" in sql:
            if "specify columns" in sql.lower():
                self._add_todo(
                    todo_type="specify_columns",
                    description="Specify explicit columns in output",
                    context=f"Output: {node.target_path or node.table_name or 'unknown'}",
                    priority="medium"
                )
            if "implement" in sql.lower():
                self._add_todo(
                    todo_type="implement_transformation",
                    description=f"Implement {node.plugin_name} transformation logic",
                    context=f"Tool #{node.tool_id}: {node.annotation or node.plugin_name}",
                    priority="high"
                )

        self._write_file(
            self.output_dir / "models" / "gold" / f"{model_name}.sql",
            "\n".join(content)
        )
        self.models_generated.append(model_name)

        # Store model info with target columns
        columns = self._extract_columns_from_node(node)
        self.models_info[model_name] = ModelInfo(
            name=model_name,
            layer="gold",
            columns=columns,
            description=f"Gold model: {node.get_display_name()} -> {node.target_path or node.table_name or 'output'}",
            source_tool_id=node.tool_id,
        )

    def _get_model_reference(self, node: AlteryxNode, workflow_prefix: str) -> str:
        """Get the model name to reference for a node."""
        if node.category == ToolCategory.INPUT:
            table = self._get_table_name(node)
            return f"stg_{workflow_prefix}_{table}"
        else:
            return f"int_{workflow_prefix}_{self._sanitize_name(node.get_display_name())}"

    def _generate_macro_reference_sql(self, node: AlteryxNode,
                                       upstream: List[AlteryxNode],
                                       source_cte: str) -> str:
        """Generate SQL that calls a DBT macro for a macro node.

        When an Alteryx workflow uses a macro, this generates SQL that calls
        the corresponding DBT macro, maintaining reusability across models.
        """
        # Get the DBT macro name from the mapping
        macro_path = node.macro_path or ""
        dbt_macro_name = self._macro_name_map.get(
            macro_path,
            self._sanitize_name(Path(macro_path).stem) if macro_path else "unknown_macro"
        )

        # Build the macro call
        if len(upstream) > 1:
            # Multiple inputs - pass them as separate arguments
            source_refs = [f"{source_cte}_{i+1}" for i in range(len(upstream))]
            macro_call = f"{{{{ {dbt_macro_name}({', '.join(source_refs)}) }}}}"
        else:
            macro_call = f"{{{{ {dbt_macro_name}({source_cte}) }}}}"

        return f"""-- Macro: {node.get_display_name()}
-- Original Alteryx macro: {macro_path}
-- Using reusable DBT macro: {dbt_macro_name}

{macro_call}"""

    def _generate_transformation_sql(self, node: AlteryxNode,
                                      upstream: List[AlteryxNode],
                                      workflow: AlteryxWorkflow) -> str:
        """Generate SQL for a transformation node with double-quoted columns."""
        source_cte = "source" if len(upstream) <= 1 else "source_1"

        # Check if this node is a macro - use the DBT macro if available
        if node.is_macro and node.macro_path:
            return self._generate_macro_reference_sql(node, upstream, source_cte)

        # Get upstream columns for explicit selects
        upstream_columns = self._get_upstream_columns(node, workflow)
        node_columns = self._get_node_columns(node, workflow)

        if node.plugin_name == "Filter":
            condition = self._convert_expression(node.expression or "1=1")
            # Filter passes through all upstream columns
            if upstream_columns:
                col_list = self._format_column_list(upstream_columns)
                final_col_list = self._format_column_list(upstream_columns)
                return f"""final as (

    select
        {col_list}
    from {source_cte}
    where {condition}

)

select
    {final_col_list}
from final"""
            else:
                return f"""final as (

    select *  -- TODO: specify columns
    from {source_cte}
    where {condition}

)

select * from final  -- TODO: specify columns"""

        elif node.plugin_name in ["Formula", "Multi-Field Formula"]:
            formulas = node.configuration.get('formulas', [])
            if formulas:
                # Start with upstream columns, then add formula columns
                if upstream_columns:
                    select_parts = [f"    {self._quote_column(c)}" for c in upstream_columns]
                else:
                    select_parts = ["    *  -- TODO: specify columns"]
                for f in formulas:
                    field = self._quote_column(f.get('field', 'new_field'))
                    expr = self._convert_expression(f.get('expression', 'NULL'))
                    select_parts.append(f"    , {expr} as {field}")

                # Final select uses node columns (upstream + formula outputs)
                if node_columns:
                    final_col_list = self._format_column_list(node_columns)
                    final_select = f"select\n    {final_col_list}\nfrom final"
                else:
                    final_select = "select * from final  -- TODO: specify columns"

                return f"""final as (

    select
{chr(10).join(select_parts)}
    from {source_cte}

)

{final_select}"""
            else:
                if upstream_columns:
                    col_list = self._format_column_list(upstream_columns)
                    return f"select\n    {col_list}\nfrom {source_cte}"
                return f"select * from {source_cte}  -- TODO: specify columns"

        elif node.plugin_name == "Join":
            join_type = node.join_type or "LEFT"
            conditions = []
            for key in node.join_keys:
                parts = key.split('=')
                if len(parts) == 2:
                    left_col = self._quote_column(parts[0].strip())
                    right_col = self._quote_column(parts[1].strip())
                    conditions.append(f"source_1.{left_col} = source_2.{right_col}")

            join_condition = " and ".join(conditions) if conditions else "1=1"

            # For joins, we need columns from both sources
            if upstream_columns:
                # Prefix columns with source alias to avoid ambiguity
                col_parts = [f"source_1.{self._quote_column(c)}" for c in upstream_columns]
                col_list = ",\n        ".join(col_parts)
                final_col_list = self._format_column_list(upstream_columns)
            else:
                col_list = "source_1.*  -- TODO: specify columns from both sources"
                final_col_list = "*  -- TODO: specify columns"

            return f"""final as (

    select
        {col_list}
    from source_1
    {join_type.lower()} join source_2
        on {join_condition}

)

select
    {final_col_list}
from final"""

        elif node.plugin_name == "Summarize":
            # Quote group by fields
            group_by_quoted = [self._quote_column(f) for f in node.group_by_fields] if node.group_by_fields else []
            group_cols = ", ".join(group_by_quoted) if group_by_quoted else "1"

            agg_parts = []
            for agg in node.aggregations:
                action = agg.get('action', 'COUNT')
                field = agg.get('field', '*')
                output = self._quote_column(agg.get('output_name', field))
                sql_func = AGGREGATION_MAP.get(action, action.upper())

                # Quote field if not *
                field_ref = self._quote_column(field) if field != '*' else field

                if sql_func.endswith('(DISTINCT'):
                    agg_parts.append(f"{sql_func} {field_ref}) as {output}")
                else:
                    agg_parts.append(f"{sql_func}({field_ref}) as {output}")

            select_clause = ", ".join(group_by_quoted) if group_by_quoted else ""
            if select_clause and agg_parts:
                select_clause += ",\n        "

            agg_clause = ",\n        ".join(agg_parts) if agg_parts else "count(*) as \"record_count\""

            # Summarize output columns are group by + aggregations
            if node_columns:
                final_col_list = self._format_column_list(node_columns)
            else:
                final_col_list = "*"

            return f"""final as (

    select
        {select_clause}{agg_clause}
    from {source_cte}
    group by {group_cols}

)

select
    {final_col_list}
from final"""

        elif node.plugin_name == "Union":
            if len(upstream) > 1:
                union_parts = []
                for i, up_node in enumerate(upstream):
                    up_cols = self._get_node_columns(up_node, workflow)
                    if up_cols:
                        col_list = self._format_column_list(up_cols)
                        union_parts.append(f"select\n    {col_list}\nfrom source_{i + 1}")
                    else:
                        union_parts.append(f"select * from source_{i + 1}  -- TODO: specify columns")
                return "\n\nunion all\n\n".join(union_parts)
            if upstream_columns:
                col_list = self._format_column_list(upstream_columns)
                return f"select\n    {col_list}\nfrom {source_cte}"
            return f"select * from {source_cte}  -- TODO: specify columns"

        elif node.plugin_name == "Select":
            if node.selected_fields:
                # Quote all field names - these ARE the explicit columns
                quoted_fields = [self._quote_column(f.split(' AS ')[0].strip()) +
                                (' as ' + self._quote_column(f.split(' AS ')[1].strip()) if ' AS ' in f.upper() else '')
                                for f in node.selected_fields]
                fields = ",\n        ".join(quoted_fields)

                # Final select uses the selected fields
                if node_columns:
                    final_col_list = self._format_column_list(node_columns)
                else:
                    final_col_list = fields

                return f"""final as (

    select
        {fields}
    from {source_cte}

)

select
    {final_col_list}
from final"""

        elif node.plugin_name == "Sort":
            sort_fields = node.configuration.get('sort_fields', [])
            if sort_fields:
                order_parts = []
                for sf in sort_fields:
                    direction = "asc" if sf.get('order', 'Ascending') == 'Ascending' else "desc"
                    field = self._quote_column(sf['field'])
                    order_parts.append(f"{field} {direction}")
                order_clause = ", ".join(order_parts)

                # Sort passes through all columns
                if upstream_columns:
                    col_list = self._format_column_list(upstream_columns)
                    return f"""final as (

    select
        {col_list}
    from {source_cte}
    order by {order_clause}

)

select
    {col_list}
from final"""
                else:
                    return f"""final as (

    select *  -- TODO: specify columns
    from {source_cte}
    order by {order_clause}

)

select * from final  -- TODO: specify columns"""

        # Default
        if upstream_columns:
            col_list = self._format_column_list(upstream_columns)
            return f"""final as (

    select
        -- TODO: Implement {node.plugin_name} transformation
        {col_list}
    from {source_cte}

)

select
    {col_list}
from final"""
        else:
            return f"""final as (

    select
        -- TODO: Implement {node.plugin_name} transformation
        *  -- TODO: specify columns
    from {source_cte}

)

select * from final  -- TODO: specify columns"""

    def _convert_expression(self, expr: str) -> str:
        """Convert Alteryx expression to Trino SQL with quoted identifiers.

        Uses the FormulaConverter to convert Alteryx functions (IIF, IsNull, IsEmpty,
        string functions, date functions, math functions, etc.) to their Trino SQL
        equivalents. This addresses Issue #6: Alteryx formulas to SQL conversion.

        Reference: https://help.alteryx.com/current/en/designer/functions.html
        """
        if not expr:
            return "NULL"

        # Use the comprehensive formula converter
        sql = self._formula_converter.convert(expr)

        # Track any conversion notes/warnings
        notes = self._formula_converter.get_conversion_notes()
        if notes:
            for note in notes:
                self._add_todo(
                    "expression",
                    f"Review formula conversion: {note}",
                    context=expr[:100] if len(expr) > 100 else expr,
                    priority="medium"
                )

        return sql

    def _find_matching_paren(self, expr: str, start: int) -> int:
        """Find the index of the closing parenthesis matching the one at start."""
        depth = 1
        i = start + 1
        while i < len(expr) and depth > 0:
            if expr[i] == '(':
                depth += 1
            elif expr[i] == ')':
                depth -= 1
            i += 1
        return i - 1 if depth == 0 else -1

    def _split_iif_args(self, args_str: str) -> list:
        """Split IIF arguments respecting nested parentheses and quotes."""
        args = []
        current = ""
        depth = 0
        in_string = False
        string_char = None

        for char in args_str:
            if char in ('"', "'") and not in_string:
                in_string = True
                string_char = char
                current += char
            elif char == string_char and in_string:
                in_string = False
                string_char = None
                current += char
            elif char == '(' and not in_string:
                depth += 1
                current += char
            elif char == ')' and not in_string:
                depth -= 1
                current += char
            elif char == ',' and depth == 0 and not in_string:
                args.append(current.strip())
                current = ""
            else:
                current += char

        if current.strip():
            args.append(current.strip())

        return args

    def _convert_iif_to_case(self, expr: str) -> str:
        """Convert IIF(condition, true_val, false_val) to CASE WHEN ... THEN ... ELSE ... END."""
        result = expr

        # Process IIF functions from innermost to outermost
        max_iterations = 50  # Prevent infinite loops
        iteration = 0

        while iteration < max_iterations:
            # Find IIF( (case insensitive)
            iif_match = re.search(r'\bIIF\s*\(', result, re.IGNORECASE)
            if not iif_match:
                break

            start_idx = iif_match.start()
            paren_start = iif_match.end() - 1  # Index of '('
            paren_end = self._find_matching_paren(result, paren_start)

            if paren_end == -1:
                # Malformed expression, skip
                break

            # Extract arguments
            args_str = result[paren_start + 1:paren_end]
            args = self._split_iif_args(args_str)

            if len(args) >= 3:
                condition = args[0]
                true_val = args[1]
                false_val = args[2]

                # Recursively convert any nested IIF in arguments
                condition = self._convert_iif_to_case(condition)
                true_val = self._convert_iif_to_case(true_val)
                false_val = self._convert_iif_to_case(false_val)

                # Build CASE WHEN statement
                case_expr = f"CASE WHEN {condition} THEN {true_val} ELSE {false_val} END"

                # Replace the IIF(...) with CASE WHEN
                result = result[:start_idx] + case_expr + result[paren_end + 1:]
            else:
                # Not enough arguments, skip this IIF
                break

            iteration += 1

        return result

    def _convert_isnull(self, expr: str) -> str:
        """Convert IsNull(field) to (field IS NULL)."""
        result = expr
        max_iterations = 50
        iteration = 0

        while iteration < max_iterations:
            match = re.search(r'\bIsNull\s*\(', result, re.IGNORECASE)
            if not match:
                break

            start_idx = match.start()
            paren_start = match.end() - 1
            paren_end = self._find_matching_paren(result, paren_start)

            if paren_end == -1:
                break

            field = result[paren_start + 1:paren_end].strip()
            sql_expr = f"({field} IS NULL)"

            result = result[:start_idx] + sql_expr + result[paren_end + 1:]
            iteration += 1

        return result

    def _convert_isempty(self, expr: str) -> str:
        """Convert IsEmpty(field) to (field = '')."""
        result = expr
        max_iterations = 50
        iteration = 0

        while iteration < max_iterations:
            match = re.search(r'\bIsEmpty\s*\(', result, re.IGNORECASE)
            if not match:
                break

            start_idx = match.start()
            paren_start = match.end() - 1
            paren_end = self._find_matching_paren(result, paren_start)

            if paren_end == -1:
                break

            field = result[paren_start + 1:paren_end].strip()
            sql_expr = f"({field} = '')"

            result = result[:start_idx] + sql_expr + result[paren_end + 1:]
            iteration += 1

        return result

    def _generate_schema_yml(self) -> None:
        """Generate schema.yml with proper column names for each layer."""
        # Generate separate schema files for each layer
        for layer in ['bronze', 'silver', 'gold']:
            layer_models = {k: v for k, v in self.models_info.items() if v.layer == layer}

            if not layer_models:
                continue

            content = [
                "version: 2",
                "",
                "models:",
            ]

            for model_name, model_info in sorted(layer_models.items()):
                content.extend([
                    f"  - name: {model_name}",
                    f"    description: \"{model_info.description}\"",
                ])

                # Add columns with proper names
                if model_info.columns:
                    content.append("    columns:")
                    for col in model_info.columns:
                        content.extend([
                            f"      - name: \"{col}\"",
                            f"        description: \"Column {col}\"",
                        ])
                else:
                    # If no columns detected, add a note
                    content.extend([
                        "    columns:",
                        "      # TODO: Add column definitions",
                        "      # - name: \"column_name\"",
                        "      #   description: \"Column description\"",
                    ])

                content.append("")

            self._write_file(
                self.output_dir / "models" / layer / f"_{layer}_schema.yml",
                "\n".join(content)
            )

    def _generate_tests(self) -> None:
        """Generate dbt tests for data quality."""
        # Generate generic tests in schema files (already done via columns)

        # Generate test configuration file
        test_content = [
            "# DBT Tests for Alteryx Migration",
            "#",
            "# This directory contains custom tests for validating migrated data.",
            "#",
            "# Test Types:",
            "# - generic/: Reusable test macros",
            "# - singular/: One-off SQL tests",
            "#",
            "# To run tests: dbt test",
            "",
        ]

        self._write_file(
            self.output_dir / "tests" / "README.md",
            "\n".join(test_content)
        )

        # Generate generic test macros
        generic_tests = [
            "-- Generic test: Check for null values in key columns",
            "-- Usage in schema.yml:",
            "--   tests:",
            "--     - not_null",
            "",
            "{% test not_null_percentage(model, column_name, max_null_pct=0.05) %}",
            "",
            "with validation as (",
            "    select",
            "        count(*) as total_rows,",
            "        sum(case when {{ column_name }} is null then 1 else 0 end) as null_rows",
            "    from {{ model }}",
            ")",
            "",
            "select *",
            "from validation",
            "where cast(null_rows as double) / cast(total_rows as double) > {{ max_null_pct }}",
            "",
            "{% endtest %}",
        ]

        self._write_file(
            self.output_dir / "tests" / "generic" / "test_not_null_percentage.sql",
            "\n".join(generic_tests)
        )

        # Generate singular tests for each gold model
        for model_name, model_info in self.models_info.items():
            if model_info.layer == "gold" and model_info.columns:
                test_name = f"test_{model_name}_row_count"
                test_sql = [
                    f"-- Singular test: Verify {model_name} has data",
                    f"-- This test fails if the model has zero rows",
                    "",
                    f"select count(*) as row_count",
                    f"from {{{{ ref('{model_name}') }}}}",
                    "having count(*) = 0",
                ]

                self._write_file(
                    self.output_dir / "tests" / "singular" / f"{test_name}.sql",
                    "\n".join(test_sql)
                )

        # Generate test schema additions for key columns
        self._generate_test_schema()

    def _generate_test_schema(self) -> None:
        """Generate schema tests for important columns."""
        # Create a tests schema file with common tests
        content = [
            "version: 2",
            "",
            "# Add these tests to your model schema files",
            "# Example test configurations:",
            "",
            "# models:",
            "#   - name: your_model",
            "#     columns:",
            "#       - name: \"id\"",
            "#         tests:",
            "#           - unique",
            "#           - not_null",
            "#       - name: \"foreign_key\"",
            "#         tests:",
            "#           - not_null",
            "#           - relationships:",
            "#               to: ref('other_model')",
            "#               field: \"id\"",
            "",
        ]

        self._write_file(
            self.output_dir / "tests" / "_test_examples.yml",
            "\n".join(content)
        )

    def _generate_validation_tests(self) -> None:
        """Generate validation tests for parallel comparison between Alteryx and DBT.

        This implements the quality testing framework (Issue #5) to support:
        - Record count comparison
        - Data point quantities
        - Null value completeness per output field
        - Layer-to-layer validation (bronze vs raw, silver vs staging, gold vs fed)
        """
        validator = QualityValidator(str(self.output_dir))

        # Generate validation test files
        validation_files = validator.write_validation_outputs(
            self.output_dir, self.models_info
        )
        self.validation_tests_generated.extend(validation_files)

        # Create seed template for expected Alteryx counts
        seed_file = create_validation_seed_template(self.output_dir)
        self.validation_tests_generated.append(seed_file)

        # Generate validation documentation
        from quality_validator import ValidationReport
        report = ValidationReport(
            report_name=f"{self.project_name}_validation",
            total_tables_validated=len(self.models_info),
        )
        validation_doc = validator.generate_validation_documentation(report)

        self._write_file(
            self.output_dir / "docs" / "VALIDATION.md",
            validation_doc
        )
        self.validation_tests_generated.append(
            str(self.output_dir / "docs" / "VALIDATION.md")
        )

        # Add TODO for validation setup
        self._current_model_name = "validation"
        self._current_layer = "tests"
        self._add_todo(
            "validation_setup",
            "Configure Alteryx output sources for parallel validation",
            "Update seeds/alteryx_expected_counts.csv with actual Alteryx output counts",
            priority="high"
        )

    def _generate_project_yml(self) -> None:
        """Generate dbt_project.yml for Starburst/Trino with bronze/silver/gold layers."""
        content = f"""
name: '{self.project_name}'
version: '1.0.0'
config-version: 2

# ============================================================
# TARGET PLATFORM: Starburst (Trino-based)
# ============================================================
# This dbt project is configured for Starburst/Trino.
# Ensure you have dbt-trino adapter installed:
#   pip install dbt-trino
# ============================================================

profile: '{self.project_name}'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  {self.project_name}:
    bronze:
      +materialized: table
      +schema: bronze
    silver:
      +materialized: table
      +schema: silver
    gold:
      +materialized: view
      +schema: gold

tests:
  {self.project_name}:
    +severity: warn

# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
# Migrated from Alteryx ETL workflows to Starburst/Trino ELT.
# Review and customize the models before running.
"""

        self._write_file(self.output_dir / "dbt_project.yml", content.strip())

        # Also generate a profiles.yml template
        profiles_content = f"""
# ============================================================
# Starburst/Trino dbt Profile Configuration
# ============================================================
# Copy this file to ~/.dbt/profiles.yml and configure your connection.
# Documentation: https://docs.getdbt.com/docs/core/connect-data-platform/trino-setup
# ============================================================

{self.project_name}:
  target: dev
  outputs:
    dev:
      type: trino
      method: ldap  # or 'none', 'kerberos', 'oauth', 'jwt', 'certificate'
      host: your-starburst-host.company.com
      port: 443
      user: your_username
      password: your_password  # Or use environment variable
      catalog: your_catalog
      schema: your_schema
      http_scheme: https
      threads: 4

    prod:
      type: trino
      method: ldap
      host: your-starburst-host.company.com
      port: 443
      user: "{{{{ env_var('DBT_USER') }}}}"
      password: "{{{{ env_var('DBT_PASSWORD') }}}}"
      catalog: your_catalog
      schema: your_schema
      http_scheme: https
      threads: 8

# Notes for Starburst Galaxy users:
# - Use method: 'oauth' or 'jwt' for authentication
# - Host format: your-cluster.galaxy.starburst.io
# - See: https://docs.starburst.io/starburst-galaxy/
"""

        self._write_file(self.output_dir / "profiles.yml.template", profiles_content.strip())

    def _sanitize_name(self, name: str) -> str:
        """Sanitize a name for use in DBT."""
        if not name:
            return "unknown"

        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        sanitized = re.sub(r'_+', '_', sanitized)
        sanitized = sanitized.strip('_').lower()

        if len(sanitized) > 50:
            sanitized = sanitized[:50]

        return sanitized or "unknown"

    def _write_file(self, path: Path, content: str) -> None:
        """Write content to a file."""
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
