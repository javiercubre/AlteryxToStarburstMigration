"""
DBT project scaffolding generator for Starburst (Trino).
Generates starter DBT models based on Alteryx workflow analysis.

Target Platform: Starburst (Trino-based)
SQL Dialect: Trino SQL
"""
import os
import re
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass, field

from models import (
    AlteryxWorkflow, AlteryxNode, MedallionLayer, ToolCategory
)
from transformation_analyzer import TransformationAnalyzer
from tool_mappings import get_dbt_prefix, AGGREGATION_MAP


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


class DBTGenerator:
    """Generates DBT project structure from Alteryx workflows."""

    def __init__(self, output_dir: str, project_name: str = "alteryx_migration"):
        self.output_dir = Path(output_dir)
        self.project_name = project_name
        self.sources: Dict[str, Dict[str, SourceInfo]] = {}  # schema -> {table -> SourceInfo}
        self.models_info: Dict[str, ModelInfo] = {}  # model_name -> ModelInfo
        self.models_generated: List[str] = []

    def generate(self, workflows: List[AlteryxWorkflow]) -> None:
        """Generate complete DBT project from workflows."""
        # Create directory structure
        self._create_structure()

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

        # Generate dbt_project.yml
        self._generate_project_yml()

        print(f"DBT project generated at: {self.output_dir}")
        print(f"Models generated: {len(self.models_generated)}")

    def _create_structure(self) -> None:
        """Create DBT project directory structure with bronze/silver/gold naming."""
        dirs = [
            self.output_dir,
            self.output_dir / "models",
            self.output_dir / "models" / "bronze",      # Renamed from staging
            self.output_dir / "models" / "silver",      # Renamed from intermediate
            self.output_dir / "models" / "gold",        # Renamed from marts
            self.output_dir / "macros",
            self.output_dir / "tests",
            self.output_dir / "tests" / "generic",
            self.output_dir / "tests" / "singular",
        ]

        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)

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
        """Determine table name from source node."""
        if node.table_name:
            return self._sanitize_name(node.table_name)

        if node.source_path:
            return self._sanitize_name(Path(node.source_path).stem)

        return f"source_{node.tool_id}"

    def _get_descriptive_gold_name(self, node: AlteryxNode, workflow: AlteryxWorkflow) -> str:
        """Generate a descriptive name for gold layer models based on actual output."""
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

        # Fallback
        return self._sanitize_name(node.get_display_name())

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
        analyzer = TransformationAnalyzer(workflow)
        medallion = analyzer.suggest_medallion_mapping()

        workflow_prefix = self._sanitize_name(workflow.metadata.name)

        # Generate bronze models (staging)
        bronze_nodes = medallion.get(MedallionLayer.BRONZE.value, [])
        for node in bronze_nodes:
            if node.category == ToolCategory.INPUT:
                self._generate_bronze_model(node, workflow_prefix)

        # Generate silver models (intermediate)
        silver_nodes = medallion.get(MedallionLayer.SILVER.value, [])
        for node in silver_nodes:
            self._generate_silver_model(node, workflow_prefix, workflow)

        # Generate gold models (marts)
        gold_nodes = medallion.get(MedallionLayer.GOLD.value, [])
        for node in gold_nodes:
            self._generate_gold_model(node, workflow_prefix, workflow)

    def _generate_bronze_model(self, node: AlteryxNode, workflow_prefix: str) -> None:
        """Generate a bronze (staging) model with table materialization."""
        schema = self._get_schema_name(node)
        table = self._get_table_name(node)
        model_name = f"stg_{workflow_prefix}_{table}"

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
            f"    select * from {{{{ source('{schema}', '{table}') }}}}",
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
            "select * from renamed",
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
                content.extend([
                    f"with {cte_name} as (",
                    "",
                    f"    select * from {{{{ ref('{up_model}') }}}}",
                    "",
                    ")," if i < len(upstream) - 1 else "),",
                    "",
                ])

        # Generate transformation logic based on tool type
        sql = self._generate_transformation_sql(node, upstream)
        content.append(sql)

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
                content.extend([
                    f"with {cte_name} as (",
                    "",
                    f"    select * from {{{{ ref('{up_model}') }}}}",
                    "",
                    ")," if i < len(upstream) - 1 else "),",
                    "",
                ])

        # Generate transformation logic
        sql = self._generate_transformation_sql(node, upstream)
        content.append(sql)

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

    def _generate_transformation_sql(self, node: AlteryxNode,
                                      upstream: List[AlteryxNode]) -> str:
        """Generate SQL for a transformation node with double-quoted columns."""
        source_cte = "source" if len(upstream) <= 1 else "source_1"

        if node.plugin_name == "Filter":
            condition = self._convert_expression(node.expression or "1=1")
            return f"""final as (

    select *
    from {source_cte}
    where {condition}

)

select * from final"""

        elif node.plugin_name in ["Formula", "Multi-Field Formula"]:
            formulas = node.configuration.get('formulas', [])
            if formulas:
                select_parts = ["    *"]
                for f in formulas:
                    field = self._quote_column(f.get('field', 'new_field'))
                    expr = self._convert_expression(f.get('expression', 'NULL'))
                    select_parts.append(f"    , {expr} as {field}")

                return f"""final as (

    select
{chr(10).join(select_parts)}
    from {source_cte}

)

select * from final"""
            else:
                return f"select * from {source_cte}"

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

            return f"""final as (

    select
        source_1.*
        -- Add columns from source_2 as needed
    from source_1
    {join_type.lower()} join source_2
        on {join_condition}

)

select * from final"""

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

            return f"""final as (

    select
        {select_clause}{agg_clause}
    from {source_cte}
    group by {group_cols}

)

select * from final"""

        elif node.plugin_name == "Union":
            if len(upstream) > 1:
                union_parts = []
                for i in range(len(upstream)):
                    union_parts.append(f"select * from source_{i + 1}")
                return "\n\nunion all\n\n".join(union_parts)
            return f"select * from {source_cte}"

        elif node.plugin_name == "Select":
            if node.selected_fields:
                # Quote all field names
                quoted_fields = [self._quote_column(f.split(' AS ')[0].strip()) +
                                (' as ' + self._quote_column(f.split(' AS ')[1].strip()) if ' AS ' in f.upper() else '')
                                for f in node.selected_fields[:20]]
                fields = ",\n        ".join(quoted_fields)
                return f"""final as (

    select
        {fields}
    from {source_cte}

)

select * from final"""

        elif node.plugin_name == "Sort":
            sort_fields = node.configuration.get('sort_fields', [])
            if sort_fields:
                order_parts = []
                for sf in sort_fields:
                    direction = "asc" if sf.get('order', 'Ascending') == 'Ascending' else "desc"
                    field = self._quote_column(sf['field'])
                    order_parts.append(f"{field} {direction}")
                order_clause = ", ".join(order_parts)
                return f"""final as (

    select *
    from {source_cte}
    order by {order_clause}

)

select * from final"""

        # Default
        return f"""final as (

    select
        -- TODO: Implement {node.plugin_name} transformation
        *
    from {source_cte}

)

select * from final"""

    def _convert_expression(self, expr: str) -> str:
        """Convert Alteryx expression to Trino SQL with quoted identifiers."""
        if not expr:
            return "NULL"

        sql = expr

        # Replace field references [FieldName] with "FieldName"
        sql = re.sub(r'\[([^\]]+)\]', r'"\1"', sql)

        # Basic function replacements
        replacements = {
            'IsNull(': 'is null -- ',
            'IsEmpty(': "= '' -- ",
            'IIF(': 'case when ',
            ', True, False)': ' then true else false end',
            'ENDIF': 'end',
            '==': '=',
            '&&': 'and',
            '||': 'or',
        }

        for old, new in replacements.items():
            sql = sql.replace(old, new)

        return sql

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
