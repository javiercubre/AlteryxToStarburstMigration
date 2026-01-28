"""
Transformation analyzer for data lineage and flow analysis.
"""
from typing import List, Dict, Set, Optional, Tuple
from collections import defaultdict

from models import (
    AlteryxWorkflow, AlteryxNode, AlteryxConnection,
    TransformationStep, DataLineage, ToolCategory, MedallionLayer
)
from tool_mappings import get_medallion_layer, get_sql_mapping, AGGREGATION_MAP


class TransformationAnalyzer:
    """Analyzes workflow transformations and builds data lineage."""

    def __init__(self, workflow: AlteryxWorkflow):
        self.workflow = workflow
        self._build_graph()

    def _build_graph(self):
        """Build adjacency lists for graph traversal."""
        # Forward adjacency (downstream)
        self.downstream: Dict[int, List[int]] = defaultdict(list)
        # Backward adjacency (upstream)
        self.upstream: Dict[int, List[int]] = defaultdict(list)

        for conn in self.workflow.connections:
            self.downstream[conn.origin_id].append(conn.destination_id)
            self.upstream[conn.destination_id].append(conn.origin_id)

    def get_ordered_transformations(self) -> List[TransformationStep]:
        """Get all transformations in execution order using topological sort."""
        # Find execution order via topological sort
        visited = set()
        order = []

        def dfs(node_id: int):
            if node_id in visited:
                return
            visited.add(node_id)

            # Visit all upstream nodes first
            for upstream_id in self.upstream.get(node_id, []):
                dfs(upstream_id)

            order.append(node_id)

        # Start DFS from all nodes
        for node in self.workflow.nodes:
            dfs(node.tool_id)

        # Create transformation steps
        steps = []
        for idx, tool_id in enumerate(order):
            node = self.workflow.get_node_by_id(tool_id)
            if node:
                step = self._create_transformation_step(node, idx + 1)
                steps.append(step)

        return steps

    def _create_transformation_step(self, node: AlteryxNode, order: int) -> TransformationStep:
        """Create a transformation step from a node."""
        # Determine medallion layer
        is_final = not self.downstream.get(node.tool_id)  # No downstream = final output
        layer = get_medallion_layer(node.category, is_final)

        # Generate description
        description = self._generate_step_description(node)

        # Generate DBT hint
        dbt_hint = self._generate_dbt_hint(node)

        return TransformationStep(
            order=order,
            tool_id=node.tool_id,
            tool_name=node.get_display_name(),
            category=node.category,
            description=description,
            expression=node.expression,
            medallion_layer=layer,
            dbt_hint=dbt_hint,
        )

    def _generate_step_description(self, node: AlteryxNode) -> str:
        """Generate a human-readable description of a transformation step."""
        desc_parts = [node.plugin_name]

        if node.category == ToolCategory.INPUT:
            if node.source_path:
                desc_parts.append(f"from '{node.source_path}'")
            elif node.table_name:
                desc_parts.append(f"from table '{node.table_name}'")
            elif node.sql_query:
                desc_parts.append("with custom SQL query")

        elif node.category == ToolCategory.OUTPUT:
            if node.target_path:
                desc_parts.append(f"to '{node.target_path}'")
            elif node.table_name:
                desc_parts.append(f"to table '{node.table_name}'")

        elif node.plugin_name == "Filter":
            if node.expression:
                desc_parts.append(f": {node.expression}")

        elif node.plugin_name in ["Formula", "Multi-Field Formula"]:
            formulas = node.configuration.get('formulas', [])
            if formulas:
                fields = [f.get('field', '') for f in formulas[:3]]
                desc_parts.append(f": {', '.join(fields)}")
                if len(formulas) > 3:
                    desc_parts.append(f" (+{len(formulas) - 3} more)")

        elif node.plugin_name == "Join":
            if node.join_keys:
                desc_parts.append(f"on {', '.join(node.join_keys[:2])}")

        elif node.plugin_name == "Summarize":
            if node.group_by_fields:
                desc_parts.append(f"GROUP BY {', '.join(node.group_by_fields[:3])}")
            if node.aggregations:
                agg_desc = [f"{a['action']}({a['field']})" for a in node.aggregations[:2]]
                desc_parts.append(f": {', '.join(agg_desc)}")

        elif node.plugin_name == "Select":
            if node.selected_fields:
                desc_parts.append(f": {len(node.selected_fields)} fields")

        elif node.plugin_name == "Sort":
            sort_fields = node.configuration.get('sort_fields', [])
            if sort_fields:
                fields = [f"{sf['field']} {sf['order']}" for sf in sort_fields[:2]]
                desc_parts.append(f"by {', '.join(fields)}")

        elif node.is_macro:
            desc_parts = [f"Macro: {node.plugin_name}"]
            if node.macro_path:
                desc_parts.append(f"({node.macro_path})")

        return ' '.join(desc_parts)

    def _generate_dbt_hint(self, node: AlteryxNode) -> str:
        """Generate a DBT/SQL hint for a transformation, preferring macro references."""
        mapping = get_sql_mapping(node.plugin_name)

        # Prefer showing the macro call if available
        if 'macro' in mapping and 'dbt' in mapping:
            macro_name = mapping['macro']
            dbt_example = mapping['dbt']
            macro_file = mapping.get('macro_file', 'migration')
            return f"-- Using macro: {macro_name} (from {macro_file}.sql)\n{dbt_example}"

        sql_template = mapping.get('sql', '-- Custom logic required')

        if node.category == ToolCategory.INPUT:
            if node.table_name:
                return f"{{{{ source('schema', '{node.table_name}') }}}}"
            elif node.source_path:
                return f"-- External file: {node.source_path}\n-- Consider loading to staging table"
            return sql_template

        elif node.plugin_name == "Filter":
            if node.expression:
                # Convert Alteryx expression syntax to SQL
                sql_expr = self._convert_alteryx_expression(node.expression)
                return f"WHERE {sql_expr}"

        elif node.plugin_name in ["Formula", "Multi-Field Formula"]:
            formulas = node.configuration.get('formulas', [])
            if formulas:
                lines = []
                for f in formulas:
                    expr = self._convert_alteryx_expression(f.get('expression', ''))
                    field = f.get('field', 'new_field')
                    lines.append(f"  {expr} AS {field}")
                return "SELECT\n" + ",\n".join(lines)

        elif node.plugin_name == "Join":
            join_type = node.join_type or "LEFT"
            if node.join_keys:
                conditions = []
                for key in node.join_keys:
                    parts = key.split('=')
                    if len(parts) == 2:
                        conditions.append(f"left_table.{parts[0].strip()} = right_table.{parts[1].strip()}")
                return f"{join_type} JOIN right_table ON {' AND '.join(conditions)}"

        elif node.plugin_name == "Summarize":
            parts = []
            if node.group_by_fields:
                parts.append(f"GROUP BY {', '.join(node.group_by_fields)}")
            if node.aggregations:
                agg_parts = []
                for agg in node.aggregations:
                    action = agg.get('action', 'COUNT')
                    field = agg.get('field', '*')
                    output = agg.get('output_name', field)
                    sql_func = AGGREGATION_MAP.get(action, action.upper())

                    if sql_func.endswith('(DISTINCT'):
                        agg_parts.append(f"{sql_func} {field}) AS {output}")
                    else:
                        agg_parts.append(f"{sql_func}({field}) AS {output}")
                parts.insert(0, f"SELECT {', '.join(agg_parts)}")
            return '\n'.join(parts)

        elif node.plugin_name == "Union":
            return "UNION ALL\n-- Stack multiple inputs"

        elif node.plugin_name == "Select":
            if node.selected_fields:
                return f"SELECT {', '.join(node.selected_fields[:10])}"

        return sql_template

    def _convert_alteryx_expression(self, expr: str) -> str:
        """Convert Alteryx expression syntax to SQL.

        Properly parses and converts Alteryx functions like IIF(), IsNull(), IsEmpty()
        to their ANSI SQL equivalents.
        """
        if not expr:
            return expr

        import re

        sql = expr.strip()

        # Replace Alteryx field references [FieldName] with SQL column references
        sql = re.sub(r'\[([^\]]+)\]', r'"\1"', sql)

        # Replace operators
        sql = sql.replace('==', '=')
        sql = sql.replace('!=', '<>')
        sql = sql.replace('&&', ' AND ')
        sql = sql.replace('||', ' OR ')

        # Convert Alteryx functions to SQL
        sql = self._convert_iif_to_case(sql)
        sql = self._convert_isnull(sql)
        sql = self._convert_isempty(sql)

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
        import re
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
        import re
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
        import re
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

    def get_data_lineage(self) -> List[DataLineage]:
        """Trace data lineage from all sources to all targets."""
        lineages = []

        for source in self.workflow.sources:
            for target in self.workflow.targets:
                paths = self._find_all_paths(source.tool_id, target.tool_id)
                for path_ids in paths:
                    path_nodes = [self.workflow.get_node_by_id(tid) for tid in path_ids]
                    path_nodes = [n for n in path_nodes if n is not None]

                    if path_nodes:
                        # Get transformation steps for this path
                        steps = []
                        for idx, node in enumerate(path_nodes):
                            step = self._create_transformation_step(node, idx + 1)
                            steps.append(step)

                        lineage = DataLineage(
                            source=source,
                            target=target,
                            path=path_nodes,
                            transformations=steps,
                        )
                        lineages.append(lineage)

        return lineages

    def _find_all_paths(self, start_id: int, end_id: int, max_paths: int = 10) -> List[List[int]]:
        """Find all paths from start to end node (limited to prevent explosion)."""
        paths = []

        def dfs(current: int, path: List[int], visited: Set[int]):
            if len(paths) >= max_paths:
                return

            if current == end_id:
                paths.append(path.copy())
                return

            for next_id in self.downstream.get(current, []):
                if next_id not in visited:
                    visited.add(next_id)
                    path.append(next_id)
                    dfs(next_id, path, visited)
                    path.pop()
                    visited.remove(next_id)

        dfs(start_id, [start_id], {start_id})
        return paths

    def get_source_inventory(self) -> List[Dict]:
        """Get inventory of all data sources."""
        sources = []

        for node in self.workflow.sources:
            source_info = {
                'name': node.get_display_name(),
                'tool_id': node.tool_id,
                'type': self._determine_source_type(node),
                'path': node.source_path or node.table_name or 'N/A',
                'connection': node.connection_string,
                'sql_query': node.sql_query,
            }
            sources.append(source_info)

        return sources

    def get_target_inventory(self) -> List[Dict]:
        """Get inventory of all output targets."""
        targets = []

        for node in self.workflow.targets:
            target_info = {
                'name': node.get_display_name(),
                'tool_id': node.tool_id,
                'type': self._determine_target_type(node),
                'path': node.target_path or node.table_name or 'N/A',
                'connection': node.connection_string,
            }
            targets.append(target_info)

        return targets

    def _determine_source_type(self, node: AlteryxNode) -> str:
        """Determine the type of data source."""
        if node.source_path:
            path_lower = node.source_path.lower()
            if path_lower.endswith('.csv'):
                return 'CSV File'
            elif path_lower.endswith(('.xls', '.xlsx')):
                return 'Excel File'
            elif path_lower.endswith('.json'):
                return 'JSON File'
            elif path_lower.endswith('.xml'):
                return 'XML File'
            elif path_lower.endswith('.yxdb'):
                return 'Alteryx Database'
            else:
                return 'File'

        if node.connection_string:
            conn_lower = node.connection_string.lower()
            if 'sqlserver' in conn_lower or 'mssql' in conn_lower:
                return 'SQL Server'
            elif 'oracle' in conn_lower:
                return 'Oracle'
            elif 'postgres' in conn_lower:
                return 'PostgreSQL'
            elif 'mysql' in conn_lower:
                return 'MySQL'
            elif 'snowflake' in conn_lower:
                return 'Snowflake'
            elif 'bigquery' in conn_lower:
                return 'BigQuery'
            elif 'redshift' in conn_lower:
                return 'Redshift'
            else:
                return 'Database'

        if 'S3' in node.tool_type:
            return 'Amazon S3'
        elif 'Azure' in node.tool_type:
            return 'Azure Blob'
        elif 'Snowflake' in node.tool_type:
            return 'Snowflake'

        return 'Unknown'

    def _determine_target_type(self, node: AlteryxNode) -> str:
        """Determine the type of output target."""
        if node.plugin_name == "Browse":
            return 'Preview/Browse'

        if node.target_path:
            path_lower = node.target_path.lower()
            if path_lower.endswith('.csv'):
                return 'CSV File'
            elif path_lower.endswith(('.xls', '.xlsx')):
                return 'Excel File'
            elif path_lower.endswith('.json'):
                return 'JSON File'
            elif path_lower.endswith('.yxdb'):
                return 'Alteryx Database'
            else:
                return 'File'

        if node.connection_string or node.table_name:
            return 'Database Table'

        return 'Unknown'

    def suggest_medallion_mapping(self) -> Dict[str, List[AlteryxNode]]:
        """Suggest medallion layer assignments for workflow nodes."""
        mapping = {
            MedallionLayer.BRONZE.value: [],
            MedallionLayer.SILVER.value: [],
            MedallionLayer.GOLD.value: [],
        }

        for node in self.workflow.nodes:
            # Skip containers - they don't process data
            if node.category == ToolCategory.CONTAINER:
                continue

            is_final = not self.downstream.get(node.tool_id)
            layer = get_medallion_layer(node.category, is_final)

            # Skip if layer is None (e.g., containers)
            if layer is None:
                continue

            # Adjust layer based on position in workflow
            if node.category == ToolCategory.TRANSFORM:
                # If it's a summarize tool at the end, it's likely Gold
                if node.plugin_name == "Summarize" and is_final:
                    layer = MedallionLayer.GOLD

            mapping[layer.value].append(node)

        return mapping
