"""
Mappings from Alteryx tools to DBT macros.
Links Alteryx tool types to the corresponding DBT macro implementations.

Target Platform: Starburst (Trino-based)
Macro Location: dbt_macros/ directory
"""

# Comprehensive mapping of Alteryx tools to DBT macros
TOOL_MACRO_MAP = {
    # Preparation Tools
    "Filter": {
        "macro": "filter_expression",
        "macro_file": "filter_helpers",
        "description": "Filter rows based on condition",
        "param_mapping": {
            "expression": "condition",
        },
        "requires_relation": True,
    },

    "Formula": {
        "macro": "add_calculated_column",
        "macro_file": "formula_helpers",
        "description": "Add calculated columns with expressions",
        "param_mapping": {
            "formulas": "columns_to_add",  # List of {field, expression} dicts
        },
        "requires_relation": True,
        "multi_column": True,  # Can add multiple columns at once
    },

    "Multi-Field Formula": {
        "macro": "add_multiple_columns",
        "macro_file": "formula_helpers",
        "description": "Add multiple calculated columns",
        "param_mapping": {
            "formulas": "columns",
        },
        "requires_relation": True,
    },

    "Select": {
        "macro": "select_columns",
        "macro_file": "select_transform",
        "description": "Select and optionally rename columns",
        "param_mapping": {
            "selected_fields": "columns",
        },
        "requires_relation": True,
    },

    "Sort": {
        "macro": "sort_data",
        "macro_file": "select_transform",
        "description": "Sort data by columns",
        "param_mapping": {
            "sort_fields": "order_by",
        },
        "requires_relation": True,
    },

    "Sample": {
        "macro": "sample_first_n",
        "macro_file": "sample_limit",
        "description": "Sample first N records",
        "param_mapping": {
            "sample_size": "n",
        },
        "requires_relation": True,
        "alternates": {
            "random": "sample_random_n",
            "percent": "sample_random_percent",
        }
    },

    "Unique": {
        "macro": "deduplicate",
        "macro_file": "deduplicate",
        "description": "Remove duplicate records",
        "param_mapping": {
            "selected_fields": "partition_by",
        },
        "requires_relation": True,
    },

    "Data Cleansing": {
        "macro": "clean_string",
        "macro_file": "null_if_empty",
        "description": "Clean and standardize string data",
        "param_mapping": {
            "column": "column_name",
        },
        "requires_relation": True,
    },

    "Record ID": {
        "macro": "generate_record_id",
        "macro_file": "generate_surrogate_key",
        "description": "Add sequential record IDs",
        "param_mapping": {},
        "requires_relation": True,
    },

    "Auto Field": {
        "macro": "auto_type_columns",
        "macro_file": "select_transform",
        "description": "Automatically detect and set field types",
        "param_mapping": {},
        "requires_relation": True,
    },

    "Multi-Row Formula": {
        "macro": "lag_lead",
        "macro_file": "window_rank",
        "description": "Reference previous/next rows",
        "param_mapping": {
            "expression": "expression",
            "num_rows": "offset",
        },
        "requires_relation": True,
    },

    "Find Replace": {
        "macro": "find_replace_simple",
        "macro_file": "find_replace",
        "description": "Find and replace values",
        "param_mapping": {
            "column": "column_name",
            "find_value": "find_text",
            "replace_value": "replace_text",
        },
        "requires_relation": True,
    },

    "Imputation": {
        "macro": "impute_with_value",
        "macro_file": "imputation",
        "description": "Fill missing values",
        "param_mapping": {
            "column": "column_name",
            "fill_value": "fill_value",
        },
        "requires_relation": True,
    },

    "Select Records": {
        "macro": "select_records_by_range",
        "macro_file": "sample_limit",
        "description": "Select specific record ranges",
        "param_mapping": {
            "start_row": "start_index",
            "end_row": "end_index",
        },
        "requires_relation": True,
    },

    "Tile": {
        "macro": "tile_equal_records",
        "macro_file": "tile_bucket",
        "description": "Divide data into tiles",
        "param_mapping": {
            "num_tiles": "num_tiles",
        },
        "requires_relation": True,
    },

    # Join Tools
    "Join": {
        "macro": "left_join",  # Default to left join
        "macro_file": "join_union",
        "description": "Join two datasets",
        "param_mapping": {
            "join_keys": "join_columns",
            "join_type": "join_type",
        },
        "requires_relation": True,
        "requires_right_relation": True,
        "alternates": {
            "INNER": "inner_join",
            "LEFT": "left_join",
            "RIGHT": "right_join",
            "FULL": "full_outer_join",
        }
    },

    "Union": {
        "macro": "union_all",
        "macro_file": "join_union",
        "description": "Stack datasets vertically",
        "param_mapping": {},
        "requires_relation": True,
        "multi_relation": True,
    },

    "Append Fields": {
        "macro": "cross_join",
        "macro_file": "join_union",
        "description": "Cartesian product (cross join)",
        "param_mapping": {},
        "requires_relation": True,
        "requires_right_relation": True,
    },

    "Join Multiple": {
        "macro": "join_multiple",
        "macro_file": "join_union",
        "description": "Join multiple tables",
        "param_mapping": {
            "join_specs": "joins",
        },
        "requires_relation": True,
        "multi_relation": True,
    },

    # Transform Tools
    "Summarize": {
        "macro": "summarize",
        "macro_file": "aggregation",
        "description": "Aggregate data with GROUP BY",
        "param_mapping": {
            "group_by_fields": "group_by",
            "aggregations": "agg_fields",
        },
        "requires_relation": True,
    },

    "Transpose": {
        "macro": "unpivot",
        "macro_file": "pivot",
        "description": "Convert columns to rows",
        "param_mapping": {
            "key_columns": "id_cols",
            "value_columns": "value_cols",
        },
        "requires_relation": True,
    },

    "Cross Tab": {
        "macro": "pivot",
        "macro_file": "pivot",
        "description": "Convert rows to columns (pivot)",
        "param_mapping": {
            "group_by": "id_cols",
            "pivot_column": "pivot_col",
            "value_column": "value_col",
        },
        "requires_relation": True,
    },

    "Count Records": {
        "macro": "count_records",
        "macro_file": "aggregation",
        "description": "Count number of records",
        "param_mapping": {},
        "requires_relation": True,
    },

    "Running Total": {
        "macro": "running_total",
        "macro_file": "running_total",
        "description": "Calculate cumulative sum",
        "param_mapping": {
            "column": "column_name",
            "partition_by": "partition_by",
            "order_by": "order_by",
        },
        "requires_relation": True,
    },

    "Weighted Average": {
        "macro": "weighted_average",
        "macro_file": "aggregation",
        "description": "Calculate weighted average",
        "param_mapping": {
            "value_column": "value_col",
            "weight_column": "weight_col",
        },
        "requires_relation": True,
    },

    "Arrange": {
        "macro": "sort_data",
        "macro_file": "select_transform",
        "description": "Sort and reorder data",
        "param_mapping": {
            "sort_fields": "order_by",
        },
        "requires_relation": True,
    },

    # Parse Tools
    "RegEx": {
        "macro": "regex_extract",
        "macro_file": "regex_functions",
        "description": "Extract text using regex",
        "param_mapping": {
            "column": "column_name",
            "pattern": "pattern",
        },
        "requires_relation": True,
        "alternates": {
            "extract": "regex_extract",
            "replace": "regex_replace",
            "match": "regex_match",
        }
    },

    "Text To Columns": {
        "macro": "split_to_columns",
        "macro_file": "split_unnest",
        "description": "Split delimited text into columns",
        "param_mapping": {
            "column": "column_name",
            "delimiter": "delimiter",
        },
        "requires_relation": True,
    },

    "JSON Parse": {
        "macro": "json_extract_path_text",
        "macro_file": "formula_helpers",
        "description": "Parse JSON data",
        "param_mapping": {
            "column": "json_column",
            "path": "path",
        },
        "requires_relation": True,
    },

    "Date Time Parse": {
        "macro": "to_timestamp",
        "macro_file": "safe_cast",
        "description": "Parse date/time strings",
        "param_mapping": {
            "column": "column_name",
            "format": "format_string",
        },
        "requires_relation": True,
    },

    # Input Tools
    "Generate Rows": {
        "macro": "date_spine",
        "macro_file": "date_spine",
        "description": "Generate sequential rows/dates",
        "param_mapping": {
            "start_value": "start_date",
            "end_value": "end_date",
        },
        "requires_relation": False,
    },
}


def get_macro_for_tool(tool_name: str, context: dict = None) -> dict:
    """
    Get the appropriate macro information for an Alteryx tool.

    Args:
        tool_name: Name of the Alteryx tool (e.g., "Filter", "Join")
        context: Optional context dict with additional info (e.g., join_type, operation)

    Returns:
        Dict with macro information or None if no mapping exists
    """
    macro_info = TOOL_MACRO_MAP.get(tool_name)
    if not macro_info:
        return None

    # Check for alternate macros based on context
    if context and "alternates" in macro_info:
        alternates = macro_info["alternates"]

        # For joins, use the specific join type
        if tool_name == "Join" and "join_type" in context:
            join_type = context["join_type"].upper()
            if join_type in alternates:
                macro_info = macro_info.copy()
                macro_info["macro"] = alternates[join_type]

        # For sample, use the appropriate sampling method
        elif tool_name == "Sample" and "sample_type" in context:
            sample_type = context["sample_type"]
            if sample_type in alternates:
                macro_info = macro_info.copy()
                macro_info["macro"] = alternates[sample_type]

        # For regex, use the appropriate operation
        elif tool_name == "RegEx" and "operation" in context:
            operation = context["operation"]
            if operation in alternates:
                macro_info = macro_info.copy()
                macro_info["macro"] = alternates[operation]

    return macro_info


def get_all_macro_files() -> set:
    """
    Get a set of all macro file names referenced in the mappings.

    Returns:
        Set of macro file names (without .sql extension)
    """
    macro_files = set()
    for tool_info in TOOL_MACRO_MAP.values():
        if "macro_file" in tool_info:
            macro_files.add(tool_info["macro_file"])
    return macro_files


def get_tools_for_macro_file(macro_file: str) -> list:
    """
    Get all Alteryx tools that map to a specific macro file.

    Args:
        macro_file: Name of the macro file (without .sql extension)

    Returns:
        List of tool names
    """
    tools = []
    for tool_name, tool_info in TOOL_MACRO_MAP.items():
        if tool_info.get("macro_file") == macro_file:
            tools.append(tool_name)
    return tools


def get_macro_coverage_stats() -> dict:
    """
    Get statistics about macro coverage.

    Returns:
        Dict with coverage statistics
    """
    from tool_mappings import PLUGIN_NAME_MAP

    total_tools = len(PLUGIN_NAME_MAP)
    covered_tools = len(TOOL_MACRO_MAP)
    coverage_pct = (covered_tools / total_tools * 100) if total_tools > 0 else 0

    return {
        "total_alteryx_tools": total_tools,
        "tools_with_macros": covered_tools,
        "coverage_percentage": round(coverage_pct, 1),
        "macro_files": len(get_all_macro_files()),
        "tools_without_macros": total_tools - covered_tools,
    }
