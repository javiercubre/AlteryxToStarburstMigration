"""
Alteryx tool type mappings and Trino SQL/DBT equivalents.

Target Platform: Starburst (Trino-based)
SQL Dialect: Trino SQL
"""
from models import ToolCategory, MedallionLayer

# Map Alteryx plugin names to categories
PLUGIN_CATEGORY_MAP = {
    # Input Tools
    "AlteryxBasePluginsGui.DbFileInput.DbFileInput": ToolCategory.INPUT,
    "AlteryxBasePluginsGui.TextInput.TextInput": ToolCategory.INPUT,
    "AlteryxBasePluginsGui.MacroInput.MacroInput": ToolCategory.INPUT,
    "AlteryxGuiToolkit.TextBox.TextBox": ToolCategory.INPUT,
    "LiterxPluginsGui.LiterxInput.LiterxInput": ToolCategory.INPUT,
    "AlteryxConnectGui.AzureBlobInput.AzureBlobInput": ToolCategory.INPUT,
    "AlteryxConnectGui.S3Input.S3Input": ToolCategory.INPUT,
    "AlteryxConnectGui.SnowflakeInput.SnowflakeInput": ToolCategory.INPUT,
    "AlteryxConnectGui.BigQueryInput.BigQueryInput": ToolCategory.INPUT,
    "AlteryxConnectGui.RedshiftInput.RedshiftInput": ToolCategory.INPUT,
    "AlteryxBasePluginsGui.DateTime.DateTime": ToolCategory.INPUT,
    "AlteryxBasePluginsGui.GenerateRows.GenerateRows": ToolCategory.INPUT,
    "AlteryxBasePluginsGui.Directory.Directory": ToolCategory.INPUT,

    # Output Tools
    "AlteryxBasePluginsGui.DbFileOutput.DbFileOutput": ToolCategory.OUTPUT,
    "AlteryxBasePluginsGui.BrowseV2.BrowseV2": ToolCategory.OUTPUT,
    "AlteryxBasePluginsGui.MacroOutput.MacroOutput": ToolCategory.OUTPUT,
    "AlteryxConnectGui.AzureBlobOutput.AzureBlobOutput": ToolCategory.OUTPUT,
    "AlteryxConnectGui.S3Output.S3Output": ToolCategory.OUTPUT,
    "AlteryxConnectGui.SnowflakeOutput.SnowflakeOutput": ToolCategory.OUTPUT,
    "AlteryxConnectGui.BigQueryOutput.BigQueryOutput": ToolCategory.OUTPUT,
    "AlteryxConnectGui.RedshiftOutput.RedshiftOutput": ToolCategory.OUTPUT,

    # Preparation Tools
    "AlteryxBasePluginsGui.Filter.Filter": ToolCategory.PREPARATION,
    "AlteryxBasePluginsGui.Formula.Formula": ToolCategory.PREPARATION,
    "AlteryxBasePluginsGui.MultiFieldFormula.MultiFieldFormula": ToolCategory.PREPARATION,
    "AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect": ToolCategory.PREPARATION,
    "AlteryxBasePluginsGui.Sort.Sort": ToolCategory.PREPARATION,
    "AlteryxBasePluginsGui.Sample.Sample": ToolCategory.PREPARATION,
    "AlteryxBasePluginsGui.Unique.Unique": ToolCategory.PREPARATION,
    "AlteryxBasePluginsGui.DataCleansing.DataCleansing": ToolCategory.PREPARATION,
    "AlteryxBasePluginsGui.RecordID.RecordID": ToolCategory.PREPARATION,
    "AlteryxBasePluginsGui.AutoField.AutoField": ToolCategory.PREPARATION,
    "AlteryxBasePluginsGui.MultiRowFormula.MultiRowFormula": ToolCategory.PREPARATION,
    "AlteryxBasePluginsGui.FindReplace.FindReplace": ToolCategory.PREPARATION,
    "AlteryxBasePluginsGui.Imputation.Imputation": ToolCategory.PREPARATION,
    "AlteryxBasePluginsGui.RandomSample.Random%Sample": ToolCategory.PREPARATION,
    "AlteryxBasePluginsGui.SelectRecords.SelectRecords": ToolCategory.PREPARATION,
    "AlteryxBasePluginsGui.Tile.Tile": ToolCategory.PREPARATION,

    # Join Tools
    "AlteryxBasePluginsGui.Join.Join": ToolCategory.JOIN,
    "AlteryxBasePluginsGui.Union.Union": ToolCategory.JOIN,
    "AlteryxBasePluginsGui.AppendFields.AppendFields": ToolCategory.JOIN,
    "AlteryxBasePluginsGui.JoinMultiple.JoinMultiple": ToolCategory.JOIN,
    "AlteryxBasePluginsGui.FindReplace.FindReplace": ToolCategory.JOIN,
    "AlteryxBasePluginsGui.MakeGroup.MakeGroup": ToolCategory.JOIN,
    "AlteryxSpatialPluginsGui.SpatialMatch.SpatialMatch": ToolCategory.JOIN,
    "AlteryxBasePluginsGui.Fuzzy.Fuzzy": ToolCategory.JOIN,

    # Transform Tools
    "AlteryxBasePluginsGui.Summarize.Summarize": ToolCategory.TRANSFORM,
    "AlteryxBasePluginsGui.Transpose.Transpose": ToolCategory.TRANSFORM,
    "AlteryxBasePluginsGui.CrossTab.CrossTab": ToolCategory.TRANSFORM,
    "AlteryxBasePluginsGui.CountRecords.CountRecords": ToolCategory.TRANSFORM,
    "AlteryxBasePluginsGui.RunningTotal.RunningTotal": ToolCategory.TRANSFORM,
    "AlteryxBasePluginsGui.WeightedAverage.WeightedAverage": ToolCategory.TRANSFORM,
    "AlteryxBasePluginsGui.Arrange.Arrange": ToolCategory.TRANSFORM,

    # Parse Tools
    "AlteryxBasePluginsGui.RegEx.RegEx": ToolCategory.PARSE,
    "AlteryxBasePluginsGui.TextToColumns.TextToColumns": ToolCategory.PARSE,
    "AlteryxBasePluginsGui.XMLParse.XMLParse": ToolCategory.PARSE,
    "AlteryxBasePluginsGui.JSONParse.JSONParse": ToolCategory.PARSE,
    "AlteryxBasePluginsGui.DateTimeParse.DateTimeParse": ToolCategory.PARSE,
    "AlteryxBasePluginsGui.Dynamic.Dynamic": ToolCategory.PARSE,

    # In-Database Tools
    "AlteryxBasePluginsGui.DbFileInputInDB.DbFileInputInDB": ToolCategory.IN_DATABASE,
    "AlteryxBasePluginsGui.DbFileOutputInDB.DbFileOutputInDB": ToolCategory.IN_DATABASE,
    "AlteryxBasePluginsGui.FilterInDB.FilterInDB": ToolCategory.IN_DATABASE,
    "AlteryxBasePluginsGui.FormulaInDB.FormulaInDB": ToolCategory.IN_DATABASE,
    "AlteryxBasePluginsGui.JoinInDB.JoinInDB": ToolCategory.IN_DATABASE,
    "AlteryxBasePluginsGui.SelectInDB.SelectInDB": ToolCategory.IN_DATABASE,
    "AlteryxBasePluginsGui.SummarizeInDB.SummarizeInDB": ToolCategory.IN_DATABASE,
    "AlteryxBasePluginsGui.UnionInDB.UnionInDB": ToolCategory.IN_DATABASE,
    "AlteryxBasePluginsGui.DataStreamIn.DataStreamIn": ToolCategory.IN_DATABASE,
    "AlteryxBasePluginsGui.DataStreamOut.DataStreamOut": ToolCategory.IN_DATABASE,

    # Reporting Tools
    "AlteryxBasePluginsGui.Render.Render": ToolCategory.REPORTING,
    "AlteryxBasePluginsGui.Email.Email": ToolCategory.REPORTING,
    "AlteryxBasePluginsGui.Layout.Layout": ToolCategory.REPORTING,
    "AlteryxBasePluginsGui.Table.Table": ToolCategory.REPORTING,
    "AlteryxBasePluginsGui.Image.Image": ToolCategory.REPORTING,
    "AlteryxBasePluginsGui.ReportText.ReportText": ToolCategory.REPORTING,
    "AlteryxBasePluginsGui.InteractiveChart.InteractiveChart": ToolCategory.REPORTING,

    # Developer Tools
    "AlteryxBasePluginsGui.RunCommand.RunCommand": ToolCategory.DEVELOPER,
    "AlteryxBasePluginsGui.AlteryxRun.AlteryxRun": ToolCategory.DEVELOPER,
    "AlteryxBasePluginsGui.ControlParameter.ControlParameter": ToolCategory.DEVELOPER,
    "AlteryxBasePluginsGui.Detour.Detour": ToolCategory.DEVELOPER,
    "AlteryxBasePluginsGui.DetourEnd.DetourEnd": ToolCategory.DEVELOPER,
    "AlteryxBasePluginsGui.Message.Message": ToolCategory.DEVELOPER,
    "AlteryxBasePluginsGui.Test.Test": ToolCategory.DEVELOPER,
    "AlteryxBasePluginsGui.Block.BlockUntilDone": ToolCategory.DEVELOPER,

    # Tool Containers (organization only - not data processing)
    "AlteryxGuiToolkit.ToolContainer.ToolContainer": ToolCategory.CONTAINER,
    "AlteryxBasePluginsGui.ControlContainer.ControlContainer": ToolCategory.CONTAINER,
}

# Simplified tool names from plugin paths
PLUGIN_NAME_MAP = {
    # Input
    "DbFileInput": "Input Data",
    "TextInput": "Text Input",
    "MacroInput": "Macro Input",
    "Directory": "Directory",
    "GenerateRows": "Generate Rows",
    "DateTime": "Date Time",

    # Output
    "DbFileOutput": "Output Data",
    "BrowseV2": "Browse",
    "MacroOutput": "Macro Output",

    # Preparation
    "Filter": "Filter",
    "Formula": "Formula",
    "MultiFieldFormula": "Multi-Field Formula",
    "AlteryxSelect": "Select",
    "Sort": "Sort",
    "Sample": "Sample",
    "Unique": "Unique",
    "DataCleansing": "Data Cleansing",
    "RecordID": "Record ID",
    "AutoField": "Auto Field",
    "MultiRowFormula": "Multi-Row Formula",
    "FindReplace": "Find Replace",
    "Imputation": "Imputation",
    "SelectRecords": "Select Records",
    "Tile": "Tile",

    # Join
    "Join": "Join",
    "Union": "Union",
    "AppendFields": "Append Fields",
    "JoinMultiple": "Join Multiple",
    "MakeGroup": "Make Group",
    "Fuzzy": "Fuzzy Match",

    # Transform
    "Summarize": "Summarize",
    "Transpose": "Transpose",
    "CrossTab": "Cross Tab",
    "CountRecords": "Count Records",
    "RunningTotal": "Running Total",
    "WeightedAverage": "Weighted Average",
    "Arrange": "Arrange",

    # Parse
    "RegEx": "RegEx",
    "TextToColumns": "Text To Columns",
    "XMLParse": "XML Parse",
    "JSONParse": "JSON Parse",
    "DateTimeParse": "DateTime Parse",
    "Dynamic": "Dynamic Input/Output",

    # In-Database
    "DbFileInputInDB": "Input In-DB",
    "DbFileOutputInDB": "Output In-DB",
    "FilterInDB": "Filter In-DB",
    "FormulaInDB": "Formula In-DB",
    "JoinInDB": "Join In-DB",
    "SelectInDB": "Select In-DB",
    "SummarizeInDB": "Summarize In-DB",
    "UnionInDB": "Union In-DB",
    "DataStreamIn": "Data Stream In",
    "DataStreamOut": "Data Stream Out",

    # Reporting
    "Render": "Render",
    "Email": "Email",
    "Layout": "Layout",
    "Table": "Table",
    "Image": "Image",
    "ReportText": "Report Text",
    "InteractiveChart": "Interactive Chart",

    # Developer
    "RunCommand": "Run Command",
    "AlteryxRun": "Run Alteryx Workflow",
    "ControlParameter": "Control Parameter",
    "Detour": "Detour",
    "DetourEnd": "Detour End",
    "Message": "Message",
    "Test": "Test",
    "BlockUntilDone": "Block Until Done",

    # Containers
    "ToolContainer": "Tool Container",
    "ControlContainer": "Control Container",
}

# Trino SQL/DBT equivalents for Alteryx tools
# Target: Starburst (Trino-based platform)
SQL_MAPPING = {
    "Input Data": {
        "sql": "SELECT * FROM {catalog}.{schema}.{table}",
        "dbt": "{{ source('schema', 'table') }}",
        "trino": "SELECT * FROM catalog.schema.table",
        "description": "Data source reference",
    },
    "Filter": {
        "sql": "WHERE {condition}",
        "dbt": "{{ filter_expression(relation, condition) }}",
        "macro": "filter_expression",
        "macro_file": "filter_helpers",
        "trino": "WHERE condition",
        "description": "Filter rows based on condition",
    },
    "Formula": {
        "sql": "SELECT *, {expression} AS {new_field} FROM ...",
        "dbt": "{{ add_calculated_column(relation, column_name, expression) }}",
        "macro": "add_calculated_column",
        "macro_file": "formula_helpers",
        "trino": "SELECT *, expression AS new_field",
        "description": "Calculate new fields or modify existing",
    },
    "Multi-Field Formula": {
        "sql": "SELECT *, {expression} AS {field1}, ... FROM ...",
        "dbt": "{{ add_multiple_columns(relation, columns) }}",
        "macro": "add_multiple_columns",
        "macro_file": "formula_helpers",
        "trino": "SELECT *, expr1 AS field1, expr2 AS field2",
        "description": "Apply formula across multiple fields",
    },
    "Select": {
        "sql": "SELECT {columns} FROM ...",
        "dbt": "{{ select_columns(relation, columns) }}",
        "macro": "select_columns",
        "macro_file": "select_transform",
        "trino": "SELECT col1, col2 AS alias",
        "description": "Select, rename, or reorder columns",
    },
    "Sort": {
        "sql": "ORDER BY {columns}",
        "dbt": "{{ sort_data(relation, order_by) }}",
        "macro": "sort_data",
        "macro_file": "select_transform",
        "trino": "ORDER BY column ASC|DESC NULLS FIRST|LAST",
        "description": "Sort data by columns",
    },
    "Join": {
        "sql": "{join_type} JOIN {right_table} ON {condition}",
        "dbt": "{{ left_join(left_relation, right_relation, join_columns) }}",
        "macro": "left_join",
        "macro_file": "join_union",
        "trino": "LEFT|RIGHT|INNER|FULL JOIN table ON condition",
        "description": "Join two datasets",
    },
    "Union": {
        "sql": "UNION ALL",
        "dbt": "{{ union_all(relations) }}",
        "macro": "union_all",
        "macro_file": "join_union",
        "trino": "SELECT ... UNION ALL SELECT ...",
        "description": "Stack datasets vertically",
    },
    "Append Fields": {
        "sql": "CROSS JOIN",
        "dbt": "{{ cross_join(left_relation, right_relation) }}",
        "macro": "cross_join",
        "macro_file": "join_union",
        "trino": "CROSS JOIN table",
        "description": "Cartesian join (append all fields)",
    },
    "Summarize": {
        "sql": "SELECT {group_by}, {aggregations} FROM ... GROUP BY {group_by}",
        "dbt": "{{ summarize(relation, group_by, agg_fields) }}",
        "macro": "summarize",
        "macro_file": "aggregation",
        "trino": "SELECT col, SUM(x), COUNT(*) FROM t GROUP BY col",
        "description": "Aggregate data with GROUP BY",
    },
    "Transpose": {
        "sql": "CROSS JOIN UNNEST(...)",
        "dbt": "{{ unpivot(relation, id_cols, value_cols) }}",
        "macro": "unpivot",
        "macro_file": "pivot",
        "trino": "SELECT id, key, value FROM t CROSS JOIN UNNEST(ARRAY['a','b'], ARRAY[col_a, col_b]) AS x(key, value)",
        "description": "Convert columns to rows (Trino UNNEST)",
    },
    "Cross Tab": {
        "sql": "Conditional aggregation with CASE WHEN",
        "dbt": "{{ pivot(relation, id_cols, pivot_col, value_col) }}",
        "macro": "pivot",
        "macro_file": "pivot",
        "trino": "SELECT id, SUM(CASE WHEN cat='A' THEN val END) AS a, SUM(CASE WHEN cat='B' THEN val END) AS b FROM t GROUP BY id",
        "description": "Convert rows to columns (pivot via conditional aggregation)",
    },
    "Unique": {
        "sql": "SELECT DISTINCT ... or ROW_NUMBER() OVER (...)",
        "dbt": "{{ deduplicate(relation, partition_by, order_by) }}",
        "macro": "deduplicate",
        "macro_file": "deduplicate",
        "trino": "SELECT DISTINCT col1, col2 FROM t -- or use ROW_NUMBER() OVER (PARTITION BY key ORDER BY col) = 1",
        "description": "Remove duplicates",
    },
    "Sample": {
        "sql": "LIMIT {n} or TABLESAMPLE",
        "dbt": "{{ sample_first_n(relation, n) }}",
        "macro": "sample_first_n",
        "macro_file": "sample_limit",
        "trino": "SELECT * FROM t TABLESAMPLE BERNOULLI(10) -- 10% sample, or use LIMIT n",
        "description": "Sample subset of data",
    },
    "Record ID": {
        "sql": "ROW_NUMBER() OVER (ORDER BY ...)",
        "dbt": "{{ generate_record_id(relation) }}",
        "macro": "generate_record_id",
        "macro_file": "generate_surrogate_key",
        "trino": "ROW_NUMBER() OVER (ORDER BY column) AS row_id",
        "description": "Add sequential row numbers",
    },
    "Multi-Row Formula": {
        "sql": "LAG() / LEAD() window functions",
        "dbt": "{{ lag_lead(relation, column_name, offset, partition_by, order_by) }}",
        "macro": "lag_lead",
        "macro_file": "window_rank",
        "trino": "LAG(value, 1) OVER (PARTITION BY group ORDER BY date) AS prev_value",
        "description": "Reference previous/next rows",
    },
    "Running Total": {
        "sql": "SUM() OVER (ORDER BY ... ROWS UNBOUNDED PRECEDING)",
        "dbt": "{{ running_total(relation, column_name, partition_by, order_by) }}",
        "macro": "running_total",
        "macro_file": "running_total",
        "trino": "SUM(amount) OVER (PARTITION BY customer ORDER BY date ROWS UNBOUNDED PRECEDING) AS running_total",
        "description": "Cumulative sum",
    },
    "RegEx": {
        "sql": "REGEXP_EXTRACT(), REGEXP_REPLACE()",
        "dbt": "{{ regex_extract(relation, column_name, pattern) }}",
        "macro": "regex_extract",
        "macro_file": "regex_functions",
        "trino": "REGEXP_EXTRACT(string, pattern, group), REGEXP_REPLACE(string, pattern, replacement)",
        "description": "Regular expression operations (Trino syntax)",
    },
    "Text To Columns": {
        "sql": "SPLIT() with UNNEST()",
        "dbt": "{{ split_to_columns(relation, column_name, delimiter) }}",
        "macro": "split_to_columns",
        "macro_file": "split_unnest",
        "trino": "SELECT id, part FROM t CROSS JOIN UNNEST(SPLIT(text_col, ',')) AS x(part)",
        "description": "Split text into multiple rows (Trino UNNEST)",
    },
    "Data Cleansing": {
        "sql": "TRIM(), COALESCE(), NULLIF()",
        "dbt": "{{ clean_string(relation, column_name) }}",
        "macro": "clean_string",
        "macro_file": "null_if_empty",
        "trino": "TRIM(col), COALESCE(col, 'default'), NULLIF(col, ''), LOWER(col), UPPER(col)",
        "description": "Clean and standardize data",
    },
    "Find Replace": {
        "sql": "REPLACE(), CASE WHEN",
        "dbt": "{{ find_replace_simple(relation, column_name, find_text, replace_text) }}",
        "macro": "find_replace_simple",
        "macro_file": "find_replace",
        "trino": "REPLACE(string, search, replacement), TRANSLATE(string, from, to)",
        "description": "Find and replace values",
    },
    "Count Records": {
        "sql": "SELECT COUNT(*)",
        "dbt": "{{ count_records(relation) }}",
        "macro": "count_records",
        "macro_file": "aggregation",
        "trino": "SELECT COUNT(*) AS record_count FROM table",
        "description": "Count number of records",
    },
    "Browse": {
        "sql": "-- Output/visualization",
        "dbt": "-- Final output (no SQL needed)",
        "trino": "-- Preview only, no Trino equivalent",
        "description": "View data output",
    },
    "Output Data": {
        "sql": "CREATE TABLE AS SELECT or INSERT INTO",
        "dbt": "{{ config(materialized='table') }}",
        "trino": "CREATE TABLE catalog.schema.table AS SELECT ... -- or INSERT INTO",
        "description": "Write to destination",
    },
}

# Medallion layer suggestions based on tool category
MEDALLION_LAYER_MAP = {
    ToolCategory.INPUT: MedallionLayer.BRONZE,
    ToolCategory.OUTPUT: MedallionLayer.GOLD,
    ToolCategory.PREPARATION: MedallionLayer.SILVER,
    ToolCategory.JOIN: MedallionLayer.SILVER,
    ToolCategory.TRANSFORM: MedallionLayer.SILVER,  # Could be GOLD for final aggregations
    ToolCategory.PARSE: MedallionLayer.BRONZE,
    ToolCategory.IN_DATABASE: MedallionLayer.SILVER,
    ToolCategory.REPORTING: MedallionLayer.GOLD,
    ToolCategory.MACRO: MedallionLayer.SILVER,
    ToolCategory.CONTAINER: None,  # Containers don't process data - skip them
    ToolCategory.UNKNOWN: MedallionLayer.SILVER,
}

# DBT model prefix based on medallion layer
DBT_PREFIX_MAP = {
    MedallionLayer.BRONZE: "stg_",   # staging
    MedallionLayer.SILVER: "int_",   # intermediate
    MedallionLayer.GOLD: "fct_",     # fact (or dim_ for dimensions)
}

# Aggregation function mappings
AGGREGATION_MAP = {
    "Sum": "SUM",
    "Count": "COUNT",
    "CountDistinct": "COUNT(DISTINCT",
    "Min": "MIN",
    "Max": "MAX",
    "Avg": "AVG",
    "First": "FIRST_VALUE",
    "Last": "LAST_VALUE",
    "Concat": "STRING_AGG",  # Or LISTAGG in some DBs
    "CountNonNull": "COUNT",
    "CountNull": "SUM(CASE WHEN {field} IS NULL THEN 1 ELSE 0 END)",
    "StdDev": "STDDEV",
    "Variance": "VARIANCE",
    "Median": "PERCENTILE_CONT(0.5)",
}


def get_category_from_plugin(plugin: str) -> ToolCategory:
    """Get the tool category from a full plugin path."""
    return PLUGIN_CATEGORY_MAP.get(plugin, ToolCategory.UNKNOWN)


def get_simple_name(plugin: str) -> str:
    """Extract a simple tool name from a full plugin path."""
    if not plugin:
        return "Unknown"

    # Extract the last part of the plugin path (e.g., "Filter" from "AlteryxBasePluginsGui.Filter.Filter")
    parts = plugin.split(".")
    if len(parts) >= 2:
        tool_name = parts[-1]
        return PLUGIN_NAME_MAP.get(tool_name, tool_name)

    return PLUGIN_NAME_MAP.get(plugin, plugin)


def get_sql_mapping(tool_name: str) -> dict:
    """Get SQL/DBT mapping for a tool."""
    return SQL_MAPPING.get(tool_name, {
        "sql": "-- Custom logic required",
        "dbt": "-- Custom logic required",
        "description": f"No direct mapping for {tool_name}",
    })


def get_medallion_layer(category: ToolCategory, is_final_output: bool = False) -> MedallionLayer:
    """Determine the appropriate medallion layer for a tool."""
    if is_final_output:
        return MedallionLayer.GOLD
    return MEDALLION_LAYER_MAP.get(category, MedallionLayer.SILVER)


def get_dbt_prefix(layer: MedallionLayer) -> str:
    """Get the DBT model name prefix for a medallion layer."""
    return DBT_PREFIX_MAP.get(layer, "int_")
