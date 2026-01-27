{#
    Macro: safe_cast
    Description: Safely casts a column to a target type with fallback value on failure.

    Alteryx Equivalent: Formula tool with error handling, ToNumber(), ToString(), etc.
    Trino Compatible: Yes (uses TRY_CAST)

    Arguments:
        column: The column or expression to cast
        target_type: The target data type (varchar, integer, double, date, timestamp, etc.)
        fallback: Value to use if cast fails (default: null)

    Example Usage:
        {{ safe_cast('order_amount', 'double', 0.0) }}
        {{ safe_cast('birth_date', 'date') }}
#}

{% macro safe_cast(column, target_type, fallback=none) %}
{% if fallback is not none %}
coalesce(try_cast({{ column }} as {{ target_type }}), {{ fallback }})
{% else %}
try_cast({{ column }} as {{ target_type }})
{% endif %}
{% endmacro %}


{#
    Macro: safe_cast_columns
    Description: Applies safe casting to multiple columns at once.

    Arguments:
        relation: The source relation
        column_types: Dictionary of column_name: target_type pairs
        fallback_map: Optional dictionary of column_name: fallback_value pairs

    Example Usage:
        {{ safe_cast_columns(
            relation=ref('raw_data'),
            column_types={'amount': 'double', 'quantity': 'integer', 'order_date': 'date'},
            fallback_map={'amount': 0.0, 'quantity': 0}
        ) }}
#}

{% macro safe_cast_columns(relation, column_types, fallback_map=none) %}

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for column in all_columns %}
        {%- if column.name in column_types %}
            {%- set fallback = fallback_map[column.name] if fallback_map and column.name in fallback_map else none %}
    {{ safe_cast(column.name, column_types[column.name], fallback) }} as {{ column.name }}
        {%- else %}
    {{ column.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }}

{% endmacro %}


{#
    Macro: to_varchar
    Description: Safely converts any column to varchar/string.

    Alteryx Equivalent: ToString() function
#}

{% macro to_varchar(column, fallback="''") %}
coalesce(cast({{ column }} as varchar), {{ fallback }})
{% endmacro %}


{#
    Macro: to_integer
    Description: Safely converts column to integer with fallback.

    Alteryx Equivalent: ToNumber() function with integer result
#}

{% macro to_integer(column, fallback=0) %}
coalesce(try_cast({{ column }} as integer), {{ fallback }})
{% endmacro %}


{#
    Macro: to_double
    Description: Safely converts column to double/decimal with fallback.

    Alteryx Equivalent: ToNumber() function
#}

{% macro to_double(column, fallback=0.0) %}
coalesce(try_cast({{ column }} as double), {{ fallback }})
{% endmacro %}


{#
    Macro: to_date
    Description: Safely parses string to date with optional format.

    Alteryx Equivalent: DateTimeParse() function
    Trino: Uses date_parse for formatted strings

    Arguments:
        column: The column to convert
        format: Date format string (Trino format, e.g., '%Y-%m-%d')
        fallback: Fallback date value (optional)
#}

{% macro to_date(column, format=none, fallback=none) %}
{% if format %}
    {% if fallback is not none %}
coalesce(try(date(date_parse({{ column }}, '{{ format }}'))), {{ fallback }})
    {% else %}
try(date(date_parse({{ column }}, '{{ format }}')))
    {% endif %}
{% else %}
    {% if fallback is not none %}
coalesce(try_cast({{ column }} as date), {{ fallback }})
    {% else %}
try_cast({{ column }} as date)
    {% endif %}
{% endif %}
{% endmacro %}


{#
    Macro: to_timestamp
    Description: Safely parses string to timestamp with optional format.

    Alteryx Equivalent: DateTimeParse() function

    Arguments:
        column: The column to convert
        format: Timestamp format string (Trino format)
        fallback: Fallback timestamp value (optional)
#}

{% macro to_timestamp(column, format=none, fallback=none) %}
{% if format %}
    {% if fallback is not none %}
coalesce(try(date_parse({{ column }}, '{{ format }}')), {{ fallback }})
    {% else %}
try(date_parse({{ column }}, '{{ format }}'))
    {% endif %}
{% else %}
    {% if fallback is not none %}
coalesce(try_cast({{ column }} as timestamp), {{ fallback }})
    {% else %}
try_cast({{ column }} as timestamp)
    {% endif %}
{% endif %}
{% endmacro %}
