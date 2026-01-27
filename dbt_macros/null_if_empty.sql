{#
    Macro: null_if_empty
    Description: Converts empty strings and whitespace-only strings to NULL.

    Alteryx Equivalent: Formula tool with IsEmpty() check, Data Cleansing tool
    Trino Compatible: Yes

    Arguments:
        column: The column to clean
        trim_first: Whether to trim whitespace before checking (default: true)

    Example Usage:
        select
            {{ null_if_empty('customer_name') }} as customer_name,
            {{ null_if_empty('email', trim_first=false) }} as email
        from {{ ref('raw_customers') }}
#}

{% macro null_if_empty(column, trim_first=true) %}
{% if trim_first %}
nullif(trim({{ column }}), '')
{% else %}
nullif({{ column }}, '')
{% endif %}
{% endmacro %}


{#
    Macro: coalesce_empty
    Description: Returns fallback value for NULL or empty strings.

    Alteryx Equivalent: IIF(IsNull([field]) OR IsEmpty([field]), default, [field])

    Arguments:
        column: The column to check
        fallback: Value to use if null or empty
        trim_first: Whether to trim before checking (default: true)
#}

{% macro coalesce_empty(column, fallback, trim_first=true) %}
coalesce({{ null_if_empty(column, trim_first) }}, {{ fallback }})
{% endmacro %}


{#
    Macro: clean_string
    Description: Comprehensive string cleaning - trims, converts empty to null, optionally normalizes case.

    Alteryx Equivalent: Data Cleansing tool + Formula tool

    Arguments:
        column: The column to clean
        null_empty: Convert empty to null (default: true)
        case_style: 'upper', 'lower', 'proper', or none (default: none)
        remove_leading_zeros: Remove leading zeros (default: false)
        max_length: Truncate to max length (optional)
#}

{% macro clean_string(column, null_empty=true, case_style=none, remove_leading_zeros=false, max_length=none) %}
{%- set result = 'trim(' ~ column ~ ')' -%}

{%- if case_style == 'upper' -%}
    {%- set result = 'upper(' ~ result ~ ')' -%}
{%- elif case_style == 'lower' -%}
    {%- set result = 'lower(' ~ result ~ ')' -%}
{%- elif case_style == 'proper' -%}
    {# Trino doesn't have PROPER/INITCAP by default, use regex #}
    {%- set result = "regexp_replace(" ~ result ~ ", '(^|\\s)(\\w)', x -> upper(x))" -%}
{%- endif -%}

{%- if remove_leading_zeros -%}
    {%- set result = "regexp_replace(" ~ result ~ ", '^0+', '')" -%}
{%- endif -%}

{%- if max_length -%}
    {%- set result = 'substr(' ~ result ~ ', 1, ' ~ max_length ~ ')' -%}
{%- endif -%}

{%- if null_empty -%}
nullif({{ result }}, '')
{%- else -%}
{{ result }}
{%- endif -%}
{% endmacro %}


{#
    Macro: is_null_or_empty
    Description: Returns true if value is null, empty string, or whitespace-only.

    Alteryx Equivalent: IsNull([field]) OR IsEmpty([field])

    Arguments:
        column: The column to check
#}

{% macro is_null_or_empty(column) %}
({{ column }} is null or trim({{ column }}) = '')
{% endmacro %}


{#
    Macro: clean_columns
    Description: Applies string cleaning to multiple columns at once.

    Arguments:
        relation: The source relation
        columns_to_clean: List of column names to clean (if null, cleans all varchar columns)
        null_empty: Convert empty to null (default: true)
        case_style: Case normalization (optional)
#}

{% macro clean_columns(relation, columns_to_clean=none, null_empty=true, case_style=none) %}

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for column in all_columns %}
        {%- if columns_to_clean is none or column.name in columns_to_clean %}
            {%- if column.is_string() %}
    {{ clean_string(column.name, null_empty=null_empty, case_style=case_style) }} as {{ column.name }}
            {%- else %}
    {{ column.name }}
            {%- endif %}
        {%- else %}
    {{ column.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }}

{% endmacro %}


{#
    Macro: standardize_nulls
    Description: Converts various null representations to actual NULL.

    Alteryx Equivalent: Data Cleansing tool (replace specific values)

    Arguments:
        column: The column to standardize
        null_values: List of values to treat as null (default: common patterns)
#}

{% macro standardize_nulls(column, null_values=none) %}
{%- set default_null_values = ['', 'NULL', 'null', 'N/A', 'n/a', 'NA', 'na', '#N/A', 'None', 'none', '-', '.'] -%}
{%- set values_to_check = null_values if null_values else default_null_values -%}

case
    when trim({{ column }}) in (
        {%- for val in values_to_check -%}
        '{{ val }}'{% if not loop.last %}, {% endif %}
        {%- endfor -%}
    ) then null
    else {{ column }}
end
{% endmacro %}
