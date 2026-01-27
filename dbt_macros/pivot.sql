{#
    Macro: pivot
    Description: Pivots rows into columns using conditional aggregation.

    Alteryx Equivalent: CrossTab tool
    Trino Compatible: Yes

    Arguments:
        relation: The source relation
        row_key: Column(s) that define the row identity (can be list)
        pivot_column: Column whose values become new column headers
        value_column: Column containing values to aggregate
        agg_function: Aggregation function ('sum', 'count', 'max', 'min', 'avg') (default: 'sum')
        pivot_values: List of values to pivot (if known ahead of time)
        then_value: Value to use in CASE WHEN (default: value_column)
        else_value: Value when condition not met (default: null)
        prefix: Prefix for pivoted column names (default: '')
        suffix: Suffix for pivoted column names (default: '')
        quote_alias: Whether to quote column aliases (default: false)

    Example Usage:
        {{ pivot(
            relation=ref('stg_sales'),
            row_key='product_id',
            pivot_column='region',
            value_column='revenue',
            agg_function='sum',
            pivot_values=['North', 'South', 'East', 'West'],
            prefix='revenue_'
        ) }}

    Input:
        product_id | region | revenue
        1          | North  | 100
        1          | South  | 150

    Output:
        product_id | revenue_North | revenue_South | revenue_East | revenue_West
        1          | 100           | 150           | null         | null
#}

{% macro pivot(relation, row_key, pivot_column, value_column, agg_function='sum', pivot_values=none, then_value=none, else_value='null', prefix='', suffix='', quote_alias=false) %}

{%- set row_keys = row_key if row_key is iterable and row_key is not string else [row_key] -%}
{%- set then_val = then_value if then_value else value_column -%}

select
    {{ row_keys | join(', ') }}
    {%- for pv in pivot_values %},
    {{ agg_function }}(case when {{ pivot_column }} = '{{ pv }}' then {{ then_val }} else {{ else_value }} end) as {% if quote_alias %}"{% endif %}{{ prefix }}{{ pv | replace(' ', '_') | replace('-', '_') }}{{ suffix }}{% if quote_alias %}"{% endif %}
    {%- endfor %}
from {{ relation }}
group by {{ row_keys | join(', ') }}

{% endmacro %}


{#
    Macro: unpivot
    Description: Unpivots columns into rows (reverse of pivot/crosstab).

    Alteryx Equivalent: Transpose tool
    Trino Compatible: Yes (uses CROSS JOIN UNNEST with ARRAY of ROW)

    Arguments:
        relation: The source relation
        key_columns: Columns to keep as identifiers (not unpivoted)
        unpivot_columns: Columns to unpivot into rows
        name_column: Name for the column containing original column names (default: 'attribute')
        value_column: Name for the column containing values (default: 'value')

    Example Usage:
        {{ unpivot(
            relation=ref('stg_metrics'),
            key_columns=['customer_id', 'date'],
            unpivot_columns=['metric_a', 'metric_b', 'metric_c'],
            name_column='metric_name',
            value_column='metric_value'
        ) }}

    Input:
        customer_id | date       | metric_a | metric_b | metric_c
        1           | 2024-01-01 | 10       | 20       | 30

    Output:
        customer_id | date       | metric_name | metric_value
        1           | 2024-01-01 | metric_a    | 10
        1           | 2024-01-01 | metric_b    | 20
        1           | 2024-01-01 | metric_c    | 30
#}

{% macro unpivot(relation, key_columns, unpivot_columns, name_column='attribute', value_column='value') %}

select
    {{ key_columns | join(', ') }},
    {{ name_column }},
    {{ value_column }}
from {{ relation }}
cross join unnest(
    array[
        {%- for col in unpivot_columns %}
        row('{{ col }}', cast({{ col }} as varchar)){% if not loop.last %},{% endif %}
        {%- endfor %}
    ]
) as t({{ name_column }}, {{ value_column }})

{% endmacro %}


{#
    Macro: dynamic_pivot
    Description: Generates pivot SQL dynamically based on distinct values.
    Note: This generates the SQL string - use with run_query for dynamic execution.

    Arguments:
        source_table: Source table name
        row_key: Row identifier column(s)
        pivot_column: Column to pivot on
        value_column: Value column
        agg_function: Aggregation function
#}

{% macro dynamic_pivot(source_table, row_key, pivot_column, value_column, agg_function='sum') %}

{%- set pivot_query %}
    select distinct {{ pivot_column }} from {{ source_table }} order by 1
{%- endset -%}

{%- set results = run_query(pivot_query) -%}
{%- set pivot_values = results.columns[0].values() -%}

{{ pivot(
    relation=source_table,
    row_key=row_key,
    pivot_column=pivot_column,
    value_column=value_column,
    agg_function=agg_function,
    pivot_values=pivot_values
) }}

{% endmacro %}
