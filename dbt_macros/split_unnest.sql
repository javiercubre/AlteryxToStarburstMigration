{#
    Macro: split_unnest
    Description: Splits a delimited column and unnests into multiple rows.

    Alteryx Equivalent: Text to Columns tool, Transpose tool
    Trino Compatible: Yes (uses CROSS JOIN UNNEST with SPLIT)

    Arguments:
        relation: The source relation
        column: The column containing delimited values
        delimiter: The delimiter string (default: ',')
        output_alias: Name for the unnested value column (default: 'value')
        trim_values: Whether to trim whitespace from split values (default: true)
        filter_empty: Whether to filter out empty values (default: true)

    Example Usage:
        {{ split_unnest(
            relation=ref('stg_products'),
            column='tags',
            delimiter=',',
            output_alias='tag'
        ) }}

    Input:  id=1, tags='red,blue,green'
    Output: id=1, tag='red'
            id=1, tag='blue'
            id=1, tag='green'
#}

{% macro split_unnest(relation, column, delimiter=',', output_alias='value', trim_values=true, filter_empty=true) %}

select
    {%- set columns = adapter.get_columns_in_relation(relation) %}
    {%- for col in columns %}
        {%- if col.name != column %}
    t.{{ col.name }},
        {%- endif %}
    {%- endfor %}
    {% if trim_values %}
    trim({{ output_alias }}) as {{ output_alias }}
    {% else %}
    {{ output_alias }}
    {% endif %}
from {{ relation }} t
cross join unnest(split({{ column }}, '{{ delimiter }}')) as x({{ output_alias }})
{% if filter_empty %}
where {% if trim_values %}trim({{ output_alias }}){% else %}{{ output_alias }}{% endif %} <> ''
{% endif %}

{% endmacro %}


{#
    Macro: split_to_columns
    Description: Splits a delimited column into multiple fixed columns.

    Alteryx Equivalent: Text to Columns tool (split to columns mode)

    Arguments:
        relation: The source relation
        column: The column containing delimited values
        delimiter: The delimiter string
        num_columns: Number of output columns to create
        column_prefix: Prefix for output column names (default: 'part')

    Example Usage:
        {{ split_to_columns(
            relation=ref('stg_addresses'),
            column='full_name',
            delimiter=' ',
            num_columns=3,
            column_prefix='name'
        ) }}

    Input:  full_name='John Adam Smith'
    Output: name_1='John', name_2='Adam', name_3='Smith'
#}

{% macro split_to_columns(relation, column, delimiter, num_columns, column_prefix='part') %}

with split_array as (
    select
        *,
        split({{ column }}, '{{ delimiter }}') as _parts
    from {{ relation }}
)

select
    {%- set columns = adapter.get_columns_in_relation(relation) %}
    {%- for col in columns %}
        {%- if col.name != column %}
    {{ col.name }},
        {%- endif %}
    {%- endfor %}
    {%- for i in range(1, num_columns + 1) %}
    element_at(_parts, {{ i }}) as {{ column_prefix }}_{{ i }}{% if not loop.last %},{% endif %}
    {%- endfor %}
from split_array

{% endmacro %}


{#
    Macro: array_agg_to_string
    Description: Aggregates multiple rows into a delimited string (reverse of split_unnest).

    Alteryx Equivalent: Summarize tool with Concatenate action

    Arguments:
        relation: The source relation
        group_by: List of columns to group by
        value_column: Column to aggregate
        delimiter: Delimiter for concatenation (default: ',')
        output_alias: Name for the aggregated column
        distinct_values: Whether to only include distinct values (default: false)
        order_by: Column to order values before concatenation (optional)
#}

{% macro array_agg_to_string(relation, group_by, value_column, delimiter=',', output_alias='concatenated', distinct_values=false, order_by=none) %}

select
    {{ group_by | join(', ') }},
    array_join(
        array_agg({% if distinct_values %}distinct {% endif %}{{ value_column }}{% if order_by %} order by {{ order_by }}{% endif %}),
        '{{ delimiter }}'
    ) as {{ output_alias }}
from {{ relation }}
group by {{ group_by | join(', ') }}

{% endmacro %}
