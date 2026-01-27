{#
    Macro: window_rank
    Description: Adds ranking columns using various ranking functions.

    Alteryx Equivalent: RecordID tool, Multi-Row Formula with ranking
    Trino Compatible: Yes

    Arguments:
        relation: The source relation
        partition_by: List of columns to partition by (optional)
        order_by: List of columns to order by
        rank_type: 'row_number', 'rank', 'dense_rank', 'percent_rank', 'ntile'
        alias: Name for the rank column (default: 'rank')
        order_direction: 'asc' or 'desc' (default: 'asc')
        ntile_buckets: Number of buckets for ntile (only used when rank_type='ntile')

    Example Usage:
        {{ window_rank(
            relation=ref('stg_sales'),
            partition_by=['region'],
            order_by=['revenue'],
            rank_type='row_number',
            alias='sales_rank',
            order_direction='desc'
        ) }}
#}

{% macro window_rank(relation, order_by, partition_by=none, rank_type='row_number', alias='rank', order_direction='asc', ntile_buckets=4) %}

select
    *,
    {% if rank_type == 'ntile' %}
    ntile({{ ntile_buckets }})
    {% else %}
    {{ rank_type }}()
    {% endif %}
    over (
        {% if partition_by %}
        partition by {{ partition_by | join(', ') }}
        {% endif %}
        order by {{ order_by | join(' ' ~ order_direction ~ ', ') }} {{ order_direction }}
    ) as {{ alias }}
from {{ relation }}

{% endmacro %}


{#
    Macro: top_n_per_group
    Description: Returns top N rows per group based on ordering.

    Alteryx Equivalent: Sample tool (First N per group), or Sort + RecordID + Filter

    Arguments:
        relation: The source relation
        partition_by: Columns defining groups
        order_by: Columns to order by within groups
        n: Number of rows to keep per group (default: 1)
        order_direction: 'asc' or 'desc'

    Example Usage:
        {{ top_n_per_group(
            relation=ref('stg_orders'),
            partition_by=['customer_id'],
            order_by=['order_date'],
            n=3,
            order_direction='desc'
        ) }}
#}

{% macro top_n_per_group(relation, partition_by, order_by, n=1, order_direction='desc') %}

with ranked as (
    select
        *,
        row_number() over (
            partition by {{ partition_by | join(', ') }}
            order by {{ order_by | join(' ' ~ order_direction ~ ', ') }} {{ order_direction }}
        ) as _row_rank
    from {{ relation }}
)

select
    {%- set columns = adapter.get_columns_in_relation(relation) %}
    {%- for column in columns %}
    {{ column.name }}{% if not loop.last %},{% endif %}
    {%- endfor %}
from ranked
where _row_rank <= {{ n }}

{% endmacro %}


{#
    Macro: lag_lead
    Description: Adds previous/next row values using LAG/LEAD window functions.

    Alteryx Equivalent: Multi-Row Formula

    Arguments:
        relation: The source relation
        value_column: Column to get lag/lead values from
        partition_by: Partition columns (optional)
        order_by: Order columns
        lag_offset: Number of rows to look back (default: 1)
        lead_offset: Number of rows to look forward (default: 1)
        lag_alias: Name for lag column
        lead_alias: Name for lead column
        default_value: Value when lag/lead is null (optional)
#}

{% macro lag_lead(relation, value_column, order_by, partition_by=none, lag_offset=1, lead_offset=1, lag_alias='prev_value', lead_alias='next_value', default_value=none) %}

select
    *,
    lag({{ value_column }}, {{ lag_offset }}{% if default_value is not none %}, {{ default_value }}{% endif %}) over (
        {% if partition_by %}
        partition by {{ partition_by | join(', ') }}
        {% endif %}
        order by {{ order_by | join(', ') }}
    ) as {{ lag_alias }},
    lead({{ value_column }}, {{ lead_offset }}{% if default_value is not none %}, {{ default_value }}{% endif %}) over (
        {% if partition_by %}
        partition by {{ partition_by | join(', ') }}
        {% endif %}
        order by {{ order_by | join(', ') }}
    ) as {{ lead_alias }}
from {{ relation }}

{% endmacro %}
