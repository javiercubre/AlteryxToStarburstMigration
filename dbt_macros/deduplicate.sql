{#
    Macro: deduplicate
    Description: Removes duplicate rows based on partition columns, keeping one row per partition.

    Alteryx Equivalent: Unique tool, or RecordID + Filter combination
    Trino Compatible: Yes

    Arguments:
        relation: The source relation (table/CTE)
        partition_by: List of columns to partition by (defines uniqueness)
        order_by: List of columns to order by within each partition (determines which row to keep)
        order_direction: 'asc' or 'desc' (default: 'desc' to keep most recent)

    Example Usage:
        {{ deduplicate(
            relation=ref('stg_customers'),
            partition_by=['customer_id'],
            order_by=['updated_at'],
            order_direction='desc'
        ) }}

    Output: Single row per partition_by combination, ordered by order_by columns
#}

{% macro deduplicate(relation, partition_by, order_by, order_direction='desc') %}

with dedupe_ranked as (
    select
        *,
        row_number() over (
            partition by {{ partition_by | join(', ') }}
            order by {{ order_by | join(' ' ~ order_direction ~ ', ') }} {{ order_direction }}
        ) as _dedupe_row_num
    from {{ relation }}
)

select
    {%- set columns = adapter.get_columns_in_relation(relation) %}
    {%- for column in columns %}
    {{ column.name }}{% if not loop.last %},{% endif %}
    {%- endfor %}
from dedupe_ranked
where _dedupe_row_num = 1

{% endmacro %}


{#
    Macro: deduplicate_simple
    Description: Simple deduplication using DISTINCT - faster but less control

    Alteryx Equivalent: Unique tool (basic mode)

    Arguments:
        relation: The source relation
        columns: List of columns to include (uses all if not specified)

    Example Usage:
        {{ deduplicate_simple(ref('stg_orders'), ['customer_id', 'product_id']) }}
#}

{% macro deduplicate_simple(relation, columns=none) %}

{% if columns %}
select distinct
    {{ columns | join(', ') }}
from {{ relation }}
{% else %}
select distinct *
from {{ relation }}
{% endif %}

{% endmacro %}
