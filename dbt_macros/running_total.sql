{#
    Macro: running_total
    Description: Calculates running/cumulative totals using window functions.

    Alteryx Equivalent: Multi-Row Formula, Running Total tool
    Trino Compatible: Yes

    Arguments:
        relation: The source relation
        value_column: Column to sum cumulatively
        partition_by: List of columns to partition by (optional, for grouped running totals)
        order_by: List of columns to order by (determines cumulative order)
        alias: Name for the running total column (default: 'running_total')
        include_current: Whether to include current row in sum (default: true)

    Example Usage:
        {{ running_total(
            relation=ref('stg_sales'),
            value_column='amount',
            partition_by=['customer_id'],
            order_by=['transaction_date'],
            alias='cumulative_sales'
        ) }}
#}

{% macro running_total(relation, value_column, partition_by=none, order_by=none, alias='running_total', include_current=true) %}

select
    *,
    sum({{ value_column }}) over (
        {% if partition_by %}
        partition by {{ partition_by | join(', ') }}
        {% endif %}
        {% if order_by %}
        order by {{ order_by | join(', ') }}
        {% endif %}
        rows between unbounded preceding and {% if include_current %}current row{% else %}1 preceding{% endif %}
    ) as {{ alias }}
from {{ relation }}

{% endmacro %}


{#
    Macro: running_count
    Description: Calculates running count of rows.

    Alteryx Equivalent: RecordID tool, Running Total (Count mode)

    Arguments:
        relation: The source relation
        partition_by: List of columns to partition by (optional)
        order_by: List of columns to order by
        alias: Name for the running count column (default: 'row_num')
#}

{% macro running_count(relation, partition_by=none, order_by=none, alias='row_num') %}

select
    *,
    count(*) over (
        {% if partition_by %}
        partition by {{ partition_by | join(', ') }}
        {% endif %}
        {% if order_by %}
        order by {{ order_by | join(', ') }}
        {% endif %}
        rows between unbounded preceding and current row
    ) as {{ alias }}
from {{ relation }}

{% endmacro %}


{#
    Macro: running_average
    Description: Calculates running/moving average.

    Alteryx Equivalent: Multi-Row Formula with average calculation

    Arguments:
        relation: The source relation
        value_column: Column to average
        partition_by: Partition columns (optional)
        order_by: Order columns
        alias: Output column name
        window_size: Number of rows for moving average (null = all preceding rows)
#}

{% macro running_average(relation, value_column, partition_by=none, order_by=none, alias='running_avg', window_size=none) %}

select
    *,
    avg({{ value_column }}) over (
        {% if partition_by %}
        partition by {{ partition_by | join(', ') }}
        {% endif %}
        {% if order_by %}
        order by {{ order_by | join(', ') }}
        {% endif %}
        {% if window_size %}
        rows between {{ window_size - 1 }} preceding and current row
        {% else %}
        rows between unbounded preceding and current row
        {% endif %}
    ) as {{ alias }}
from {{ relation }}

{% endmacro %}
