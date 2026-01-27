{#
    Macro: date_spine
    Description: Generates a continuous date series between start and end dates.

    Alteryx Equivalent: Generate Rows tool for date sequences
    Trino Compatible: Yes (uses sequence() and unnest())

    Arguments:
        start_date: Start date (can be literal 'YYYY-MM-DD' or date expression)
        end_date: End date (can be literal 'YYYY-MM-DD' or date expression)
        date_column: Name for the output date column (default: 'date_day')

    Example Usage:
        {{ date_spine(
            start_date="date '2024-01-01'",
            end_date="date '2024-12-31'",
            date_column='calendar_date'
        ) }}

    Output: One row per day from start_date to end_date (inclusive)
#}

{% macro date_spine(start_date, end_date, date_column='date_day') %}

select
    cast({{ date_column }} as date) as {{ date_column }}
from unnest(
    sequence(
        {{ start_date }},
        {{ end_date }},
        interval '1' day
    )
) as t({{ date_column }})

{% endmacro %}


{#
    Macro: date_spine_with_attributes
    Description: Generates date spine with common date attributes (day of week, month, quarter, etc.).

    Alteryx Equivalent: Generate Rows + DateTime tool

    Arguments:
        start_date: Start date
        end_date: End date
        date_column: Name for the date column (default: 'date_day')

    Output columns: date_day, day_of_week, day_of_month, day_of_year, week_of_year,
                    month_num, month_name, quarter, year, is_weekend, is_month_start, is_month_end
#}

{% macro date_spine_with_attributes(start_date, end_date, date_column='date_day') %}

with base_dates as (
    {{ date_spine(start_date, end_date, date_column) }}
)

select
    {{ date_column }},
    day_of_week({{ date_column }}) as day_of_week,
    day_of_month({{ date_column }}) as day_of_month,
    day_of_year({{ date_column }}) as day_of_year,
    week_of_year({{ date_column }}) as week_of_year,
    month({{ date_column }}) as month_num,
    format_datetime({{ date_column }}, 'MMMM') as month_name,
    quarter({{ date_column }}) as quarter,
    year({{ date_column }}) as year,
    case when day_of_week({{ date_column }}) in (6, 7) then true else false end as is_weekend,
    case when day_of_month({{ date_column }}) = 1 then true else false end as is_month_start,
    case when {{ date_column }} = last_day_of_month({{ date_column }}) then true else false end as is_month_end,
    date_trunc('week', {{ date_column }}) as week_start,
    date_trunc('month', {{ date_column }}) as month_start,
    date_trunc('quarter', {{ date_column }}) as quarter_start,
    date_trunc('year', {{ date_column }}) as year_start
from base_dates

{% endmacro %}


{#
    Macro: timestamp_spine
    Description: Generates a continuous timestamp series at specified intervals.

    Arguments:
        start_timestamp: Start timestamp
        end_timestamp: End timestamp
        interval_unit: 'minute', 'hour', 'day' (default: 'hour')
        interval_value: Number of units between each timestamp (default: 1)
        timestamp_column: Output column name (default: 'timestamp_value')
#}

{% macro timestamp_spine(start_timestamp, end_timestamp, interval_unit='hour', interval_value=1, timestamp_column='timestamp_value') %}

select
    {{ timestamp_column }}
from unnest(
    sequence(
        {{ start_timestamp }},
        {{ end_timestamp }},
        interval '{{ interval_value }}' {{ interval_unit }}
    )
) as t({{ timestamp_column }})

{% endmacro %}


{#
    Macro: fill_date_gaps
    Description: Fills gaps in a date series by cross-joining with date spine.

    Alteryx Equivalent: Append Fields with date dimension + Filter/Join

    Arguments:
        relation: Source relation with dates
        date_column: Date column in source
        min_date: Minimum date (optional, derives from data if not specified)
        max_date: Maximum date (optional, derives from data if not specified)
        group_columns: Columns to carry forward for each date (for SCD-style fills)
        fill_method: 'null' (leave nulls), 'forward' (LOCF), 'zero' (fill numerics with 0)
#}

{% macro fill_date_gaps(relation, date_column, min_date=none, max_date=none, group_columns=none, fill_method='null') %}

{%- set min_dt = min_date if min_date else '(select min(' ~ date_column ~ ') from ' ~ relation ~ ')' -%}
{%- set max_dt = max_date if max_date else '(select max(' ~ date_column ~ ') from ' ~ relation ~ ')' -%}

with date_range as (
    {{ date_spine(min_dt, max_dt, date_column) }}
),

{% if group_columns %}
groups as (
    select distinct {{ group_columns | join(', ') }}
    from {{ relation }}
),

scaffolding as (
    select
        d.{{ date_column }},
        {{ group_columns | join(', ') }}
    from date_range d
    cross join groups
)
{% else %}
scaffolding as (
    select {{ date_column }}
    from date_range
)
{% endif %}

select
    s.*,
    {%- set other_columns = adapter.get_columns_in_relation(relation) %}
    {%- for col in other_columns %}
        {%- if col.name != date_column and (not group_columns or col.name not in group_columns) %}
    r.{{ col.name }}{% if not loop.last %},{% endif %}
        {%- endif %}
    {%- endfor %}
from scaffolding s
left join {{ relation }} r
    on s.{{ date_column }} = r.{{ date_column }}
    {% if group_columns %}
    {%- for gc in group_columns %}
    and s.{{ gc }} = r.{{ gc }}
    {%- endfor %}
    {% endif %}

{% endmacro %}


{#
    Macro: date_diff_days
    Description: Calculates the difference in days between two dates.

    Alteryx Equivalent: DateTimeDiff([date1], [date2], 'days')
#}

{% macro date_diff_days(date1, date2) %}
date_diff('day', {{ date2 }}, {{ date1 }})
{% endmacro %}


{#
    Macro: add_days
    Description: Adds days to a date.

    Alteryx Equivalent: DateTimeAdd([date], [days], 'days')
#}

{% macro add_days(date_column, days) %}
date_add('day', {{ days }}, {{ date_column }})
{% endmacro %}
