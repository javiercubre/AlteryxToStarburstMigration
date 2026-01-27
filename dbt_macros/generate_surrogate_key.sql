{#
    Macro: generate_surrogate_key
    Description: Generates a surrogate key (hash) from one or more columns.

    Alteryx Equivalent: Formula tool with MD5_ASCII() or SHA256() function
    Trino Compatible: Yes (uses xxhash64 or md5)

    Arguments:
        columns: List of column names to include in the hash
        hash_function: 'xxhash64' (faster) or 'md5' (more common) (default: 'md5')

    Example Usage:
        select
            {{ generate_surrogate_key(['customer_id', 'order_date']) }} as order_key,
            *
        from {{ ref('stg_orders') }}

    Note: This is similar to dbt_utils.generate_surrogate_key but uses Trino-native functions
#}

{% macro generate_surrogate_key(columns, hash_function='md5') %}

{% if hash_function == 'xxhash64' %}
cast(xxhash64(to_utf8(concat(
    {%- for column in columns %}
    coalesce(cast({{ column }} as varchar), '_null_')
    {%- if not loop.last %}, '|', {% endif %}
    {%- endfor %}
))) as varchar)
{% else %}
to_hex(md5(to_utf8(concat(
    {%- for column in columns %}
    coalesce(cast({{ column }} as varchar), '_null_')
    {%- if not loop.last %}, '|', {% endif %}
    {%- endfor %}
))))
{% endif %}

{% endmacro %}


{#
    Macro: generate_record_id
    Description: Adds a unique sequential record ID to each row.

    Alteryx Equivalent: RecordID tool

    Arguments:
        relation: The source relation
        id_alias: Name for the ID column (default: 'record_id')
        start_value: Starting value for the sequence (default: 1)
        order_by: Optional columns to determine ID order

    Example Usage:
        {{ generate_record_id(
            relation=ref('stg_customers'),
            id_alias='customer_sequence',
            order_by=['created_at']
        ) }}
#}

{% macro generate_record_id(relation, id_alias='record_id', start_value=1, order_by=none) %}

select
    row_number() over (
        {% if order_by %}
        order by {{ order_by | join(', ') }}
        {% else %}
        order by (select null)
        {% endif %}
    ) + {{ start_value - 1 }} as {{ id_alias }},
    *
from {{ relation }}

{% endmacro %}


{#
    Macro: generate_uuid
    Description: Generates a UUID for each row.

    Alteryx Equivalent: Formula tool with UUID generation
    Trino Compatible: Yes (uses uuid() function)

    Arguments:
        alias: Name for the UUID column (default: 'uuid')
#}

{% macro generate_uuid(alias='uuid') %}
cast(uuid() as varchar) as {{ alias }}
{% endmacro %}


{#
    Macro: add_metadata_columns
    Description: Adds common metadata columns (load timestamp, source file, hash key).

    Alteryx Equivalent: Adding audit/metadata columns via Formula tool

    Arguments:
        relation: The source relation
        include_load_ts: Add load timestamp column (default: true)
        include_source_file: Add source file column if available (default: false)
        include_row_hash: Add hash of all columns (default: false)
        key_columns: Columns for surrogate key (optional, used with include_row_hash)
#}

{% macro add_metadata_columns(relation, include_load_ts=true, include_source_file=false, include_row_hash=false, key_columns=none) %}

select
    *
    {%- if include_load_ts %},
    current_timestamp as _loaded_at
    {%- endif %}
    {%- if include_source_file %},
    '{{ this.identifier }}' as _source_model
    {%- endif %}
    {%- if include_row_hash and key_columns %},
    {{ generate_surrogate_key(key_columns) }} as _row_hash
    {%- endif %}
from {{ relation }}

{% endmacro %}
