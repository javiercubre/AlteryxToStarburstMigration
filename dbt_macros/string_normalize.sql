{#
    Macro: string_normalize
    Description: Normalizes strings for consistent comparison and matching.

    Alteryx Equivalent: Data Cleansing tool + Formula tool
    Trino Compatible: Yes

    Arguments:
        column: The column to normalize
        options: Dict with normalization options:
            - trim: Remove leading/trailing whitespace (default: true)
            - case: 'upper', 'lower', or none (default: 'lower')
            - remove_accents: Replace accented chars (default: false)
            - remove_punctuation: Remove punctuation (default: false)
            - remove_extra_spaces: Collapse multiple spaces to one (default: true)
            - alphanumeric_only: Keep only letters and numbers (default: false)

    Example Usage:
        select
            {{ string_normalize('company_name', {'case': 'upper', 'remove_punctuation': true}) }} as normalized_name
        from {{ ref('stg_companies') }}
#}

{% macro string_normalize(column, options={}) %}
{%- set trim = options.get('trim', true) -%}
{%- set case_style = options.get('case', 'lower') -%}
{%- set remove_accents = options.get('remove_accents', false) -%}
{%- set remove_punctuation = options.get('remove_punctuation', false) -%}
{%- set remove_extra_spaces = options.get('remove_extra_spaces', true) -%}
{%- set alphanumeric_only = options.get('alphanumeric_only', false) -%}

{%- set result = column -%}

{# Step 1: Trim whitespace #}
{%- if trim -%}
    {%- set result = 'trim(' ~ result ~ ')' -%}
{%- endif -%}

{# Step 2: Case normalization #}
{%- if case_style == 'upper' -%}
    {%- set result = 'upper(' ~ result ~ ')' -%}
{%- elif case_style == 'lower' -%}
    {%- set result = 'lower(' ~ result ~ ')' -%}
{%- endif -%}

{# Step 3: Remove accents (common replacements) #}
{%- if remove_accents -%}
    {%- set result = "translate(" ~ result ~ ", 'àáâãäåèéêëìíîïòóôõöùúûüýÿñçÀÁÂÃÄÅÈÉÊËÌÍÎÏÒÓÔÕÖÙÚÛÜÝÑÇ', 'aaaaaaeeeeiiiiooooouuuuyyncAAAAAAEEEEIIIIOOOOOUUUUYNC')" -%}
{%- endif -%}

{# Step 4: Remove punctuation #}
{%- if remove_punctuation -%}
    {%- set result = "regexp_replace(" ~ result ~ ", '[^a-zA-Z0-9\\s]', '')" -%}
{%- endif -%}

{# Step 5: Alphanumeric only (more aggressive than remove_punctuation) #}
{%- if alphanumeric_only -%}
    {%- set result = "regexp_replace(" ~ result ~ ", '[^a-zA-Z0-9]', '')" -%}
{%- endif -%}

{# Step 6: Remove extra spaces (collapse multiple to single) #}
{%- if remove_extra_spaces and not alphanumeric_only -%}
    {%- set result = "regexp_replace(" ~ result ~ ", '\\s+', ' ')" -%}
{%- endif -%}

{{ result }}
{% endmacro %}


{#
    Macro: extract_numbers
    Description: Extracts all numeric characters from a string.

    Alteryx Equivalent: REGEX_Replace([field], '[^0-9]', '')

    Arguments:
        column: The column to extract numbers from
#}

{% macro extract_numbers(column) %}
regexp_replace({{ column }}, '[^0-9]', '')
{% endmacro %}


{#
    Macro: extract_letters
    Description: Extracts all letter characters from a string.

    Alteryx Equivalent: REGEX_Replace([field], '[^a-zA-Z]', '')

    Arguments:
        column: The column to extract letters from
#}

{% macro extract_letters(column) %}
regexp_replace({{ column }}, '[^a-zA-Z]', '')
{% endmacro %}


{#
    Macro: phone_normalize
    Description: Normalizes phone numbers to digits only.

    Alteryx Equivalent: REGEX_Replace for phone cleaning

    Arguments:
        column: Phone number column
        include_country_code: Keep country code (default: true)
        default_country_code: Country code to prepend if missing (optional)
#}

{% macro phone_normalize(column, include_country_code=true, default_country_code=none) %}
{%- set cleaned = "regexp_replace(" ~ column ~ ", '[^0-9]', '')" -%}
{% if not include_country_code %}
{# Remove leading 1 for US numbers if they're 11 digits #}
case
    when length({{ cleaned }}) = 11 and substr({{ cleaned }}, 1, 1) = '1'
    then substr({{ cleaned }}, 2)
    else {{ cleaned }}
end
{% elif default_country_code %}
case
    when length({{ cleaned }}) = 10
    then '{{ default_country_code }}' || {{ cleaned }}
    else {{ cleaned }}
end
{% else %}
{{ cleaned }}
{% endif %}
{% endmacro %}


{#
    Macro: email_normalize
    Description: Normalizes email addresses (lowercase, trim).

    Alteryx Equivalent: Formula with LowerCase(Trim([email]))

    Arguments:
        column: Email column
        extract_domain: If true, returns only the domain (default: false)
        extract_local: If true, returns only the local part (default: false)
#}

{% macro email_normalize(column, extract_domain=false, extract_local=false) %}
{% if extract_domain %}
lower(trim(regexp_extract({{ column }}, '@(.+)$', 1)))
{% elif extract_local %}
lower(trim(regexp_extract({{ column }}, '^([^@]+)@', 1)))
{% else %}
lower(trim({{ column }}))
{% endif %}
{% endmacro %}


{#
    Macro: proper_case
    Description: Converts string to proper/title case (capitalize first letter of each word).

    Alteryx Equivalent: TitleCase() function
    Trino: Uses regexp_replace with callback

    Arguments:
        column: The column to convert
#}

{% macro proper_case(column) %}
regexp_replace(
    lower({{ column }}),
    '(^|[^a-zA-Z])([a-z])',
    x -> concat(element_at(regexp_extract_all(x, '(^|[^a-zA-Z])'), 1), upper(element_at(regexp_extract_all(x, '([a-z])'), 1)))
)
{% endmacro %}


{#
    Macro: pad_left
    Description: Left-pads a string to a specified length.

    Alteryx Equivalent: PadLeft([field], length, 'char')

    Arguments:
        column: The column to pad
        length: Target length
        pad_char: Character to pad with (default: '0')
#}

{% macro pad_left(column, length, pad_char='0') %}
lpad(cast({{ column }} as varchar), {{ length }}, '{{ pad_char }}')
{% endmacro %}


{#
    Macro: pad_right
    Description: Right-pads a string to a specified length.

    Alteryx Equivalent: PadRight([field], length, 'char')

    Arguments:
        column: The column to pad
        length: Target length
        pad_char: Character to pad with (default: ' ')
#}

{% macro pad_right(column, length, pad_char=' ') %}
rpad(cast({{ column }} as varchar), {{ length }}, '{{ pad_char }}')
{% endmacro %}


{#
    Macro: contains
    Description: Returns true if string contains substring.

    Alteryx Equivalent: Contains([field], 'substring')

    Arguments:
        column: The column to search in
        substring: The substring to search for
        case_sensitive: Whether search is case-sensitive (default: true)
#}

{% macro contains(column, substring, case_sensitive=true) %}
{% if case_sensitive %}
strpos({{ column }}, '{{ substring }}') > 0
{% else %}
strpos(lower({{ column }}), lower('{{ substring }}')) > 0
{% endif %}
{% endmacro %}


{#
    Macro: starts_with
    Description: Returns true if string starts with prefix.

    Alteryx Equivalent: StartsWith([field], 'prefix')
#}

{% macro starts_with(column, prefix, case_sensitive=true) %}
{% if case_sensitive %}
substr({{ column }}, 1, {{ prefix | length }}) = '{{ prefix }}'
{% else %}
lower(substr({{ column }}, 1, {{ prefix | length }})) = lower('{{ prefix }}')
{% endif %}
{% endmacro %}


{#
    Macro: ends_with
    Description: Returns true if string ends with suffix.

    Alteryx Equivalent: EndsWith([field], 'suffix')
#}

{% macro ends_with(column, suffix, case_sensitive=true) %}
{% if case_sensitive %}
substr({{ column }}, -{{ suffix | length }}) = '{{ suffix }}'
{% else %}
lower(substr({{ column }}, -{{ suffix | length }})) = lower('{{ suffix }}')
{% endif %}
{% endmacro %}
