{% macro hex_to_base58(hex_string) %}
    PC_DBT_DB.PROD.HEX_TO_BASE58({{ hex_string }})
{% endmacro %}
