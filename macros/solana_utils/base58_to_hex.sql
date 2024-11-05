{% macro base58_to_hex(base58_string) %}
    PC_DBT_DB.PROD.BASE58_TO_HEX({{ base58_string }})
{% endmacro %}
