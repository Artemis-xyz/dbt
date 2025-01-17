{% macro big_endian_hex_to_decimal(hex_string) %}
    PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL({{ hex_string }})
{% endmacro %}
