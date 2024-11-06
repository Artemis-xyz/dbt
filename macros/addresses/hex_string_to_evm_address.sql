{% macro hex_string_to_evm_address(hex_string) %}
    PC_DBT_DB.PROD.HEX_STRING_TO_EVM_ADDRESS({{ hex_string }})
{% endmacro %}
