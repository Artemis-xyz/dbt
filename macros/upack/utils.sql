{% macro hex_to_number(v) %}
    {{ target.schema }}.hex_to_int({{ v }}::string)::number
{% endmacro %}

{% macro hex_to_timestamp(v) %}
    {{ target.schema }}.hex_to_int({{ v }}::string)::timestamp
{% endmacro %}

{% macro to_address(v) %} lower({{ v }}::string) {% endmacro %}

{% macro to_json(v) %} parse_json({{ v }}::string) {% endmacro %}

{% macro to_number(v) %} {{ v }}::number {% endmacro %}

{% macro to_float(v) %} {{ v }}::float {% endmacro %}

{% macro to_string(v) %} {{ v }}::string {% endmacro %}

{% macro to_timestamp(v) %} {{ v }}::timestamp {% endmacro %}
