{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {# 'If there is no custom schema set, use the default schema' #}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {%- if target.name == "dev" -%}

            {{ env_var('SYSTEM_SNOWFLAKE_USER').split('@')[0] | replace('.', '_') | upper }}

        {%- else -%}

            {{ custom_schema_name | trim }}

        {%- endif -%}

    {%- endif -%}

{%- endmacro %}
