{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {# 'If there is no custom schema set and we are not in dev, use the default schema' #}
    {%- if custom_schema_name is none and target.name != "dev" -%}

        {{ default_schema }}

    {%- else -%}

        {#
            'If using the dev target, use the dev schema for the user.
            Otherwise, use the default behavior of dbt'
        #}
        {%- if target.name == "dev" -%}

            DEV_{{ env_var('SYSTEM_SNOWFLAKE_USER').split('@')[0] | replace('.', '_') | upper }}

        {%- else -%}

            {{ default_schema }}_{{ custom_schema_name | trim }}

        {%- endif -%}

    {%- endif -%}

{%- endmacro %}
