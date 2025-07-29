{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- set default_database = target.database -%}

    {# 'If there is no custom db set and we are not in dev, use the default db' #}
    {%- if custom_database_name is none and target.name != "dev" -%}

        {{ default_database }}

    {%- else -%}

        {#-
            'If the custom db is set but we are in dev, set
            the output db to the default db of the dev target
            
            If the custom db is set but we are not in dev, set
            the output db to the custom db'
        #}

        {%- if target.name == "dev" -%}

            DEV

        {%- else -%}

            {{ custom_database_name | trim }}

        {%- endif -%}

    {%- endif -%}

{%- endmacro %}
