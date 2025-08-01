{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {# 
        'If using the dev target, use the name of the model file as its alias.
        Otherwise, use the default behavior of dbt'
    #}
    {%- if target.name == "dev" and not node.version -%}
        {{ return(node.name) }}
    {%- endif -%}

    {#
        'If a custom alias is provided, use it.
        Otherwise, use the name of the node and append the version if it exists'
    #}
    {%- if custom_alias_name -%}

        {{ custom_alias_name | trim }}

    {%- elif node.version -%}

        {{ return(node.name ~ "_v" ~ (node.version | replace(".", "_"))) }}

    {%- else -%}

        {{ node.name }}

    {%- endif -%}

{%- endmacro %}