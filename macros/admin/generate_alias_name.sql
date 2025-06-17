{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if target.name == "dev" -%}
        {{ return(node.name) }}
    {%- endif -%}


    {%- if custom_alias_name -%}

        {{ custom_alias_name | trim }}

    {%- elif node.version -%}

        {{ return(node.name ~ "_v" ~ (node.version | replace(".", "_"))) }}

    {%- else -%}

        {{ node.name }}

    {%- endif -%}

{%- endmacro %}