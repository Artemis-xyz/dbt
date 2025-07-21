{% macro ref(model_name) %}
    -- 'Wrap the logic in the execute flag so that the code only executes at run-time, not parse time
    -- https://docs.getdbt.com/reference/dbt-jinja-functions/graph#accessing-models'
    {%- if execute -%}
        -- Get the node for the model so we can obtain it's config
        {%- set node = graph.nodes.get("model." ~ project_name ~ "." ~ model_name) -%}

        -- If node lookup fails, use the builtin ref (seeds)
        {%- if node is none -%}
            {{ return(builtins.ref(model_name)) }}
        {%- endif -%}

        -- Get the relation for the model
        -- If there is an alias, use it, otherwise use the model name
        {%- set relation = adapter.get_relation(
            database=node.database or 'PC_DBT_DB',
            schema=node.schema or 'PROD',
            identifier=node.alias or node.name
        ) -%}

      -- If the relation exists in the dev schema, use it
      {%- if relation is not none -%}
          {{ return(builtins.ref(model_name)) }}
      {%- else -%}
        -- Otherwise, use production data
        {%- set prod_database = node.config.database or 'PC_DBT_DB' -%}

        -- PROD is hardcoded in profiles.yml and will be used as the default prefix for any schema names
        {%- if node.config.schema is none -%}
            {%- set prod_schema = 'PROD' -%}
        {%- else -%}
            {%- set prod_schema = 'PROD_' ~ node.config.schema -%}
        {%- endif -%}

        {%- set prod_model_name = node.config.alias or model_name -%}

        {%- set prod_model_name = node.alias or model_name -%}

        -- Return the below as a relation
        {% set relation = api.Relation.create(database=prod_database, schema=prod_schema, identifier=prod_model_name) %}
        -- {{ return(prod_database.upper() ~ '.' ~ prod_schema.upper() ~ '.' ~ prod_model_name.upper()) }}
    {%- endif -%}
  {%- else -%}
    {{ return(builtins.ref(model_name)) }}
  {%- endif -%}
{% endmacro %}