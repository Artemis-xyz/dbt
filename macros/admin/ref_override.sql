{% macro ref(model_name) %}
    -- 'Wrap the logic in the execute flag so that the code only executes at run-time, not parse time
    -- https://docs.getdbt.com/reference/dbt-jinja-functions/graph#accessing-models'
    {%- if execute -%}
        -- Get the node for the model so we can obtain it's config
        {%- set node = graph.nodes.get("model." ~ project_name ~ "." ~ model_name) -%}

        -- Get the relation for the model
        -- If there is an alias, use it, otherwise use the model name
        {%- set relation = adapter.get_relation(
            database=node.database,
            schema=node.schema,
            identifier=node.alias or node.name
        ) -%}

      -- If the relation exists in the dev schema, use it
      {%- if relation is not none -%}
          {{ return(builtins.ref(model_name)) }}
      {%- else -%}
        -- Otherwise, use production data
        {%- set prod_database = node.config.database -%}

        -- PROD is hardcoded in profiles.yml and will be used as the default prefix for any schema names
        {%- set prod_schema = 'PROD_' ~ node.config.schema -%}

        -- {%- set prod_model_name = node.config.alias or model_name -%}
        {%- set prod_model_name = node.alias or model_name -%}

        {{ return(prod_database.upper() ~ '.' ~ prod_schema.upper() ~ '.' ~ prod_model_name.upper()) }}
    {%- endif -%}
  {%- else -%}
    {{ return(builtins.ref(model_name)) }}
  {%- endif -%}
{% endmacro %}