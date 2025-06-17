{% macro ref(model_name) %}
    {%- if execute -%}
    {%- set node = graph.nodes.get("model." ~ project_name ~ "." ~ model_name) -%}

    {%- set relation = adapter.get_relation(
      database=node.database,
      schema=node.schema,
      identifier=node.alias or node.name
    ) -%}
    
    {%- if relation is not none -%}
      {{ print('Model exists in dev schema, using it') }}
      {{ return(builtins.ref(model_name)) }}
    {%- else -%}
      {{ print('Model does not exist in dev schema, using production') }}
      {%- set prod_schema = target.schema.replace('DEV_' ~ target.user.upper(), 'PROD') -%}
      {{ return(adapter.quote(prod_schema) ~ '.' ~ adapter.quote(model_name)) }}
    {%- endif -%}
  {%- else -%}
    {{ return(builtins.ref(model_name)) }}
  {%- endif -%}
{% endmacro %}