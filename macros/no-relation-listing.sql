-- {# Macros for disabling listing of relations from the warehouse.
--  Enabled by default for the spellbook profile.  Run dbt compile with the flag --vars 'no-relation-listing: "true"'
--  on the command line to explicitly enable or disable this.
--  This speeds up DBT compile times. It must not be used in combination with dbt run.
--  #}
{% macro get_catalog(information_schema, schemas) -%}
  {% do log(target) %}
  {{ return(adapter.dispatch('get_catalog')(information_schema, schemas)) }}
{%- endmacro %}

{% macro list_schemas(database) -%}
  {% do log(target) %}
  {{ return(adapter.dispatch('list_schemas')(database)) }}
{%- endmacro %}

{% macro list_relations_without_caching(schema_relation) %}
  {{ return(adapter.dispatch('list_relations_without_caching')(schema_relation)) }}
{%- endmacro %}