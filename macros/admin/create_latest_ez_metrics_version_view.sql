{% macro create_latest_ez_metrics_version_view() %}

    -- this hook will run only if the model is versioned, it's the latest version, and it has the ez_metrics tag
    -- otherwise, it's a no-op
    {% if model.get('version') and model.get('version') == model.get('latest_version') and 'ez_metrics' in (model.get('tags') or []) %}

        {% set new_relation = this.incorporate(path={"identifier": "ez_metrics"}) %}

        {% set existing_relation = load_relation(new_relation) %}

        {% if existing_relation and not existing_relation.is_view %}
            {{ drop_relation_if_exists(existing_relation) }}
        {% endif %}
        
        {% set create_view_sql -%}
            -- this syntax may vary by data platform
            create or replace view {{ new_relation }}
              as select * from {{ this }}
        {%- endset %}
        
        {% do log("Creating view " ~ new_relation ~ " pointing to " ~ this, info = true) if execute %}
        
        {{ return(create_view_sql) }}
        
    {% else %}
    
        -- no-op
        select 1 as id
    
    {% endif %}

{% endmacro %}