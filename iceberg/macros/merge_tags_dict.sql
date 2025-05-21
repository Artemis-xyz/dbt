{% macro merge_tags_dict(tag_dict) %}
  -- This macro generates a string that will be executed during the run phase
  -- The key is to return a string that contains SQL, not to execute SQL directly
  
  {% set database = model.database %}
  {% set schema = model.schema %}
  {% set alias = model.alias %}
  
  -- Return a string containing the SQL to be executed
  {% set sql_string %}
    -- Create table if it doesn't exist
    create table if not exists artemis_iceberg.metadata.table_tags (
        database_name string,
        schema_name string,
        table_name string,
        tag_key string,
        tag_value string,
        tagged_at timestamp
    );
    
    {% for k, v in tag_dict.items() %}
    -- Merge statement for tag {{ k }}
    merge into artemis_iceberg.metadata.table_tags t
    using (
        select
            '{{ database }}' as database_name,
            '{{ schema }}' as schema_name,
            '{{ alias }}' as table_name,
            '{{ k }}' as tag_key,
            '{{ v }}' as tag_value,
            current_timestamp as tagged_at
    ) s
    on t.database_name = s.database_name
       and t.schema_name = s.schema_name
       and t.table_name = s.table_name
       and t.tag_key = s.tag_key
    when matched then update set
        tag_value = s.tag_value,
        tagged_at = s.tagged_at
    when not matched then insert (
        database_name, schema_name, table_name, tag_key, tag_value, tagged_at
    ) values (
        s.database_name, s.schema_name, s.table_name, s.tag_key, s.tag_value, s.tagged_at
    );
    {% endfor %}
  {% endset %}
  
  -- Return the SQL as a string
  {{ return(sql_string) }}
{% endmacro %}