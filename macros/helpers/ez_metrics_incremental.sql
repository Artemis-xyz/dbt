{% macro ez_metrics_incremental(date_column, backfill_date=None) %}
    {% if is_incremental() %}
        {% if backfill_date %}
            where DATE({{ date_column }}) >= DATE('{{ backfill_date }}')
        {% else %}
            where {{ date_column }} > (select max(this.date) from {{ this }} as this)
        {% endif %}
    {% endif %}
{% endmacro %}