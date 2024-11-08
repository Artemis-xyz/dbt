{% macro is_nyc_operating_hours(timestamp_column) %}
    case 
        when date_part('DOW', convert_timezone('UTC', 'America/New_York', {{ timestamp_column }})) in (0, 6) then false
        when convert_timezone('UTC', 'America/New_York', {{ timestamp_column }})::time between '09:00:00' and '15:59:59' then true
        else false
    end
{% endmacro %}