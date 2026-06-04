{% macro weather_level(temp_column) %}

case
     when {{temp_column}} >= 36 then 'Extreme'
     when {{temp_column}} >= 32 then 'Hot'
     when {{temp_column}} >= 28 then 'Warm'
     else 'Normal'
end

{% endmacro %}