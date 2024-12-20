{% macro yesterday_date() %}
  {% set partition_key = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=1)).strftime("%Y-%m-%d") %}
{% endmacro %}