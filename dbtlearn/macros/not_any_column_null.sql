{% macro not_any_column_null(model) %}
  SELECT * FROM {{ model }} WHERE
  {% for col in adapter.get_columns_in_relation(model) %}
        {{ col.column }} IS NULL OR
  {%endfor%}
  FALSE
{%endmacro%}