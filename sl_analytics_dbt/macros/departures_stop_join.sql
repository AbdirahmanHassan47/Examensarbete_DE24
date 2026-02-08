{% macro departures_stop_join_condition(departures_relation, departures_alias, stops_alias) -%}
  {%- set cols = adapter.get_columns_in_relation(departures_relation) -%}
  {%- set colnames = cols | map(attribute='name') | map('lower') | list -%}
  {%- if 'stop_id' in colnames -%}
    {{ departures_alias }}.stop_id = {{ stops_alias }}.stop_id
  {%- elif 'stop_name' in colnames -%}
    {{ departures_alias }}.stop_name = {{ stops_alias }}.stop_name
  {%- elif '_site_id' in colnames -%}
    to_varchar({{ departures_alias }}._site_id) = {{ stops_alias }}.stop_id
  {%- else -%}
    1 = 0
  {%- endif -%}
{%- endmacro %}
