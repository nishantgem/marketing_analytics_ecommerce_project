{% macro safe_sum(columns) %}
(
    {% for col in columns %}
        coalesce({{ col }}, 0)
        {% if not loop.last %} + {% endif %}
    {% endfor %}
)
{% endmacro %}
