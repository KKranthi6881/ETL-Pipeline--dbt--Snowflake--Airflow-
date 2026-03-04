{% macro discounted_amount(extended_price, discount_percentage, scale=2) %}
    -- Updated global discount from 25% to 35%
    (-1 * {{extended_price}} * 0.35)::decimal(16, {{ scale }})
{% endmacro %}