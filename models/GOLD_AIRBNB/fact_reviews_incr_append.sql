{{
    config(
    materialized = 'incremental',
    incremental_strategy='append'
    )
}}
SELECT * FROM {{ ref('stg_reviews') }}
WHERE review_text is not null
{% if is_incremental() %}
and review_date > (select max(review_date) from {{this}})
{% endif %}
