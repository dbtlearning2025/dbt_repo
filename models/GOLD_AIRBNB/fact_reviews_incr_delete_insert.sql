{{
    config(
    materialized = 'incremental',
    unique_key= ['listing_id','reviewer_name'],
    incremental_strategy='delete+insert',
    )
}}
SELECT * FROM {{ ref('stg_reviews') }}
WHERE review_text is not null
{% if is_incremental() %}
and review_date > (select max(review_date) from {{this}})
{% endif %}
