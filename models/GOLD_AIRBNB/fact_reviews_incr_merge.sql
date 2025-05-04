{{
    config(
    materialized = 'incremental',
    unique_key = ['listing_id','reviewer_name'],
    merge_update_columns = ['review_text','review_date','review_sentiment']
    )
}}
SELECT * FROM {{ ref('stg_reviews') }}
WHERE review_text is not null
{% if is_incremental() %}
and review_date > (select max(review_date) from {{this}})
{% endif %}
