--How to handle deleted records from source 
--in dbt incremental materialization.
--Incremental materialization handle only inserts and updates.
--Then how to handle deleted records from source table.
--These deleted records also deleted from target table.
{{
    config(
    materialized = 'incremental',
    unique_key = ['listing_id','reviewer_name'],
    post_hook=[ "DELETE FROM {{ this }} WHERE  (listing_id,reviewer_name)  NOT IN (SELECT listing_id,reviewer_name FROM {{ ref('stg_reviews') }})"
    ]

    )
}}
SELECT * FROM {{ ref('stg_reviews') }}
WHERE review_text is not null
{% if is_incremental() %}
and review_date > (select max(review_date) from {{this}})
{% endif %}
