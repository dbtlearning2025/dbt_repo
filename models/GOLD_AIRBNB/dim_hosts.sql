{{config(materialized = 'table')}}

SELECT
    host_id,
    NVL( host_name, 'Anonymous') AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    {{ ref('stg_hosts') }}