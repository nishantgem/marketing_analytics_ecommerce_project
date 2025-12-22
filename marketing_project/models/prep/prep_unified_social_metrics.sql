{{ config(materialized='view') }}

with meta_mx as (
    select * from {{ ref('stg_meta_mx') }}
),

meta_non_mx as (
    select * from {{ ref('stg_meta_non_mx') }}
),

pinterest as (
    select * from {{ ref('stg_pinterest') }}
),

reddit as (
    select * from {{ ref('stg_reddit') }}
),

unified as (

    select
        -- Join keys
        coalesce(mx.campaign_id, mn.campaign_id, p.campaign_id, r.campaign_id) as campaign_id,

        -- Convert nanoseconds → microseconds → timestamp → date (BigQuery)
        date(
            timestamp_micros(
                coalesce(mx.date, mn.date, p.date, r.date) / 1000
            )
        ) as date,

        -- Platform identifiers
        mx.platform as meta_mx_platform,
        mn.platform as meta_non_mx_platform,
        p.platform