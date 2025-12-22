{{ config(materialized='table') }}

select
    date,
    campaign_id,
    campaign_name_cn,
    platform,
    funding_source_fs,
    kpi_pk,
    objective_ob,
    quarter,
    month,
    cw_iso,
    year,
    division_bs,
    media_channel_ch,
    type,
    platform_pl,
    data_source,
    sub_brand_sb,
    product_category_pr,
    mindset_md,
    business_activity,

    -- Platform metrics
    meta_mx_cost_usd,
    meta_non_mx_cost_usd,
    pinterest_cost_usd,
    reddit_cost_usd,

    meta_mx_impressions,
    meta_non_mx_impressions,
    pinterest_impressions,
    reddit_impressions,

    meta_mx_clicks,
    meta_non_mx_clicks,
    pinterest_clicks,
    reddit_clicks,

    meta_mx_conversions,
    meta_non_mx_conversions,
    pinterest_conversions,
    reddit_conversions,

    meta_mx_revenue_usd,
    meta_non_mx_revenue_usd,
    pinterest_revenue_usd,
    reddit_revenue_usd,

    -- Aggregates
    {{ safe_sum(['meta_mx_cost_usd','meta_non_mx_cost_usd','pinterest_cost_usd','reddit_cost_usd']) }} as total_spend_usd,
    {{ safe_sum(['meta_mx_impressions','meta_non_mx_impressions','pinterest_impressions','reddit_impressions']) }} as total_impressions,
    {{ safe_sum(['meta_mx_clicks','meta_non_mx_clicks','pinterest_clicks','reddit_clicks']) }} as total_clicks,
    {{ safe_sum(['meta_mx_conversions','meta_non_mx_conversions','pinterest_conversions','reddit_conversions']) }} as total_conversions,
    {{ safe_sum(['meta_mx_revenue_usd','meta_non_mx_revenue_usd','pinterest_revenue_usd','reddit_revenue_usd']) }} as total_revenue_usd

from {{ ref('prep_unified_social_metrics') }}
