{{ config(materialized='table') }}

select
    date,
    campaign_id,
    campaign_name,

    -- Platform-level fields (kept for Tableau)
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

    --------------------------------------------------------------------
    -- Aggregated metrics using the macro
    --------------------------------------------------------------------

    {{ safe_sum([
        'meta_mx_cost_usd',
        'meta_non_mx_cost_usd',
        'pinterest_cost_usd',
        'reddit_cost_usd'
    ]) }} as total_spend_usd,

    {{ safe_sum([
        'meta_mx_impressions',
        'meta_non_mx_impressions',
        'pinterest_impressions',
        'reddit_impressions'
    ]) }} as total_impressions,

    {{ safe_sum([
        'meta_mx_clicks',
        'meta_non_mx_clicks',
        'pinterest_clicks',
        'reddit_clicks'
    ]) }} as total_clicks,

    {{ safe_sum([
        'meta_mx_conversions',
        'meta_non_mx_conversions',
        'pinterest_conversions',
        'reddit_conversions'
    ]) }} as total_conversions,

    {{ safe_sum([
        'meta_mx_revenue_usd',
        'meta_non_mx_revenue_usd',
        'pinterest_revenue_usd',
        'reddit_revenue_usd'
    ]) }} as total_revenue_usd,

    -- ROAS
    case 
        when {{ safe_sum([
            'meta_mx_cost_usd',
            'meta_non_mx_cost_usd',
            'pinterest_cost_usd',
            'reddit_cost_usd'
        ]) }} = 0 then null
        else (
            {{ safe_sum([
                'meta_mx_revenue_usd',
                'meta_non_mx_revenue_usd',
                'pinterest_revenue_usd',
                'reddit_revenue_usd'
            ]) }}
        ) / (
            {{ safe_sum([
                'meta_mx_cost_usd',
                'meta_non_mx_cost_usd',
                'pinterest_cost_usd',
                'reddit_cost_usd'
            ]) }}
        )
    end as roas_usd,

    -- CTR
    case 
        when {{ safe_sum([
            'meta_mx_impressions',
            'meta_non_mx_impressions',
            'pinterest_impressions',
            'reddit_impressions'
        ]) }} = 0 then null
        else (
            {{ safe_sum([
                'meta_mx_clicks',
                'meta_non_mx_clicks',
                'pinterest_clicks',
                'reddit_clicks'
            ]) }} * 1.0
        ) / (
            {{ safe_sum([
                'meta_mx_impressions',
                'meta_non_mx_impressions',
                'pinterest_impressions',
                'reddit_impressions'
            ]) }}
        )
    end as ctr,

    -- CPC
    case 
        when {{ safe_sum([
            'meta_mx_clicks',
            'meta_non_mx_clicks',
            'pinterest_clicks',
            'reddit_clicks'
        ]) }} = 0 then null
        else (
            {{ safe_sum([
                'meta_mx_cost_usd',
                'meta_non_mx_cost_usd',
                'pinterest_cost_usd',
                'reddit_cost_usd'
            ]) }}
        ) / (
            {{ safe_sum([
                'meta_mx_clicks',
                'meta_non_mx_clicks',
                'pinterest_clicks',
                'reddit_clicks'
            ]) }}
        )
    end as cpc_usd,

    -- CPM
    case 
        when {{ safe_sum([
            'meta_mx_impressions',
            'meta_non_mx_impressions',
            'pinterest_impressions',
            'reddit_impressions'
        ]) }} = 0 then null
        else (
            {{ safe_sum([
                'meta_mx_cost_usd',
                'meta_non_mx_cost_usd',
                'pinterest_cost_usd',
                'reddit_cost_usd'
            ]) }} * 1000.0
        ) / (
            {{ safe_sum([
                'meta_mx_impressions',
                'meta_non_mx_impressions',
                'pinterest_impressions',
                'reddit_impressions'
            ]) }}
        )
    end as cpm_usd

from {{ ref('prep_unified_social_metrics') }}
