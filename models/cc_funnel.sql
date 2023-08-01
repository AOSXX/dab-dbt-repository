{{
    config(
        materialized='table',
        partition_by={
            "field": "date_lead",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

with
    clean_cc_funnel as (select * from {{ ref("stg_cc_funnel") }}),
    clean_cc_funnel_priority as (select * from {{ ref("stg_cc_funnel_priority") }})

select
    # ## KEY ###
    company,
    # ##########
    -- prospect infos --
    sector,
    priority,
    -- date --
    date_lead,
    date_opportunity,
    date_customer,
    date_lost,
    -- deal stage --
    case
        when date_lost is not null
        then '4 - Lost'
        when date_customer is not null
        then '3 - Customer'
        when date_opportunity is not null
        then '2 - Opportunity'
        when date_lead is not null
        then '1 - Lead'
        else null
    end as deal_stage,
    -- rate --
    case
        when date_lost is not null
        then 0
        when date_customer is not null
        then 1
        else null
    end as lead2customer,
    case
        when date_lost is not null
        then 0
        when date_opportunity is not null
        then 1
        else null
    end as lead2opportunity,
    case
        when date_lost is not null and date_opportunity is not null
        then 0
        when date_customer is not null
        then 1
        else null
    end as opportunity2customer,
    -- time --
    date_diff(date_customer, date_lead, day) as lead2customer_time,
    date_diff(date_opportunity, date_lead, day) as lead2opportunity_time,
    date_diff(date_customer, date_opportunity, day) as opportunity2customer_time
from clean_cc_funnel
left join clean_cc_funnel_priority using (company)
