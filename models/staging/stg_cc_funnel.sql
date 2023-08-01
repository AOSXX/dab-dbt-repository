SELECT
    sector
    , company
    , date_lead
    , opportunity_DATE AS date_opportunity
    , CAST(date_customer AS date) AS date_customer
    , date_lost AS date_lost
FROM 
    `da-lecture-jvr.raw_data_circle.raw_cc_funnel`