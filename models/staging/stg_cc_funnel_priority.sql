SELECT 
    compny_NAME AS company
    , REPLACE (priority, 'loow', 'Low') AS priority
FROM `da-lecture-jvr.raw_data_circle.raw_cc_funnel_priority`