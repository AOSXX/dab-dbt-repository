WITH nb_products_parcel AS (
    SELECT
        parcel_id
        , SUM(qty) AS qty
        , COUNT(DISTINCT model_name) AS nb_model
    FROM {{ ref('stg_cc_parcel_products') }}
    GROUP BY parcel_id
)

SELECT
    parcel_id
    , parcel_tracking
    , transporter
    , priority
    , date_purchase
    , date_shipping
    , date_delivery
    , date_cancelled
    , EXTRACT(MONTH FROM date_purchase) AS month_purchase
    , CASE
        WHEN date_cancelled IS NOT NULL THEN 'Cancelled'
        WHEN date_shipping IS NULL THEN 'In Progress'
        WHEN date_delivery IS NULL THEN 'In Transit'
        WHEN date_delivery IS NOT NULL THEN 'Delivered'
        ELSE NULL
    END AS status
    , DATE_DIFF(date_shipping, date_purchase, DAY) AS expedition_time
    , DATE_DIFF(date_delivery, date_shipping, DAY) AS transport_time
    , DATE_DIFF(date_delivery, date_purchase, DAY) AS delivery_time
  -- delay
  ,IF(date_delivery IS NULL,NULL,IF(DATE_DIFF(date_delivery,date_purchase,DAY)>5,1,0)) AS delay
	-- Metrics --
	,qty
	,nb_model


    
FROM {{ ref ('stg_cc_parcel') }}
LEFT JOIN nb_products_parcel USING(parcel_id)