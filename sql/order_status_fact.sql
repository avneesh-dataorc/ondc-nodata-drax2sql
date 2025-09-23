CREATE OR REPLACE VIEW local_internal.order_status_fact AS
SELECT
    -- Dimensions (for filtering/grouping)
    "Date",
    "Seller NP",
    "Buyer NP",
    "Network order id",
    "provider key",
    "Provider Id",
    "Seller Name",
    "Seller Pincode",
    "Delivery Pincode",
    cancellation_code,
    "Created at",
    "Updated at",
    "Shipped at",
    "Ready to Ship",
    "Promised time",
    domain,
    "on-time-del",
    tat_dif,
    tat_diff_days,
    day_diff,
    min_diff,
    tat_time,
    "Category",
    "Consolidated_category",

    -- Base measures (exact DAX measure names)
    no_key AS "Confirmed",
    CASE WHEN "ONDC order_status" = 'Delivered' THEN no_key ELSE 0 END AS "Delivered",
    CASE WHEN "ONDC order_status" = 'Cancelled' THEN no_key ELSE 0 END AS "Cancelled",
    CASE WHEN "ONDC order_status" = 'In Process' THEN no_key ELSE 0 END AS "In Process",
    CASE WHEN "ONDC order_status" = 'Part Delivered' THEN no_key ELSE 0 END AS "Part Delivered",
    
    -- Buyer NP: DISTINCTCOUNT(Ordernhm[Buyer NP])
    -- Note: This will be COUNT(DISTINCT "Buyer NP") in aggregation queries
    
    -- Cancellation code cal: COUNT(Ordernhm[cancellation_code])
    CASE WHEN cancellation_code IS NOT NULL THEN 1 ELSE 0 END AS "Cancellation code cal",

    -- TAT breach measures
    COALESCE(tat_dif, 0) AS "Tat breach",
    CASE WHEN "ONDC order_status" = 'Delivered' AND COALESCE(tat_dif,0) = 0 THEN no_key ELSE 0 END AS "Delivered with TAT",
    CASE WHEN "ONDC order_status" = 'Delivered' AND COALESCE(tat_dif,0) > 0 THEN no_key ELSE 0 END AS "Delivered beyond TAT",

    -- DBO measure
    CASE
      WHEN "ONDC order_status" = 'Delivered'
       AND "Updated at" IS NOT NULL
       AND "Updated at" <= "Promised time"
      THEN no_key ELSE 0
    END AS "DBO",

    -- Breach time measures
    CASE
      WHEN "ONDC order_status" IN ('Delivered','Cancelled') THEN EXTRACT(EPOCH FROM ("Updated at" - "Promised time"))/60.0
      WHEN "ONDC order_status" = 'In Process' THEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - "Promised time"))/60.0
      ELSE NULL
    END AS "Breach (mins)",
    CASE
      WHEN "ONDC order_status" IN ('Delivered','Cancelled') THEN EXTRACT(EPOCH FROM ("Updated at" - "Promised time"))/3600.0
      WHEN "ONDC order_status" = 'In Process' THEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - "Promised time"))/3600.0
      ELSE NULL
    END AS "Breach (hrs)",
    CASE
      WHEN "ONDC order_status" IN ('Delivered','Cancelled') THEN EXTRACT(EPOCH FROM ("Updated at" - "Promised time"))/86400.0
      WHEN "ONDC order_status" = 'In Process' THEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - "Promised time"))/86400.0
      ELSE NULL
    END AS "Breach (days)",

    -- Delivery ageing measures
    CASE WHEN "ONDC order_status" = 'Delivered' AND day_diff = 0 THEN no_key ELSE 0 END AS "Same Day",
    CASE WHEN "ONDC order_status" = 'Delivered' AND day_diff = 1 THEN 1 ELSE 0 END AS "Next Day",
    CASE WHEN "ONDC order_status" = 'Delivered' AND day_diff = 2 THEN 1 ELSE 0 END AS "Day + 2",
    CASE WHEN "ONDC order_status" = 'Delivered' AND day_diff > 2 THEN 1 ELSE 0 END AS "More than 2 Days",
    COALESCE("on-time-del", 0) AS "On time deliveries",

    -- Average measures (for AVG aggregation)
    CASE WHEN "ONDC order_status" = 'Delivered' THEN tat_time END AS "Average TAT (in mins)",
    CASE WHEN "ONDC order_status" = 'Delivered' AND min_diff > 0 THEN min_diff END AS "Avg Delivery (mins)",
    CASE
      WHEN "ONDC order_status" = 'Delivered' AND "Category" = 'F&B'
      THEN EXTRACT(EPOCH FROM ("Updated at" - "Created at"))/60.0
      ELSE NULL
    END AS "F&B mins diff",

    -- F&B minute bucket measures
    CASE WHEN "ONDC order_status"='Delivered' AND "Category"='F&B' AND min_diff <= 15 THEN no_key ELSE 0 END AS "Within 15 mins",
    CASE WHEN "ONDC order_status"='Delivered' AND "Category"='F&B' AND min_diff > 15 AND min_diff<=30 THEN no_key ELSE 0 END AS "15 - 30 mins",
    CASE WHEN "ONDC order_status"='Delivered' AND "Category"='F&B' AND min_diff > 30 AND min_diff<=45 THEN no_key ELSE 0 END AS "30 - 45 mins",
    CASE WHEN "ONDC order_status"='Delivered' AND "Category"='F&B' AND min_diff > 45 AND min_diff<=60 THEN no_key ELSE 0 END AS "45 - 60 mins",
    CASE WHEN "ONDC order_status"='Delivered' AND "Category"='F&B' AND min_diff > 60 AND min_diff<=120 THEN 1 ELSE 0 END AS "60 - 120 mins",
    CASE WHEN "ONDC order_status"='Delivered' AND "Category"='F&B' AND min_diff > 120 THEN 1 ELSE 0 END AS "Above 120 mins",

    -- TAT time bucket measures (seconds)
    CASE WHEN "ONDC order_status"='Delivered' AND tat_dif > 0 AND tat_dif<=300 THEN no_key ELSE 0 END AS "TAT < 5 mins",
    CASE WHEN "ONDC order_status"='Delivered' AND tat_dif > 300 AND tat_dif<=900 THEN no_key ELSE 0 END AS "TAT 5-15 mins",
    CASE WHEN "ONDC order_status"='Delivered' AND tat_dif > 900 AND tat_dif<=1800 THEN no_key ELSE 0 END AS "TAT 15-30 mins",
    CASE WHEN "ONDC order_status"='Delivered' AND tat_dif > 1800 AND tat_dif<=3600 THEN no_key ELSE 0 END AS "TAT 30-60 mins",
    CASE WHEN "ONDC order_status"='Delivered' AND tat_dif > 3600 THEN no_key ELSE 0 END AS "TAT > 60 mins",

    -- TAT day bucket measures
    CASE WHEN "ONDC order_status"='Delivered' AND tat_diff_days = 1 THEN no_key ELSE 0 END AS "TAT+1",
    CASE WHEN "ONDC order_status"='Delivered' AND tat_diff_days = 2 THEN no_key ELSE 0 END AS "TAT + 2",
    CASE WHEN "ONDC order_status"='Delivered' AND tat_diff_days = 3 THEN no_key ELSE 0 END AS "TAT + 3",
    CASE WHEN "ONDC order_status"='Delivered' AND tat_diff_days = 4 THEN no_key ELSE 0 END AS "TAT + 4",
    CASE WHEN "ONDC order_status"='Delivered' AND tat_diff_days > 4 THEN no_key ELSE 0 END AS "TAT > 4",

    -- TAT (Same day) measure
    CASE
      WHEN "ONDC order_status"='Delivered'
       AND tat_diff_days < 1
       AND "Promised time" < "Updated at"
      THEN no_key ELSE 0
    END AS "TAT (Same day)",

    -- Non F&B days diff measure
    CASE
      WHEN "ONDC order_status" = 'Delivered' AND "Category" <> 'F&B'
      THEN EXTRACT(DAY FROM ("Updated at" - "Created at"))
      ELSE NULL
    END AS "Non F&B days diff",

    -- Shipped mark measure
    CASE WHEN "Shipped at" IS NOT NULL THEN no_key ELSE 0 END AS "Shipped mark"

FROM local_internal."Ordernhm_last_2_months";