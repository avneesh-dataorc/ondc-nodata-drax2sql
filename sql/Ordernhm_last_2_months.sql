-- local_internal."Ordernhm_last_2_months" source

CREATE OR REPLACE VIEW local_internal."Ordernhm_last_2_months"
AS SELECT ol."Buyer NP",
    ol."Seller NP",
    ol."Network order id",
    lower((ol."Seller NP"::text || '_'::text) || ol."Provider Id"::text) AS "provider key",
    ol."Provider Id",
    ol."Seller Name",
    ol."Seller Pincode",
    ol."Delivery Pincode",
    ol.cancellation_code,
    ol."Created at",
    ol."Date",
    ol.domain,
    ol."on-time-del",
    ol."Shipped at",
    ol."Ready to Ship",
    ol."Promised time",
    ol.tat_dif,
    ol.tat_diff_days,
    ol.day_diff,
    ol.min_diff,
    ol.tat_time,
    ol.no_key,
    ol."ONDC order_status",
    ol."Updated at",
    ol."Category",
    ol."Consolidated_category"
   FROM local_internal.order_level ol
  WHERE ol."Date" >= '2025-08-01'::date;