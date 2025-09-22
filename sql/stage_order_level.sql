-- local_internal.stage_order_level definition

-- Drop table

-- DROP TABLE local_internal.stage_order_level;

CREATE TABLE local_internal.stage_order_level (
	"Buyer NP" varchar NULL,
	"Seller NP" varchar NULL,
	"Network order id" varchar NULL,
	"Provider Id" varchar NULL,
	"Seller Name" varchar NULL,
	"Seller Pincode" varchar NULL,
	"Delivery Pincode" varchar NULL,
	cancellation_code varchar NULL,
	"Created at" timestamp NULL,
	"Date" date NULL,
	"domain" varchar NULL,
	"on-time-del" float8 NULL,
	"Shipped at" timestamp NULL,
	"Ready to Ship" timestamp NULL,
	"Promised time" timestamp NULL,
	tat_dif float8 NULL,
	tat_diff_days float8 NULL,
	day_diff float8 NULL,
	min_diff float8 NULL,
	tat_time float8 NULL,
	no_key int4 NULL,
	"ONDC order_status" varchar NULL,
	"Updated at" timestamp NULL,
	"Category" varchar NULL,
	"Consolidated_category" varchar NULL,
	row_updated_at timestamp NULL,
	network_transaction_id varchar NULL
);