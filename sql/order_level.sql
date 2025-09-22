-- local_internal.order_level definition

-- Drop table

-- DROP TABLE local_internal.order_level;

CREATE TABLE local_internal.order_level (
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
	network_transaction_id varchar NULL,
	modified_date date DEFAULT CURRENT_DATE NULL
);
CREATE INDEX idx_buyer_np ON local_internal.order_level USING btree ("Buyer NP");
CREATE INDEX idx_category ON local_internal.order_level USING btree ("Category");
CREATE INDEX idx_date ON local_internal.order_level USING btree ("Date");
CREATE INDEX idx_ondc_order_status ON local_internal.order_level USING btree ("ONDC order_status");
CREATE INDEX idx_provider_id ON local_internal.order_level USING btree ("Provider Id");
CREATE INDEX idx_seller_np ON local_internal.order_level USING btree ("Seller NP");
CREATE UNIQUE INDEX idx_seller_np_network_order ON local_internal.order_level USING btree (lower((((("Seller NP")::text || ("Network order id")::text) || ("Provider Id")::text) || (network_transaction_id)::text)));