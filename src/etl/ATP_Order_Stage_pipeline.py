import os
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

from datetime import datetime,date
import logging
import time
from urllib.parse import quote_plus  # PY2: from urllib import quote_plus

import pandas as pd
from sqlalchemy.engine import create_engine
from pyathena import connect

import psycopg2
from io import StringIO


def get_logger(name):
    """
    Create and configure a logger for console output.
    
    Args:
        name (str): Name of the logger
        
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Only configure if not already configured
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(console_handler)
        
        # Prevent duplicate logs
        logger.propagate = False
    
    return logger


def get_athena_connection():
    """Get Athena connection with environment variables."""
    return connect(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
        s3_staging_dir=os.getenv("S3_STAGING_DIR"),
        region_name=os.getenv("AWS_REGION"),
        schema_name=os.getenv("DATABASE_NAME")
    )


logger = get_logger("ATP_Order_Stage_data_pipeline")

class DatabaseHandler:
    """Simple database handler for executing SQL queries."""
    
    def execute_sql_psycopg2(self, query):
        """Execute SQL query using psycopg2."""
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PWD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT")
            )
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            logger.info("SQL query executed successfully")
        except Exception as e:
            logger.error(f"Error executing SQL query: {str(e)}")
            if conn:
                conn.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


class ATP_Order_Stage_data_pipeline():
    def __init__(self):
        self.name = "ATP_Order_Stage_data_pipeline"
        self.download_dir = os.getenv("DOWNLOAD_DIRECTORY")
        self.db_handler = DatabaseHandler()

    def fetch_and_insert_data(self, chunksize=50000, target_date=None):
        """
        Fetch data in chunks and insert it into PostgreSQL in real-time.
        
        Args:
            chunksize (int): Number of rows to process in each chunk
            target_date (str, optional): Date in YYYY-MM-DD format. If None, uses current date.
        """
        # Use current date if no target_date provided
        if target_date is None:
            target_date = datetime.now().strftime('%Y-%m-%d')
        
        query = f"""
        with base_table as (
        select
            "network order id",
            "buyer np name",
            "seller np name",
            "fulfillment status",
            coalesce(case
                when "fulfillment status" like '%RTO%'
                and ("cancellation code" is null
                or "f_cancellation_code" is null) then '013'
                when "cancellation code" is null then null
                when substring("cancellation code",-3, 4) not in ('001', '002', '003', '004', '005', '006', '007', '008', '009', '010', '011', '012', '013', '014', '015', '016',
            '017', '018', '019', '020', '021', '022') then '052'
                else substring("cancellation code",-3, 4)
            end,
            case
                when "f_cancellation_code" is null then null
                when substring("f_cancellation_code",-3, 4) not in ('001', '002', '003', '004', '005', '006', '007', '008', '009', '010', '011', '012', '013', '014', '015', '016',
            '017', '018', '019', '020', '021', '022') then '052'
                else substring("f_cancellation_code",-3, 4)
            end,
            '050')as "cancellation_code",
            "f_rto_requested_at",
            row_number() over (partition by ("network order id" || "seller np name"|| "network transaction id" ||
                (case
                when "seller np name" = 'webapi.magicpin.in/oms_partner/ondc'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'F&B'
                when "seller np name" like '%dominos%'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'F&B'
                when "item consolidated category" like 'Agri%'
                or "domain" like '%AGR%' then 'Agriculture'
                when "seller np name" like '%agrevolution%' then 'Agriculture'
                when "seller np name" like '%enam.gov%' then 'Agriculture'
                when "seller np name" like '%crofarm%'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'Grocery'
                when "seller np name" like '%rebelfoods%'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'F&B'
                when "seller np name" like '%uengage%'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'F&B'
                when "seller np name" = 'api.esamudaay.com/ondc/sdk/bpp/retail/lespl'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'F&B'
                when "seller np name" = 'api.kiko.live/ondc-seller'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'Grocery'
                when "item category" = 'F&B' then 'F&B'
                when "item category" = 'Grocery' then 'Grocery'
                when "item category" is not null
                and "item consolidated category" is null then 'Others'
                when "item category" is null then 'Undefined'
                else "item consolidated category"
            end))
        order by
                (
            case
                when "seller np name" = 'webapi.magicpin.in/oms_partner/ondc'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'F&B'
                when "seller np name" like '%dominos%'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'F&B'
                when "item consolidated category" like 'Agri%'
                or "domain" like '%AGR%' then 'Agriculture'
                when "seller np name" like '%agrevolution%' then 'Agriculture'
                when "seller np name" like '%enam.gov%' then 'Agriculture'
                when "seller np name" like '%crofarm%'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'Grocery'
                when "seller np name" like '%rebelfoods%'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'F&B'
                when "seller np name" like '%uengage%'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'F&B'
                when "seller np name" = 'api.esamudaay.com/ondc/sdk/bpp/retail/lespl'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'F&B'
                when "seller np name" = 'api.kiko.live/ondc-seller'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'Grocery'
                when "item category" = 'F&B' then 'F&B'
                when "item category" = 'Grocery' then 'Grocery'
                when "item category" is not null
                and "item consolidated category" is null then 'Others'
                when "item category" is null then 'Undefined'
                else "item consolidated category"
            end) ) max_record_key,
            case
                when "seller np name" = 'webapi.magicpin.in/oms_partner/ondc'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'F&B'
                when "seller np name" like '%dominos%'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'F&B'
                when "item consolidated category" like 'Agri%'
                or "domain" like '%AGR%' then 'Agriculture'
                when "seller np name" like '%agrevolution%' then 'Agriculture'
                when "seller np name" like '%enam.gov%' then 'Agriculture'
                when "seller np name" like '%crofarm%'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'Grocery'
                when "seller np name" like '%rebelfoods%'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'F&B'
                when "seller np name" like '%uengage%'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'F&B'
                when "seller np name" = 'api.esamudaay.com/ondc/sdk/bpp/retail/lespl'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'F&B'
                when "seller np name" = 'api.kiko.live/ondc-seller'
                and "item consolidated category" is null
                or "item consolidated category" = '' then 'Grocery'
                when "item category" = 'F&B' then 'F&B'
                when "item category" = 'Grocery' then 'Grocery'
                when "item category" is not null
                and "item consolidated category" is null then 'Others'
                when "item category" is null then 'Undefined'
                else "item consolidated category"
            end as "item consolidated category",
            "domain",
            date_parse("o_completed on date & time", '%Y-%m-%dT%H:%i:%s') as "Completed at",
            date_parse("o_cancelled at date & time", '%Y-%m-%dT%H:%i:%s') as "Cancelled at",
            date_parse("f_order-picked-up at date & time", '%Y-%m-%dT%H:%i:%s') as "Shipped at",
            date_parse("f_packed at date & time", '%Y-%m-%dT%H:%i:%s') as "Ready to Ship",
            coalesce(date_parse("promised_time_to_deliver_from_on_confirm", '%Y-%m-%dT%H:%i:%s'),
            date_parse("Promised time to deliver Date & Time from on_select", '%Y-%m-%dT%H:%i:%s')) as "Promised time",
            "Delivery Pincode",
            date_parse("O_Created Date & Time", '%Y-%m-%dT%H:%i:%s') as "Created at",
            date(date_parse("O_Created Date & Time", '%Y-%m-%dT%H:%i:%s')) as "Date",
            date_parse("row_updated_at", '%Y-%m-%dT%H:%i:%s') as row_updated_at,
            "provider_id",
            "seller pincode",
            "seller name",
            date_parse("F_Order Delivered at Date & Time From Fulfillments", '%Y-%m-%dT%H:%i:%s') as "Completed at Ful",
            case
                when not("order status" in ('Cancelled', 'Completed')
                or ("order status" like '%Return%')) then 
            (case
                    when ("o_completed on date & time" is not null
                    or "F_Order Delivered at Date & Time From Fulfillments" is not null)
                    and "o_cancelled at date & time" is null then 'Completed'
                    when "o_completed on date & time" is null
                    and "F_Order Delivered at Date & Time From Fulfillments" is null
                    and "o_cancelled at date & time" is not null then 'Cancelled'
                    when ("o_completed on date & time" is not null
                    or "F_Order Delivered at Date & Time From Fulfillments" is not null)
                    and "o_cancelled at date & time" is not null then "order status"
                    else "order status"
                end)
                else "order status"
            end as "order status",
            "network transaction id"
        from "default".shared_order_fulfillment_nhm_fields_view_hudi 
        where date(date_parse("row_updated_at", '%Y-%m-%dT%H:%i:%s')) = DATE('{target_date}')
        and (case 
            when upper(on_confirm_sync_response) = 'NACK' then 1
            when on_confirm_error_code is not null then 1
            else 0
        end) = 0
        ),								
        table1 as (
        select
            "seller np name" as "Seller NP",
                "buyer np name" as "Buyer NP",
                "max_record_key",
                "domain",
                "Created at",
                "Shipped at",
                "Ready to Ship",
                row_updated_at,
                coalesce(case
                when "item consolidated category" = 'F&B' then "Promised time" + interval '5' minute
                else "Promised time"
            end,
            "Created at") as "Promised time",
                "Date",
                "fulfillment status",
                "item consolidated category",
                "network order id" as "Network order id",
            case
                when "order status" is null
                    or "order status" = '' then 'In Process'
                    when "order status" = 'Cancelled' then 'Cancelled'
                    when "order status" = 'Completed' then 'Delivered'
                    when lower("order status") = 'delivered' then 'Delivered'
                    when "order status" like 'Liquid%' then 'Delivered'
                    when "order status" like '%leted' then 'Delivered'
                    when "order status" like 'Return%' then 'Delivered'
                    when "fulfillment status" like 'RTO%' then 'Cancelled'
                    else 'In Process'
                end as "ONDC order_status",
            case
                when "fulfillment status" like '%RTO%' then coalesce("cancellation_code",
                '013')
                when (case
                    when trim("order status") = 'Cancelled' then 'Cancelled'
                    else "fulfillment status"
                end) = 'Cancelled' then coalesce("cancellation_code",
                '050')
                else null
            end as "cancellation_code",
                    case
                    when (case
                    when trim("order status") = 'Completed' then 'Delivered'
                    when trim("fulfillment status") like 'RTO%' then 'Cancelled'
                    when trim("order status") like '%Return%' then 'Delivered'
                    when trim("order status") = 'Cancelled' then 'Cancelled'
                    else 'In Process'
                end) = 'Delivered' then coalesce("Completed at Ful",
                        "Completed at")
                else null
            end as "Completed at",
                    case
                        when (case
                    when trim("fulfillment status") like 'RTO%' then 'Cancelled'
                    when trim(lower("order status")) = 'cancelled' then 'Cancelled'
                    else null
                end) = 'Cancelled' then "Cancelled at"
                else null
            end as "Cancelled at",
                    provider_id ,
                    case
                        when "seller pincode" like '%XXX%' then 'Undefined'
                when "seller pincode" like '' then 'Undefined'
                when "seller pincode" like '%*%' then 'Undefined'
                when "seller pincode" like 'null' then 'Undefined'
                when "seller pincode" is null then 'Undefined'
                else "seller pincode"
            end as "Seller Pincode",
                    case
                        when upper("Delivery Pincode") like '%XXX%' then 'Undefined'
                when "Delivery pincode" like '' then 'Undefined'
                when "Delivery Pincode" like '%*%' then 'Undefined'
                when "Delivery Pincode" like 'null' then 'Undefined'
                when "Delivery Pincode" is null then 'Undefined'
                else "Delivery Pincode"
            end as "Delivery Pincode",
                    lower(trim("seller name")) as "seller name",
                    "network transaction id"
        from
                    base_table),
        merger_table as (
        select
            "Network order id","network transaction id" ,
            ARRAY_JOIN(ARRAY_AGG(distinct "domain" order by "domain"),',') as "domain",
            ARRAY_JOIN(ARRAY_AGG(distinct "ONDC order_status" order by "ONDC order_status"), ',') as "ONDC order_status",
            ARRAY_JOIN(ARRAY_AGG(distinct "item consolidated category" order by "item consolidated category"), ',') as "item consolidated category"
        from
            table1
        group by
            1,2),
        trial as 
        (
        select
            t1."Buyer NP",
            t1."Seller NP",
            t1."Created at",
            t1."Network order id",
            t1."seller name",
            t1."Shipped at",
            t1."Ready to Ship",
            t1."Promised time",
            t1."Date",
            t1."cancellation_code",
            t1."Completed at",
            t1."Cancelled at",
            t1."provider_id",
            t1."Seller Pincode",
            t1."Delivery Pincode",
            t1."max_record_key",
            t1.row_updated_at,
            mt."domain",
            mt."ONDC order_status",
            mt."item consolidated category",
            t1."network transaction id"
        from
            table1 t1
        join merger_table mt on
            t1."Network order id" || t1."network transaction id" = mt."Network order id"|| mt."network transaction id"
        where
            t1."max_record_key" = 1),
        table_l as (
        select
            "Buyer NP",
            "Seller NP",
            "Network order id",
            "domain",
            "item consolidated category" as "Consolidated_category",
            case
                when "ONDC order_status" like '%,%' then 'In Process'
                else "ONDC order_status"
            end as "ONDC order_status",	
            case
                when "item consolidated category" like '%F&B%'
                    and "item consolidated category" like '%Undefined%' then 'F&B'
                    when "item consolidated category" like '%,%' then 'Multi Category'
                    else "item consolidated category"
                end as "Category",
                concat("Seller NP", '_', LOWER("provider_id")) as "provider key",
                "network transaction id",
                ARRAY_JOIN(ARRAY_AGG(distinct "provider_id" order by "provider_id"), ',') as "Provider Id",
                ARRAY_JOIN(ARRAY_AGG(distinct "seller name" order by "seller name"), ',') as "Seller Name",
                max("Seller Pincode") as "Seller Pincode",
                max("Delivery Pincode") as "Delivery Pincode",
                MIN("cancellation_code") as "cancellation_code",
                max("Created at") as "Created at",
                max("Date") as "Date",
                max("Shipped at") as "Shipped at",
                max("Completed at") as "Completed at",
                max("Cancelled at") as "Cancelled at",
                max("Ready to Ship") as "Ready to Ship",
                max("Promised time") as "Promised time",
                max("row_updated_at") as row_updated_at,
                DATE_DIFF('second', max("Promised time"), max("Completed at")) as "tat_dif",
                DATE_DIFF('day', max("Promised time"), max("Completed at")) as "tat_diff_days",
                DATE_DIFF('day', max("Created at"), max("Completed at")) as "day_diff",
                DATE_DIFF('minute', max("Created at"), max("Completed at")) as "min_diff",
                DATE_DIFF('minute', max("Created at"), max("Promised time")) as "tat_time",
                case
                    when row_number() over (partition by "Network order id" || "Seller NP"|| "network transaction id" || "item consolidated category"
                order by
                        MAX("Date") desc) > 1 then 0
                    else 1
                end as "no_key"
            from
                trial
            group by
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,9)
        select
            "Buyer NP",
            "Seller NP",
            "Network order id",
            "Provider Id",
            "Seller Name",
            "Seller Pincode",
            "Delivery Pincode",
            "cancellation_code",
            "Created at",
            "Date",
            "domain",
            case
                when ("ONDC order_status" = 'Delivered'
                    and ("Completed at" <= "Promised time")
                        and "Completed at" is not null) then 1
                else 0
            end as "on-time-del",
            "Shipped at",
            "Ready to Ship",
            "Promised time",
            "tat_dif",
            "tat_diff_days",
            "day_diff",
            "min_diff",
            "tat_time",
            "no_key",
            "ONDC order_status",
            case
                when "ONDC order_status" = 'Delivered' then "Completed at"
                when "ONDC order_status" = 'Cancelled' then "Cancelled at"
                else "Created at"
            end as "Updated at",
            "Category",
            "Consolidated_category",
            row_updated_at,
            "network transaction id" as network_transaction_id
        from
            table_l
        """
        logger.info(f"Worker starting processing data")
        try:
            conn = get_athena_connection()
            for chunk in pd.read_sql_query(query, conn, chunksize=chunksize):
                self.insert_data_batch(chunk)
                time.sleep(5)
        except Exception as e:
            logger.error(f"Error during fetch and insert: {str(e)}")
              # Delay before the next insertion

    def insert_data_batch(self, chunk):
        """
        Insert a chunk of data into PostgreSQL.
        """
        try:
            connpost = psycopg2.connect(
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PWD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT")
            )
            cursor = connpost.cursor()
            buffer = StringIO()
            chunk.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            cursor.copy_expert(
                """
                COPY local_internal.stage_order_level (
                "Buyer NP" ,"Seller NP" ,"Network order id" ,"Provider Id" ,"Seller Name" ,"Seller Pincode" ,"Delivery Pincode" ,
                "cancellation_code" ,"Created at" ,"Date","domain" ,"on-time-del" ,"Shipped at" ,"Ready to Ship" ,"Promised time" ,"tat_dif" ,
                "tat_diff_days" ,"day_diff" ,"min_diff" ,"tat_time" ,"no_key" ,"ONDC order_status" ,"Updated at" ,"Category" ,"Consolidated_category","row_updated_at","network_transaction_id"
                )
                FROM STDIN WITH (FORMAT CSV)
                """,
                buffer
            )
            connpost.commit()
            logger.info("Chunk successfully inserted into PostgreSQL.")
        except Exception as e:
            logger.error(f"Error inserting chunk: {str(e)}")
            if connpost:
                connpost.rollback()
        finally:
            if cursor:
                cursor.close()
            if connpost:
                connpost.close()

    def insert_update(self):

        update_insert_query = """
        INSERT INTO local_internal.order_level AS target (
        "Buyer NP", "Seller NP", "Network order id", "Provider Id", "Seller Name", 
        "Seller Pincode", "Delivery Pincode", cancellation_code, "Created at", "Date", 
        "domain", "on-time-del", "Shipped at", "Ready to Ship", "Promised time", 
        tat_dif, tat_diff_days, day_diff, min_diff, tat_time, no_key, 
        "ONDC order_status", "Updated at", "Category", "Consolidated_category","network_transaction_id"
        )
        SELECT 
        stage."Buyer NP", stage."Seller NP", stage."Network order id", stage."Provider Id", stage."Seller Name", 
        stage."Seller Pincode", stage."Delivery Pincode", stage.cancellation_code, stage."Created at", stage."Date", 
        stage."domain", stage."on-time-del", stage."Shipped at", stage."Ready to Ship", stage."Promised time", 
        stage.tat_dif, stage.tat_diff_days, stage.day_diff, stage.min_diff, stage.tat_time, stage.no_key, 
        stage."ONDC order_status", stage."Updated at", stage."Category", stage."Consolidated_category", stage.network_transaction_id
        FROM local_internal.stage_order_level stage
        ON CONFLICT (lower("Seller NP" || "Network order id" || "Provider Id" || network_transaction_id)) DO UPDATE
        SET 
        "Buyer NP" = EXCLUDED."Buyer NP",
        "Seller NP" = EXCLUDED."Seller NP",
        "Provider Id" = EXCLUDED."Provider Id",
        "Seller Name" = EXCLUDED."Seller Name",
        "Seller Pincode" = EXCLUDED."Seller Pincode",
        "Delivery Pincode" = EXCLUDED."Delivery Pincode",
        cancellation_code = EXCLUDED.cancellation_code,
        "Created at" = EXCLUDED."Created at",
        "Date" = EXCLUDED."Date",
        "domain" = EXCLUDED."domain",
        "on-time-del" = EXCLUDED."on-time-del",
        "Shipped at" = EXCLUDED."Shipped at",
        "Ready to Ship" = EXCLUDED."Ready to Ship",
        "Promised time" = EXCLUDED."Promised time",
        tat_dif = EXCLUDED.tat_dif,
        tat_diff_days = EXCLUDED.tat_diff_days,
        day_diff = EXCLUDED.day_diff,
        min_diff = EXCLUDED.min_diff,
        tat_time = EXCLUDED.tat_time,
        no_key = EXCLUDED.no_key,
        "ONDC order_status" = EXCLUDED."ONDC order_status",
        "Updated at" = EXCLUDED."Updated at",
        "Category" = EXCLUDED."Category",
        "Consolidated_category" = EXCLUDED."Consolidated_category",
        "network_transaction_id" = EXCLUDED.network_transaction_id;
        """
        logger.info(f" Task: Update and Insert Start\n")
        try:
            self.db_handler.execute_sql_psycopg2(update_insert_query)
            logger.info(f" Task: Update and Insert Compelete\n")
        except Exception as e:
            logger.error(f"Error during Update and Insert: {str(e)}")
              # Delay before the next insertion

    def dimension(self):
        queries = {
        "dim_seller_np": """
            INSERT INTO local_internal.dim_seller_np (seller_np)
            SELECT DISTINCT "Seller NP" FROM local_internal.stage_order_level o
            WHERE o."Seller NP" IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM local_internal.dim_seller_np d WHERE o."Seller NP" = d.seller_np
            )
        """,
        "dim_buyer_np": """
            INSERT INTO local_internal.dim_buyer_np (buyer_np)
            SELECT DISTINCT "Buyer NP" FROM local_internal.stage_order_level o
            WHERE o."Buyer NP" IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM local_internal.dim_buyer_np d WHERE o."Buyer NP" = d.buyer_np
            )
        """,
        "dim_category": """
            INSERT INTO local_internal.dim_category (category)
            SELECT DISTINCT "Category" FROM local_internal.stage_order_level o
            WHERE o."Category" IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM local_internal.dim_category d WHERE o."Category" = d.category
            )
        """,
        "dim_provider_key": """
            INSERT INTO local_internal.dim_item_provider_key(provider_k)
            SELECT DISTINCT LOWER(o."Seller NP" || '_' || o."Provider Id") 
            FROM local_internal.stage_order_level o
            WHERE LOWER(o."Seller NP" || '_' || o."Provider Id") IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM local_internal.dim_item_provider_key d 
                WHERE d.provider_k = LOWER(o."Seller NP" || '_' || o."Provider Id")
            )
        """
        }

        for name, query in queries.items():
            try:
                self.db_handler.execute_sql_psycopg2(query)
                logger.info(f"Successfully executed query for {name}\n")
            except Exception as e:
                logger.error(f"Error executing query for {name}: {str(e)}")


    def run(self, target_date=None):
        """
        Main execution logic to process and insert data.
        
        Args:
            target_date (str, optional): Date in YYYY-MM-DD format. If None, uses current date.
        """
        try:
            # Use current date if no target_date provided
            if target_date is None:
                target_date = datetime.now().strftime('%Y-%m-%d')
            
            logger.info(f"Processing data for date: {target_date}")
            
            sql_query = "TRUNCATE local_internal.stage_order_level"
            self.db_handler.execute_sql_psycopg2(sql_query)
            logger.info("Table truncated for full load.")

            time.sleep(5)
            self.fetch_and_insert_data(target_date=target_date)

            time.sleep(30)
            self.insert_update()

            # time.sleep(5)
            # self.dimension()


        except Exception as e:
            logger.error(f"An error occurred during data insertion: {str(e)}")


def main():
    """
    Main function to execute the ATP Order Stage data pipeline.
    This allows the script to be run with 'uv run' command.
    
    Usage:
        python ATP_Order_Stage_pipeline.py [YYYY-MM-DD]
        
    If no date is provided, it will process data for the current date.
    """
    import sys
    
    # Check if date argument is provided
    target_date = None
    if len(sys.argv) > 1:
        try:
            # Validate date format
            target_date = sys.argv[1]
            datetime.strptime(target_date, '%Y-%m-%d')
            logger.info(f"Date argument provided: {target_date}")
        except ValueError:
            logger.error("Invalid date format. Please use YYYY-MM-DD format.")
            sys.exit(1)
    
    logger.info("Starting ATP Order Stage Data Pipeline...")
    
    try:
        # Create pipeline instance
        pipeline = ATP_Order_Stage_data_pipeline()
        
        # Run the pipeline
        pipeline.run(target_date=target_date)
        
        logger.info("ATP Order Stage Data Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
