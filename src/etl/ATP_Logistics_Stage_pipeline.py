import os
import pandas as pd
import requests
from .base_dashboard import DashboardBase,TransformationBase
from concurrent.futures import ThreadPoolExecutor, as_completed
from utility.logger import get_logger, log_process
from utility.utils import retry
from utility.etl import ETL
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

from datetime import datetime,date
from utility.db_utils import DBHandler

import logging.config
import time
from urllib.parse import quote_plus  # PY2: from urllib import quote_plus

import pandas as pd
from sqlalchemy.engine import create_engine
from pyathena import connect

import psycopg2
from psycopg2 import sql
from io import StringIO


conn = connect(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
            s3_staging_dir=os.getenv("S3_STAGING_DIR"),
            region_name=os.getenv("AWS_REGION"),
            schema_name=os.getenv("DATABASE_NAME")
        )


logger = get_logger("ATP_Logistics_Stage_data_pipeline")

class ATP_Logistics_Stage_data_pipeline(DashboardBase):
    def __init__(self):
        self.name = "ATP_Logistics_Stage_data_pipeline"
        super().__init__(self.name, None)
        self.download_dir = os.getenv("DOWNLOAD_DIRECTORY")

    def fetch_and_insert_data(self, chunksize=50000):
        """
        Fetch data in chunks and insert it into PostgreSQL in real-time.
        """
        query = f"""
        with master_log as (
        select
            "bap_id",
            "bpp_id",
            "order_id",
            "transaction_id",
            "fulfillment_status",
            "fulfillment_type",
            case
                when latest_order_status = 'Cancelled' 
            then coalesce(REGEXP_EXTRACT(SUBSTRING("cancellation_code", -3, 3),
                '[0-9]+'),
                '052')
                else null
            end as "cancellation_code",
            "latest_order_status",
            "network_retail_order_id",
            case
                when "pick_up_pincode" like '%XXX%' then 'Undefined'
                when "pick_up_pincode" like '' then 'Undefined'
                when "pick_up_pincode" like '%*%' then 'Undefined'
                when "pick_up_pincode" like 'null' then 'Undefined'
                when "pick_up_pincode" is null then 'Undefined'
                else "pick_up_pincode"
            end as "pick_up_pincode",
            case
                when upper("delivery_pincode") like '%XXX%' then 'Undefined'
                when "delivery_pincode" like '' then 'Undefined'
                when "delivery_pincode" like '%*%' then 'Undefined'
                when "delivery_pincode" like 'null' then 'Undefined'
                when "delivery_pincode" is null then 'Undefined'
                else "delivery_pincode"
            end as "delivery_pincode",
            "network_retail_order_category",
            "shipment_type",
            "motorable_distance",
            cod_order,
            "provider_name",
            case
                when UPPER(on_confirm_sync_response) = 'NACK' then 1
                when on_confirm_error_code is not null then 1
                else 0
            end as drop_row,
            pickup_tat_duration,
            rts_tat_duration,
            date(date_parse(order_created_at,
            '%Y-%m-%dT%H:%i:%s')) as "date",
            date_parse(order_created_at,
            '%Y-%m-%dT%H:%i:%s') as order_created_at,
            date_parse("o_completed_on_date",
            '%Y-%m-%dT%H:%i:%s') as o_completed_on_date,
            date_parse("o_cancelled_at_date",
            '%Y-%m-%dT%H:%i:%s') as o_cancelled_at_date,
            date_parse("Promised time to deliver",
            '%Y-%m-%dT%H:%i:%s') as promised_time_to_deliver,
            date_parse("f_order_delivered_at_date",
            '%Y-%m-%dT%H:%i:%s') as f_order_delivered_at_date,
            date_parse("f_order_picked_up_date",
            '%Y-%m-%dT%H:%i:%s') as f_order_picked_up_date,
            date_parse("f_out_for_delivery_since_date",
            '%Y-%m-%dT%H:%i:%s') as f_out_for_delivery_since_date,
            date_parse("f_ready_to_ship_at_date",
            '%Y-%m-%dT%H:%i:%s') as f_ready_to_ship_at_date,		
            date_parse("f_cancelled_at_date",
            '%Y-%m-%dT%H:%i:%s') as f_cancelled_at_date,		
            date_parse("f_agent_assigned_at_date",
            '%Y-%m-%dT%H:%i:%s') as f_agent_assigned_at_date,		
            date_parse("pickup_tat",
            '%Y-%m-%dT%H:%i:%s') as pickup_tat,		
            date_parse("rts_tat",
            '%Y-%m-%dT%H:%i:%s') as rts_tat,
            date_parse("f_order_picked_up_date_from_fulfillment",
            '%Y-%m-%dT%H:%i:%s') as f_order_picked_up_date_from_fulfillment,		
            date_parse("f_at_pickup_from_date",
            '%Y-%m-%dT%H:%i:%s') as f_at_pickup_from_date,
            date_parse("f_at_delivery_from_date",
            '%Y-%m-%dT%H:%i:%s') as f_at_delivery_from_date,
            date(substring(row_updated_at , 1, 10)) as row_updated
        from
            "default".shared_logistics_item_fulfillment_view_with_date
        where
            date(substring(row_updated_at , 1, 10)) = date(now()))
        select
            m1."bap_id",
            m1."bpp_id",
            m1."order_id",
            m1."transaction_id",
            m1."fulfillment_status",
            m1.cod_order,
            m1."date",
            m1."order_created_at",	
            m1."cancellation_code",
            m1."latest_order_status",
            m1."network_retail_order_id" as retail_order_id,
            m1."pick_up_pincode",
            m1."delivery_pincode",
            m1."network_retail_order_category" as retail_category,
            MAX(m1."shipment_type") as shipment_type,
            MAX(m1."motorable_distance") as motorable_distance,
            MAX(m1."provider_name") as provider_name,
            MAX(m1.pickup_tat) as pickup_tat,
            MAX(m1.rts_tat) as rts_tat,
            MAX(m1.f_ready_to_ship_at_date) as f_ready_to_ship_at_date,
            MAX(m1.f_at_pickup_from_date) as f_at_pickup_from_date,
            MAX(m1.f_agent_assigned_at_date) as f_agent_assigned_at_date,
            MAX(
            coalesce(
            m1.f_at_delivery_from_date,
            m1.o_completed_on_date,
            m1.f_order_delivered_at_date
            )
        ) as delivered_date,
            MAX(
            coalesce(
            m1.f_cancelled_at_date,
            m1.o_cancelled_at_date
            )
        ) as cancelled_date,
            MAX(
            coalesce(
            m1.f_order_picked_up_date_from_fulfillment,
            m1.f_order_picked_up_date,
            m1.f_out_for_delivery_since_date
            )
        ) as picked_date,
            MAX(m1.drop_row) as drop_row,
            MAX(m1.row_updated) as row_updated,
            MAX(m1.promised_time_to_deliver) as promised_time_to_deliver,
            MAX(m1."fulfillment_type") as fulfillment_type	
        from
            master_log m1
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14;
        """
        logger.info(f"Worker starting processing data")
        try:
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
                COPY logistics_incremental.inc_log (
                bap_id,bpp_id,order_id,transaction_id,fulfillment_status,cod_order,"date",order_created_at,
                cancellation_code,latest_order_status,retail_order_id,pick_up_pincode,delivery_pincode,retail_category,
                shipment_type,motorable_distance,provider_name,pickup_tat,rts_tat,f_ready_to_ship_at_date,f_at_pickup_from_date,
                f_agent_assigned_at_date,delivered_date,cancelled_date,picked_date,drop_row,row_updated,promised_time_to_deliver,
                fulfillment_type
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

    def delete_rows(self):

        delete_query = """
        DELETE FROM logistics_incremental.master_log target
        USING logistics_incremental.inc_log stage
        WHERE target.bpp_id = stage.bpp_id
        AND target.order_id = stage.order_id
        AND target.transaction_id = stage.transaction_id
        AND target.order_created_at = stage.order_created_at;
        """
        logger.info(f" Task: Logistics rows deleted\n")
        try:
            self.db_handler.execute_sql_psycopg2(delete_query)
            logger.info(f" Task: Logistics rows deleted Compelete\n")
        except Exception as e:
            logger.error(f"Error Logistics rows deleted: {str(e)}")
              # Delay before the next insertion

    def insert_rows(self):

        insert_query = """
        INSERT INTO logistics_incremental.master_log (
            bap_id, bpp_id, order_id, transaction_id, fulfillment_status,
            cod_order, "date", order_created_at, cancellation_code,
            latest_order_status, retail_order_id, pick_up_pincode,
            delivery_pincode, retail_category, shipment_type,
            motorable_distance, provider_name, pickup_tat, rts_tat,
            f_ready_to_ship_at_date, f_at_pickup_from_date, f_agent_assigned_at_date,
            delivered_date, cancelled_date, picked_date, drop_row, row_updated, promised_time_to_deliver,fulfillment_type
        )
        SELECT
            stage.bap_id, stage.bpp_id, stage.order_id, stage.transaction_id, stage.fulfillment_status,
            stage.cod_order, stage."date", stage.order_created_at, stage.cancellation_code,
            stage.latest_order_status, stage.retail_order_id, stage.pick_up_pincode,
            stage.delivery_pincode, stage.retail_category, stage.shipment_type,
            stage.motorable_distance, stage.provider_name, stage.pickup_tat, stage.rts_tat,
            stage.f_ready_to_ship_at_date, stage.f_at_pickup_from_date, stage.f_agent_assigned_at_date,
            stage.delivered_date, stage.cancelled_date, stage.picked_date, stage.drop_row, stage.row_updated,
            stage.promised_time_to_deliver,stage.fulfillment_type
        FROM logistics_incremental.inc_log stage;
        """
        logger.info(f" Task: Logistics rows insert start\n")
        try:
            self.db_handler.execute_sql_psycopg2(insert_query)
            logger.info(f" Task: Logistics rows insert Compelete\n")
        except Exception as e:
            logger.error(f"Error Logistics rows insert: {str(e)}")


    @log_process(logger)
    def run(self):
        """
        Main execution logic to process and insert data.
        """
        try:
            sql_query = "TRUNCATE logistics_incremental.inc_log"
            self.db_handler.execute_sql_psycopg2(sql_query)
            logger.info("Table Logistics Stage truncated .")

            time.sleep(5)
            self.fetch_and_insert_data()

            time.sleep(5)
            self.delete_rows()

            time.sleep(5)
            self.insert_rows()


        except Exception as e:
            logger.error(f"An error occurred during data insertion: {str(e)}")
