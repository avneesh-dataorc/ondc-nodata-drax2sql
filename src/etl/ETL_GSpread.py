import os
import sys
import logging
import pandas as pd
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# Handle imports for both direct execution and module execution
try:
    from ..common.gspread_util import GoogleSheetsClient
    from ..common.db_util import get_db_manager
except ImportError:
    # Add the src directory to the path for direct execution
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from common.gspread_util import GoogleSheetsClient
    from common.db_util import get_db_manager

load_dotenv()


class PincodeETL:
    """
    ETL class for processing pincode data from Google Spreadsheets.
    Handles data transformation, mapping, and database operations.
    """
    
    def __init__(self, config_dir: Optional[str] = None):
        """
        Initialize the Pincode ETL processor.
        
        Args:
            config_dir: Directory containing Google Sheets credentials.
        """
        self.config_dir = config_dir or os.getenv('CONFIG_DIR')
        self.gsheets_client = GoogleSheetsClient(self.config_dir)
        self.db_manager = get_db_manager()
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger for ETL operations."""
        logger = logging.getLogger(f"{__name__}.PincodeETL")
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
    
    def transform_pincode_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform pincode data for database insertion.
        
        Args:
            df: Raw DataFrame from Google Sheets
            
        Returns:
            pd.DataFrame: Transformed DataFrame ready for database insertion
        """
        try:
            self.logger.info("Starting data transformation")
            
            # Map spreadsheet columns to database columns
            column_mapping = {
                'pincode': 'pincode',
                'districtname': 'districtname', 
                'statename': 'statename',
                'tier': 'tier',
                'zones': 'zones'
            }
            
            # Rename columns to match database schema
            df_mapped = df.rename(columns=column_mapping)
            
            # Ensure all required columns exist (fill missing with None)
            for col in column_mapping.values():
                if col not in df_mapped.columns:
                    df_mapped[col] = None
                    self.logger.warning(f"Column '{col}' not found in spreadsheet, filling with None")
            
            # Select only the columns we need
            df_final = df_mapped[list(column_mapping.values())].copy()
            
            # Convert all columns to string and handle NaN values
            df_final = df_final.astype(str).replace('nan', None)
            
            self.logger.info(f"Data transformation completed. Processed {len(df_final)} rows")
            return df_final
            
        except Exception as e:
            self.logger.error(f"Error in data transformation: {e}")
            raise
    
    def load_pincode_data(self, spreadsheet_id: str, spreadsheet_range: str, target_table: str = None) -> bool:
        """
        Load pincode data from Google Spreadsheet to database.
        
        Args:
            spreadsheet_id: The ID of the Google Spreadsheet containing pincode data
            spreadsheet_range: The range to fetch from the spreadsheet
            target_table: Target table name (optional, uses TBL_PINCODE env var if not provided)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            target_table = target_table or os.getenv('TBL_PINCODE', 'pincode_data')
            
            self.logger.info(f"Starting pincode data ETL process")
            self.logger.info(f"Spreadsheet ID: {spreadsheet_id}")
            self.logger.info(f"Spreadsheet Range: {spreadsheet_range}")
            self.logger.info(f"Target table: {target_table}")
            
            # Authenticate with Google Sheets
            if not self.gsheets_client.authenticate():
                raise RuntimeError("Failed to authenticate with Google Sheets")
            
            # Fetch data from spreadsheet
            df = self.gsheets_client.fetch_sheet_data(spreadsheet_id, range_name=spreadsheet_range)
            if df is None or df.empty:
                self.logger.error("Failed to fetch data or no data available")
                return False
            
            # Transform data
            df_transformed = self.transform_pincode_data(df)
            
            # Create table if it doesn't exist
            self.db_manager.create_tables()
            
            # Insert data into database (truncate first for clean import)
            success = self.db_manager.bulk_insert_dataframe(
                df_transformed, 
                target_table, 
                if_exists='append',  # Append after truncate
                truncate_first=True  # Truncate table before inserting
            )
            
            if success:
                self.logger.info(f"Successfully loaded {len(df_transformed)} pincode records to {target_table}")
                return True
            else:
                self.logger.error("Failed to insert data into database")
                return False
                
        except Exception as e:
            self.logger.error(f"Error in pincode data ETL process: {e}")
            return False
    


def run_pincode_etl():
    """
    Main function to run the pincode ETL process.
    This function uses environment variables for configuration.
    """
    try:
        # Get configuration from environment variables
        config_dir = os.getenv('CONFIG_DIR')
        spreadsheet_id = os.getenv('SPREAD_SHEET_PIN_CODE')
        spreadsheet_range = os.getenv('SPREAD_SHEET_PIN_CODE_RANGE')
        target_table = os.getenv('TBL_PINCODE', 'pincode_data')
        
        if not config_dir:
            raise ValueError("CONFIG_DIR environment variable is required")
        if not spreadsheet_id:
            raise ValueError("SPREAD_SHEET_PIN_CODE environment variable is required")
        if not spreadsheet_range:
            raise ValueError("SPREAD_SHEET_PIN_CODE_RANGE environment variable is required")
        
        # Initialize ETL processor
        etl = PincodeETL(config_dir)
        
        # Run the ETL process
        success = etl.load_pincode_data(spreadsheet_id, spreadsheet_range, target_table)
        
        if success:
            print("Pincode ETL process completed successfully!")
            return True
        else:
            print("Pincode ETL process failed!")
            return False
            
    except Exception as e:
        print(f"Error in pincode ETL process: {e}")
        return False


if __name__ == "__main__":
    # Run the ETL process when script is executed directly
    run_pincode_etl()