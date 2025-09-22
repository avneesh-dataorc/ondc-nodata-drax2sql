import os
import logging
import pandas as pd
from typing import Optional, List, Dict, Any
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

from common.db_util import get_db_manager

load_dotenv()

# Google Sheets API scopes
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']


class GoogleSheetsETL:
    """
    A modular ETL class for fetching data from Google Spreadsheets and loading it into database.
    This class handles authentication, data fetching, and database operations in a clean, reusable way.
    """
    
    def __init__(self, config_dir: Optional[str] = None):
        """
        Initialize the Google Sheets ETL processor.
        
        Args:
            config_dir: Directory containing credentials.json and token.json files.
                       If not provided, uses CONFIG_DIR environment variable.
        """
        self.config_dir = config_dir or os.getenv('CONFIG_DIR')
        if not self.config_dir:
            raise ValueError("Config directory must be provided either as parameter or CONFIG_DIR env variable")
        
        self.credentials_path = os.path.join(self.config_dir, 'credentials.json')
        self.token_path = os.path.join(self.config_dir, 'token.json')
        
        self.service = None
        self.db_manager = get_db_manager()
        self.logger = self._setup_logger()
        
        # Validate files exist
        self._validate_config_files()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger for ETL operations."""
        logger = logging.getLogger(f"{__name__}.GoogleSheetsETL")
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
    
    def _validate_config_files(self):
        """Validate that required configuration files exist."""
        if not os.path.exists(self.credentials_path):
            raise FileNotFoundError(f"Credentials file not found: {self.credentials_path}")
        
        self.logger.info(f"Using config directory: {self.config_dir}")
        self.logger.info(f"Credentials file: {self.credentials_path}")
        self.logger.info(f"Token file: {self.token_path}")
    
    def authenticate(self) -> bool:
        """
        Authenticate with Google Sheets API using OAuth2.
        
        Returns:
            bool: True if authentication successful, False otherwise
        """
        try:
            creds = None
            
            # Load existing token if available
            if os.path.exists(self.token_path):
                creds = Credentials.from_authorized_user_file(self.token_path, SCOPES)
                self.logger.info("Loaded existing credentials from token file")
            
            # If there are no (valid) credentials available, let the user log in
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                    self.logger.info("Refreshed expired credentials")
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.credentials_path, SCOPES)
                    creds = flow.run_local_server(port=0)
                    self.logger.info("Completed OAuth2 flow for new credentials")
                
                # Save the credentials for the next run
                with open(self.token_path, 'w') as token:
                    token.write(creds.to_json())
                self.logger.info("Saved credentials to token file")
            
            # Build the service
            self.service = build('sheets', 'v4', credentials=creds)
            self.logger.info("Google Sheets API service initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Authentication failed: {e}")
            return False
    
    def fetch_sheet_data(self, spreadsheet_id: str, sheet_name: str = None, 
                        range_name: str = None) -> Optional[pd.DataFrame]:
        """
        Fetch data from a Google Spreadsheet.
        
        Args:
            spreadsheet_id: The ID of the Google Spreadsheet
            sheet_name: Name of the sheet (optional, defaults to first sheet)
            range_name: Specific range to fetch (optional, defaults to all data)
            
        Returns:
            pd.DataFrame: Fetched data as pandas DataFrame, or None if failed
        """
        try:
            if not self.service:
                raise RuntimeError("Google Sheets service not initialized. Call authenticate() first.")
            
            # Construct the range
            if range_name:
                range_to_fetch = range_name
            elif sheet_name:
                range_to_fetch = f"{sheet_name}!A:Z"  # Fetch all columns
            else:
                range_to_fetch = "A:Z"  # Default to first sheet
            
            self.logger.info(f"Fetching data from spreadsheet {spreadsheet_id}, range: {range_to_fetch}")
            
            # Call the Sheets API
            sheet = self.service.spreadsheets()
            result = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=range_to_fetch
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                self.logger.warning("No data found in the specified range")
                return pd.DataFrame()
            
            # Convert to DataFrame
            # First row as column headers
            headers = values[0] if values else []
            data_rows = values[1:] if len(values) > 1 else []
            
            # Create DataFrame
            df = pd.DataFrame(data_rows, columns=headers)
            
            # Clean column names (remove spaces, special characters)
            df.columns = df.columns.str.strip().str.lower().str.replace(' ', '').str.replace('[^a-zA-Z0-9_]', '_', regex=True)
            
            self.logger.info(f"Successfully fetched {len(df)} rows with {len(df.columns)} columns")
            return df
            
        except HttpError as e:
            self.logger.error(f"Google Sheets API error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error fetching sheet data: {e}")
            return None
    
    def load_pincode_data(self, spreadsheet_id: str, target_table: str = None) -> bool:
        """
        Load pincode data from Google Spreadsheet to database.
        
        Args:
            spreadsheet_id: The ID of the Google Spreadsheet containing pincode data
            target_table: Target table name (optional, uses TBL_PINCODE env var if not provided)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            target_table = target_table or os.getenv('TBL_PINCODE', 'pincode_data')
            
            self.logger.info(f"Starting pincode data ETL process")
            self.logger.info(f"Spreadsheet ID: {spreadsheet_id}")
            self.logger.info(f"Target table: {target_table}")
            
            # Fetch data from spreadsheet
            df = self.fetch_sheet_data(spreadsheet_id)
            if df is None or df.empty:
                self.logger.error("Failed to fetch data or no data available")
                return False
            
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
            
            # Select only the columns we need
            df_final = df_mapped[list(column_mapping.values())].copy()
            
            # Convert all columns to string and handle NaN values
            df_final = df_final.astype(str).replace('nan', None)
            
            self.logger.info(f"Processed {len(df_final)} rows for database insertion")
            
            # Create table if it doesn't exist
            self.db_manager.create_tables()
            
            # Insert data into database (truncate first for clean import)
            success = self.db_manager.bulk_insert_dataframe(
                df_final, 
                target_table, 
                if_exists='append',  # Append after truncate
                truncate_first=True  # Truncate table before inserting
            )
            
            if success:
                self.logger.info(f"Successfully loaded {len(df_final)} pincode records to {target_table}")
                return True
            else:
                self.logger.error("Failed to insert data into database")
                return False
                
        except Exception as e:
            self.logger.error(f"Error in pincode data ETL process: {e}")
            return False
    
    def load_generic_spreadsheet_data(self, spreadsheet_id: str, target_table: str, 
                                    column_mapping: Dict[str, str] = None, truncate_first: bool = True) -> bool:
        """
        Load generic spreadsheet data to database with custom column mapping.
        
        Args:
            spreadsheet_id: The ID of the Google Spreadsheet
            target_table: Target table name
            column_mapping: Dictionary mapping spreadsheet columns to database columns
            truncate_first: Whether to truncate the table before inserting data
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.logger.info(f"Starting generic spreadsheet data ETL process")
            self.logger.info(f"Spreadsheet ID: {spreadsheet_id}")
            self.logger.info(f"Target table: {target_table}")
            
            # Fetch data from spreadsheet
            df = self.fetch_sheet_data(spreadsheet_id)
            if df is None or df.empty:
                self.logger.error("Failed to fetch data or no data available")
                return False
            
            # Apply column mapping if provided
            if column_mapping:
                df = df.rename(columns=column_mapping)
                self.logger.info(f"Applied column mapping: {column_mapping}")
            
            # Convert all columns to string and handle NaN values
            df = df.astype(str).replace('nan', None)
            
            self.logger.info(f"Processed {len(df)} rows for database insertion")
            
            # Insert data into database
            success = self.db_manager.bulk_insert_dataframe(
                df, 
                target_table, 
                if_exists='append',
                truncate_first=truncate_first
            )
            
            if success:
                self.logger.info(f"Successfully loaded {len(df)} records to {target_table}")
                return True
            else:
                self.logger.error("Failed to insert data into database")
                return False
                
        except Exception as e:
            self.logger.error(f"Error in generic spreadsheet ETL process: {e}")
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
        target_table = os.getenv('TBL_PINCODE', 'pincode_data')
        
        if not config_dir:
            raise ValueError("CONFIG_DIR environment variable is required")
        if not spreadsheet_id:
            raise ValueError("SPREAD_SHEET_PIN_CODE environment variable is required")
        
        # Initialize ETL processor
        etl = GoogleSheetsETL(config_dir)
        
        # Authenticate with Google Sheets
        if not etl.authenticate():
            raise RuntimeError("Failed to authenticate with Google Sheets")
        
        # Run the ETL process
        success = etl.load_pincode_data(spreadsheet_id, target_table)
        
        if success:
            print("✅ Pincode ETL process completed successfully!")
            return True
        else:
            print("❌ Pincode ETL process failed!")
            return False
            
    except Exception as e:
        print(f"❌ Error in pincode ETL process: {e}")
        return False


if __name__ == "__main__":
    # Run the ETL process when script is executed directly
    run_pincode_etl()
