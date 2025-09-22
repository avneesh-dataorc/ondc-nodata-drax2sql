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

load_dotenv()

# Google Sheets API scopes
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']


class GoogleSheetsClient:
    """
    A utility class for Google Sheets operations.
    Handles authentication and data fetching from Google Spreadsheets.
    """
    
    def __init__(self, config_dir: Optional[str] = None):
        """
        Initialize the Google Sheets client.
        
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
        self.logger = self._setup_logger()
        
        # Validate files exist
        self._validate_config_files()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger for Google Sheets operations."""
        logger = logging.getLogger(f"{__name__}.GoogleSheetsClient")
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