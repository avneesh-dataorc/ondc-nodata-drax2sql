import os
import logging
from typing import Optional, Dict, Any
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

load_dotenv()

# Create base class for ORM models
Base = declarative_base()

class DatabaseManager:
    """
    A modular database manager using SQLAlchemy ORM for handling database connections
    and operations. This class provides a clean interface for database interactions
    and can be extended for different table types.
    """
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize the database manager.
        
        Args:
            connection_string: Database connection string. If not provided,
                             will use environment variables to construct one.
        """
        self.engine = None
        self.SessionLocal = None
        self.logger = self._setup_logger()
        self.connection_string = connection_string or self._get_connection_string()
        
    def _get_connection_string(self) -> str:
        """
        Construct database connection string from environment variables.
        
        Returns:
            str: Database connection string
        """
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'postgres')
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PWD', '')
        db_schema = os.getenv('DB_SCHEMA', 'public')
        
        # Log connection details (without password for security)
        self.logger.info(f"Database connection details:")
        self.logger.info(f"  Host: {db_host}")
        self.logger.info(f"  Port: {db_port}")
        self.logger.info(f"  Database: {db_name}")
        self.logger.info(f"  User: {db_user}")
        self.logger.info(f"  Schema: {db_schema}")
        self.logger.info(f"  Password: {'***' if db_password else 'Not set'}")
        
        return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger for database operations."""
        logger = logging.getLogger(f"{__name__}.DatabaseManager")
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
    
    def connect(self) -> bool:
        """
        Establish database connection and create session factory.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.engine = create_engine(
                self.connection_string,
                echo=False,  # Set to True for SQL query logging
                pool_pre_ping=True,  # Verify connections before use
                pool_recycle=3600  # Recycle connections every hour
            )
            
            # Test the connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            # Create session factory
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )
            
            self.logger.info("Database connection established successfully")
            return True
            
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to connect to database: {e}")
            return False
    
    def create_tables(self) -> bool:
        """
        Create all tables defined in the table definitions.
        
        Returns:
            bool: True if tables created successfully, False otherwise
        """
        try:
            # Create pincode table
            pincode_table = get_pincode_table()
            pincode_table.metadata.create_all(bind=self.engine)
            
            # Create RLS Buyer NP table
            rls_buyer_table = get_rls_buyer_np_table()
            rls_buyer_table.metadata.create_all(bind=self.engine)
            
            # Create RLS Seller NP table
            rls_seller_table = get_rls_seller_np_table()
            rls_seller_table.metadata.create_all(bind=self.engine)
            
            # Create Cancellation Code table
            cancellation_code_table = get_cancellation_code_table()
            cancellation_code_table.metadata.create_all(bind=self.engine)
            
            # Create Seller NP table
            seller_np_table = get_seller_np_table()
            seller_np_table.metadata.create_all(bind=self.engine)
            
            self.logger.info("Database tables created successfully")
            return True
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to create tables: {e}")
            return False
    
    def get_session(self) -> Session:
        """
        Get a database session for ORM operations.
        
        Returns:
            Session: SQLAlchemy session object
        """
        if not self.SessionLocal:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self.SessionLocal()
    
    def execute_raw_query(self, query: str, params: Optional[Dict] = None) -> Any:
        """
        Execute a raw SQL query.
        
        Args:
            query: SQL query string
            params: Optional parameters for the query
            
        Returns:
            Query result
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return result.fetchall()
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to execute query: {e}")
            raise
    
    def truncate_table(self, table_name: str) -> bool:
        """
        Truncate a database table.
        
        Args:
            table_name: Name of the table to truncate
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Get schema from environment variable
            schema = os.getenv('DB_SCHEMA', 'public')
            full_table_name = f"{schema}.{table_name}" if schema != 'public' else table_name
            
            self.logger.info(f"Truncating table: {full_table_name}")
            
            with self.engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {full_table_name}"))
                conn.commit()
            
            self.logger.info(f"Successfully truncated table: {full_table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to truncate table {full_table_name}: {e}")
            return False
    
    def bulk_insert_dataframe(self, df, table_name: str, if_exists: str = 'append', truncate_first: bool = False) -> bool:
        """
        Bulk insert pandas DataFrame into database table.
        
        Args:
            df: Pandas DataFrame to insert
            table_name: Name of the target table
            if_exists: How to behave if table exists ('append', 'replace', 'fail')
            truncate_first: Whether to truncate the table before inserting
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Get schema from environment variable
            schema = os.getenv('DB_SCHEMA', 'public')
            full_table_name = f"{schema}.{table_name}" if schema != 'public' else table_name
            
            # Truncate table if requested
            if truncate_first:
                if not self.truncate_table(table_name):
                    self.logger.error(f"Failed to truncate table before insert")
                    return False
            
            self.logger.info(f"Inserting {len(df)} rows into {full_table_name}")
            
            df.to_sql(
                table_name,
                self.engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=1000
            )
            self.logger.info(f"Successfully inserted {len(df)} rows into {full_table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to insert DataFrame into {full_table_name}: {e}")
            return False
    
    def close(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            self.logger.info("Database connection closed")


# Global database manager instance
db_manager = None

def get_db_manager() -> DatabaseManager:
    """
    Get or create a global database manager instance.
    
    Returns:
        DatabaseManager: Global database manager instance
    """
    global db_manager
    if db_manager is None:
        db_manager = DatabaseManager()
        db_manager.connect()
    return db_manager

def get_db_session() -> Session:
    """
    Get a database session from the global database manager.
    
    Returns:
        Session: SQLAlchemy session object
    """
    return get_db_manager().get_session()


# Table definitions using SQLAlchemy Table (no primary key required)
def get_pincode_table():
    """Get Pincode table definition without primary key."""
    schema = os.getenv('DB_SCHEMA', 'public')
    table_name = os.getenv('TBL_PINCODE', 'nodata_pincode')
    
    return Table(
        table_name,
        MetaData(),
        Column('pincode', String, nullable=True),
        Column('districtname', String, nullable=True),
        Column('statename', String, nullable=True),
        Column('tier', String, nullable=True),
        Column('zones', String, nullable=True),
        schema=schema
    )


def get_rls_buyer_np_table():
    """Get RLS Buyer NP table definition without primary key."""
    schema = os.getenv('DB_SCHEMA', 'public')
    table_name = os.getenv('TBL_RLS_BUYER_NP', 'rls_buyer_np')
    
    return Table(
        table_name,
        MetaData(),
        Column('bapid', String, nullable=True),
        Column('email_address', String, nullable=True),
        Column('tsp', String, nullable=True),
        Column('update_date', String, nullable=True),
        schema=schema
    )


def get_rls_seller_np_table():
    """Get RLS Seller NP table definition without primary key."""
    schema = os.getenv('DB_SCHEMA', 'public')
    table_name = os.getenv('TBL_RLS_SELLER_NP', 'rls_seller_np')
    
    return Table(
        table_name,
        MetaData(),
        Column('bppid', String, nullable=True),
        Column('email_address', String, nullable=True),
        Column('tsp', String, nullable=True),
        Column('update_date', String, nullable=True),
        Column('tsp_secondary', String, nullable=True),
        schema=schema
    )


def get_cancellation_code_table():
    """Get Cancellation Code table definition without primary key."""
    schema = os.getenv('DB_SCHEMA', 'public')
    table_name = os.getenv('TBL_RLS_CANCEL_CODE', 'cancellation_code')
    
    return Table(
        table_name,
        MetaData(),
        Column('code', String, nullable=True),
        Column('reason_for_cancellation', String, nullable=True),
        Column('is_triggers_rto', String, nullable=True),
        Column('by', String, nullable=True),
        Column('attributed_to', String, nullable=True),
        Column('whether_applicable_for_part_cancel', String, nullable=True),
        Column('sorting', String, nullable=True),
        schema=schema
    )


def get_seller_np_table():
    """Get Seller NP table definition without primary key."""
    schema = os.getenv('DB_SCHEMA', 'public')
    table_name = os.getenv('TBL_SELLER_NP', 'seller_np')
    
    return Table(
        table_name,
        MetaData(),
        Column('seller_np_name', String, nullable=True),
        Column('seller_np', String, nullable=True),
        Column('updated_date', String, nullable=True),
        Column('snp_mask', String, nullable=True),
        Column('tsp_powered', String, nullable=True),
        Column('spoc_email', String, nullable=True),
        schema=schema
    )

