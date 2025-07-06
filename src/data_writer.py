import logging
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Connection, Engine
import pandas as pd
from typing import Optional

logger = logging.getLogger(__name__)

class DataWriter:
    def __init__(self, db_connection_string: str, table_name: str):
        self.db_connection_string = db_connection_string
        self.table_name = table_name
        self.engine: Optional[Engine] = None

    def _get_engine(self) -> Engine:
        """Lazily creates and returns the SQLAlchemy engine, and tests connection."""
        if self.engine is None:
            try:
                self.engine = create_engine(self.db_connection_string)
                # Test connection and commit for SQLite to ensure connection is live
                with self.engine.connect() as connection:
                    connection.execute(text("SELECT 1"))
                    connection.commit() # Important for SQLite connection state
                logger.info("Successfully connected to the database.")
            except Exception as e:
                logger.error(f"Error connecting to the database: {e}")
                self.engine = None
                raise
        return self.engine

    def write_data(self, df: pd.DataFrame, if_exists: str = 'replace'):
        """
        Writes the DataFrame to the database.

        Args:
            df (pd.DataFrame): The DataFrame to write.
            if_exists (str): How to behave if the table already exists.
                             'fail', 'replace', or 'append'.
        """
        engine = self._get_engine()
        if engine is None:
            logger.error("Database connection not established. Cannot write data.")
            return

        try:
            logger.info(f"Attempting to write {len(df)} rows to table '{self.table_name}' with if_exists='{if_exists}'...")

            # --- Explicitly handle 'replace' strategy for robustness ---
            if if_exists == 'replace':
                with engine.connect() as connection:
                    inspector = inspect(engine)
                    # Check if the table exists before attempting to drop
                    if self.table_name in inspector.get_table_names():
                        logger.info(f"Table '{self.table_name}' exists. Dropping it explicitly.")
                        connection.execute(text(f"DROP TABLE IF EXISTS {self.table_name}"))
                        connection.commit() # Commit the drop operation to ensure it takes effect
                    else:
                        logger.info(f"Table '{self.table_name}' does not exist, proceeding with creation.")
                # After explicit drop, we'll always append (or create if not existing)
                df.to_sql(self.table_name, con=engine, if_exists='append', index=False)
            else:
                # For 'append' or 'fail', let pandas handle it directly
                df.to_sql(self.table_name, con=engine, if_exists=if_exists, index=False)

            logger.info(f"Data successfully written to table '{self.table_name}'.")

        except Exception as e:
            logger.error(f"Error writing data to database: {e}")
            raise

    def table_exists(self) -> bool:
        """
        Checks if the target table already exists in the database.
        """
        engine = self._get_engine()
        if engine is None:
            return False
        try:
            inspector = inspect(engine)
            return self.table_name in inspector.get_table_names()
        except Exception as e:
            logger.error(f"Error checking if table '{self.table_name}' exists: {e}")
            return False

    def read_data(self) -> pd.DataFrame:
        """
        Reads all data from the target table into a DataFrame.
        """
        engine = self._get_engine()
        if engine is None:
            logger.error("Database connection not established. Cannot read data.")
            return pd.DataFrame()

        try:
            logger.info(f"Attempting to read data from table '{self.table_name}'...")
            df = pd.read_sql_table(self.table_name, con=engine)
            logger.info(f"Successfully read {len(df)} rows from table '{self.table_name}'.")
            return df
        except Exception as e:
            logger.error(f"Error reading data from table '{self.table_name}': {e}")
            raise