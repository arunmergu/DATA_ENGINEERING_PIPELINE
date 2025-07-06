import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """
    Centralized configuration management for the data pipeline.
    Loads configuration from .env file, then falls back to system environment variables.
    """
    # Database connection string for generic use (e.g., SQLite file path, or full URL for any DB)
    # This will be used if set in .env or environment variables.
    DB_URL = os.getenv("DB_URL")

    # PostgreSQL specific connection details (only used if DB_URL is NOT set)
    # These will be ignored if DB_URL is provided in .env or environment
    DB_USER = os.getenv("DB_USER", "user")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "bynd_db") # Default database name for PostgreSQL

    # Table name for sinking data
    DB_TABLE_NAME = os.getenv("DB_TABLE_NAME", "customers_and_transactions")

    # Source data file path, relative to the project root
    SOURCE_DATA_FILE = os.getenv("SOURCE_DATA_FILE", "data/mock_dataset.csv")

    @property
    def DB_CONNECTION_STRING(self) -> str:
        """
        Constructs the database connection string.
        Prioritizes DB_URL (for SQLite or other direct URLs) if set.
        Otherwise, it constructs a PostgreSQL string using individual components.
        """
        if self.DB_URL:
            return self.DB_URL
        else:
            # Fallback to PostgreSQL specific construction if DB_URL is not provided
            return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    def __str__(self):
        """
        Provides a string representation of the config,
        masking sensitive information.
        """
        # Determine what to display for the connection string
        db_string_display = ""
        if self.DB_URL:
            db_string_display = f"DB_URL={self.DB_URL}"
        else:
            db_string_display = (
                f"DB_HOST={self.DB_HOST}, "
                f"DB_PORT={self.DB_PORT}, "
                f"DB_NAME={self.DB_NAME}, "
                f"DB_USER={self.DB_USER}, "
                f"DB_PASSWORD={'*****' if self.DB_PASSWORD else 'None'}"
            )

        return (
            f"Config(\n"
            f"  {db_string_display},\n"
            f"  DB_TABLE_NAME={self.DB_TABLE_NAME},\n"
            f"  SOURCE_DATA_FILE={self.SOURCE_DATA_FILE}\n"
            f")"
        )