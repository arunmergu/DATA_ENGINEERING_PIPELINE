
# src/config.py
import os

class Config:
    # Database connection string, defaulting to a local SQLite path if not set
    # In CI/CD, this will be overridden by GitHub Secrets
    DB_CONNECTION_STRING = os.getenv('DB_URL', 'sqlite:///./data/bynd_pipeline.db')
    DB_TABLE_NAME = os.getenv('DB_TABLE_NAME', 'customers_and_transactions')

    # Path to your input data file within the container
    # This path is relative to the WORKDIR in Dockerfile or mounted volume
    SOURCE_DATA_FILE = './data/mock_dataset.csv'

    # You can add other configurations here as needed
    # For example, if you had a different environment for development vs. production:
    # ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')