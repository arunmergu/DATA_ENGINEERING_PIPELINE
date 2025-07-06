from src.data_reader import CSVReader
from src.transformer import DataTransformer
from src.data_writer import DataWriter
from src.config import Config
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_pipeline():
    """
    Executes the data engineering pipeline: read, transform, and load.
    
    """
    config = Config()
    # Adjust path for execution from project root
    source_filepath = os.path.join(os.path.dirname(__file__), '..', config.SOURCE_DATA_FILE)

    print("Starting data pipeline...")

    # 1. Read Data
    reader = CSVReader(source_filepath)
    data_df = reader.read_data()
    logger.info(f"Columns in raw DataFrame after reading CSV: {data_df.columns.tolist()}")


    if data_df is None:
        print("Pipeline aborted due to data reading failure.")
        return

    # 2. Transform Data
    transformer = DataTransformer()
    transformed_df = transformer.transform_data(data_df) 
    logger.info(f"Columns in transformed_df before writing: {transformed_df.columns.tolist()}")


    if transformed_df is None:
        print("Pipeline aborted due to data transformation failure.")
        return

    # 3. Sink Data
    writer = DataWriter(config.DB_CONNECTION_STRING, config.DB_TABLE_NAME) # CHANGE 'DatabaseWriter' to 'DataWriter' and add table_name
    writer.write_data(transformed_df, if_exists='replace') # REMOVE config.DB_TABLE_NAME from here

    print("Data pipeline finished.")

if __name__ == "__main__":
    run_pipeline()