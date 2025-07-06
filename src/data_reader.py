import pandas as pd
import logging

logger = logging.getLogger(__name__)

class CSVReader:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def read_data(self) -> pd.DataFrame:
        try:
            logger.info(f"Attempting to read data from {self.file_path}")
            df = pd.read_csv(self.file_path)
            
            # --- START COLUMN CLEANING LOGIC ---
            # Clean column names: strip whitespace, lowercase, replace spaces with underscores
            original_columns = df.columns.tolist()
            new_columns = []
            seen_columns = set() # To handle potential duplicate names after cleaning

            for col in original_columns:
                cleaned_col = col.strip().lower().replace(' ', '_')
                
                # If a cleaned column name already exists (e.g., from 'account Created at' and 'account created at'),
                # append a suffix to make it unique temporarily.
                # The transformer will then pick the correct one.
                if cleaned_col in seen_columns:
                    suffix = 1
                    while f"{cleaned_col}_{suffix}" in seen_columns:
                        suffix += 1
                    cleaned_col = f"{cleaned_col}_{suffix}"
                seen_columns.add(cleaned_col)
                new_columns.append(cleaned_col)
            
            df.columns = new_columns
            logger.info(f"Cleaned DataFrame columns: {df.columns.tolist()}")
            # --- END COLUMN CLEANING LOGIC ---

            return df
        except FileNotFoundError:
            logger.error(f"Error: File not found at {self.file_path}")
            raise
        except pd.errors.EmptyDataError:
            logger.error(f"Error: No data in file at {self.file_path}")
            raise
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise