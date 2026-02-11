import pandas as pd
from config.log_config import logger_config
from config.validate import check_validate_dataframe, cal_hash_file, check_data_exist, check_validate_csv
from config.constants import REQUIRED_COLUMNS, COLUMNS_MAPPING, DATA_TYPES

logger = logger_config('src.transform.cleaner')


class DataCleaner:
    #  Phương thức tĩnh để làm sạch dữ liệu trong DataFrame
    @staticmethod
    def clean_data(df: pd.DataFrame) -> pd.DataFrame:
        # Step by step các bước làm sạch dữ liệu:
        # Đổi tên cột theo COLUMNS_MAPPING
        # Đổi kiểu dữ liệu theo DATA_TYPES
        # Xử lý giá trị thiếu
        # Filter theo business rule
        try:
            logger.info("Starting data cleaning process.")

            df = df.copy() # Tạo bản sao của DataFrame để tránh thay đổi dữ liệu gốc

            # 1. Rename columns
            df = DataCleaner._rename_columns(df)
            
            # 2. Convert data types
            df = DataCleaner._convert_types(df)
            
            # 3. Clean text columns
            df = DataCleaner._clean_text_columns(df)
            
            # 4. Handle nulls
            df = DataCleaner._handle_nulls(df)
            
            # 5. Apply business rules
            df = DataCleaner._apply_business_rules(df)
            logger.info("Data cleaning process completed successfully.")
            return df
        except Exception as e:
            logger.exception(f"An error occurred during data cleaning: {e}")
            raise
    

    @staticmethod
    def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
        # Rename columns based on COLUMNS_MAPPING
        df = df.rename(columns= COLUMNS_MAPPING)
        logger.info("Columns renamed according to mapping.")
        return df
    
    @staticmethod
    def _convert_types(df: pd.DataFrame) -> pd.DataFrame:
        # Convert columns to specified data types
        for col, dtype in DATA_TYPES.items():
            if col in df.columns:
                if dtype in ['Int64', 'int']:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                elif dtype  == 'float':
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        return df
    
    @staticmethod
    def _clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
        text_cols = ['model', 'transmission', 'fuel_type']
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].str.strip().str.lower()
        return df
    
    @staticmethod
    def _handle_nulls(df: pd.DataFrame) -> pd.DataFrame:

        # Drop rows with nulls in critical columns FIRST
        critical_cols = ['model', 'year', 'price']  # ✅ Dùng tên sau khi rename
        df = df.dropna(subset=critical_cols)
    

        # Fill null for numerical columns with median
        num_cols = ['mileage', 'tax', 'mpg', 'engine_size']
        for col in num_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0)
                logger.info(f"Filled null values in {col} with 0.")
        # Drop rows with nulls in required columns
        logger.info(f"Handled null values. Remaining records: {len(df)}.")
        return df
    
    @staticmethod
    def _apply_business_rules(df: pd.DataFrame) -> pd.DataFrame:
        # Filter out records with price <= 0
        df = df[df['price'] > 0]
        df = df[df['year'] >= 2014]
        df = df[df['mileage'] >= 0]
        logger.info(f"Applied business rules. Remaining records after filtering: {len(df)}.")
        return df