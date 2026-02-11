import pandas as pd
from pathlib import Path
from config.log_config import logger_config
from config.constants import TABLE_RAW, TABLE_CLEAN, COLUMNS_MAPPING, DATA_TYPES, REQUIRED_COLUMNS
from config.validate import cal_hash_file, check_data_exist, check_validate_csv, check_validate_dataframe
from src.utils.db_manager import DBManager


logger = logger_config('src.load.db_loader')

class DBLoader:

    @staticmethod
    def create_raw_and_clean_table():
        with DBManager.get_cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_RAW}(
                model TEXT,
                year INT,
                price INT,
                transmission TEXT,
                mileage INT,
                fuel_type TEXT,
                tax INT,
                mpg FLOAT,
                engine_size FLOAT,
                src_file TEXT,
                file_hash TEXT,
                ingest_at TIMESTAMP DEFAULT NOW());
            """)

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_CLEAN}(
                    model TEXT NOT NULL,
                    year INT NOT NULL,
                    price INT NOT NULL,
                    transmission TEXT NOT NULL,
                    mileage INT NOT NULL,
                    fuel_type TEXT NOT NULL,
                    tax INT NOT NULL,
                    mpg FLOAT NOT NULL,
                    engine_size FLOAT NOT NULL,
                    src_file TEXT NOT NULL,
                    ingest_at TIMESTAMP DEFAULT NOW());
            """)
            logger.info("Tables created successfully")

    @staticmethod
    def delete_existing(csv_path: str, table_name:str):
        if table_name not in [TABLE_CLEAN, TABLE_RAW]:
            raise ValueError(f"Invalid table name: {table_name}")
        with DBManager.get_cursor() as cur:
            cur.execute(f"""
                DELETE FROM {table_name}
                WHERE src_file = %s
            """, (csv_path,))
        logger.info(f"Deleted existing data from {table_name}")        

    @staticmethod
    def load_to_raw_table(df:pd.DataFrame, csv_path:str, skip_if_exist:bool = True):
        try:
            current_hash = cal_hash_file(csv_path)
            existed, old_hash = check_data_exist(csv_path, TABLE_RAW)
            if existed and skip_if_exist:
                if old_hash == current_hash:
                    logger.info(f"Data from {csv_path} already exists. Skipping.")
                    return
                
                DBLoader.delete_existing(csv_path, TABLE_RAW)
            logger.info(f"Loading {len(df)} rows to {TABLE_RAW}")

            # Sau khi xử lý các bước check hash rồi thì giờ insert vô thâu
            data_to_insert = []
            for _, row in df.iterrows():
                data_to_insert.append((
                    str(row.get('model', '')).strip(),
                    row.get('year'),
                    row.get('price'),
                    str(row.get('transmission', '')).strip(),
                    row.get('mileage'),
                    str(row.get('fuelType', '')).strip(),
                    row.get('tax'),
                    row.get('mpg'),
                    row.get('engineSize'),
                    csv_path,
                    current_hash
                ))
            with DBManager.get_cursor() as cur:
                cur.executemany(f"""
                    INSERT INTO {TABLE_RAW} (model, year, price, transmission, mileage, fuel_type, 
                        tax, mpg, engine_size, src_file, file_hash)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, data_to_insert)
            logger.info(f"Successfully loaded raw data")

        except Exception as e:
            logger.exception(f"Load raw failed: {e}")
            raise
    
    @staticmethod
    def load_to_clean_table(df: pd.DataFrame, csv_file:str, skip_if_exist:bool=True):
        try:
            current_hash = cal_hash_file(csv_file)
            existed, old_hash = check_data_exist(csv_file, TABLE_CLEAN)
            
            if existed and skip_if_exist:
                if current_hash == old_hash:
                    logger.info(f"Cleaned data from {csv_file} already exists. Skipping.")
                    return
                DBLoader.delete_existing(csv_file, TABLE_CLEAN)
            
            data_to_load = []
            for _, row in df.iterrows():
                data_to_load.append((
                    row['model'],
                    int(row['year']),
                    int(row['price']),
                    row['transmission'],
                    int(row['mileage']),
                    row['fuel_type'],
                    int(row['tax']),
                    float(row['mpg']),
                    float(row['engine_size']),
                    csv_file
                ))
            
            with DBManager.get_cursor() as cur:
                cur.executemany(f"""
                    INSERT INTO {TABLE_CLEAN}
                    (model, year, price, transmission, mileage, fuel_type, tax, mpg, engine_size, src_file)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, data_to_load)
                logger.info(f"Successfully loaded clean data")

        except Exception as e:
            logger.exception(f"Load clean failed: {e}")
            raise
    

