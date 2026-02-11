import pandas as pd
from pathlib import Path
from config.log_config import logger_config
from src.transform.validate import cal_hash_file, check_data_exist, check_validate_csv, check_validate_dataframe
from config.constants import TABLE_RAW, TABLE_CLEAN, DATA_TYPES, REQUIRED_COLUMNS, COLUMNS_MAPPING

logger = logger_config('src.extract.csv_extractor')

# Đây là nơi sẽ chứa các hàm để trích xuất dữ liệu từ file CSV
# Từ file CSV, ta sẽ đọc dữ liệu vào DataFrame của pandas
class ExtractorCSV:

    @staticmethod
    # Đây là phương thức tĩnh, không cần tham số self hoặc cls
    # Có nghĩa là ta có thể gọi trực tiếp phương thức này từ class mà không cần tạo instance của class
    # Ví dụ: ExtractorCSV.load_csv('path/to/csv')
    def extract(csv_path:str) -> pd.DataFrame:
        try:
            file_path = Path(csv_path)
            if not file_path.exists():
                logger.error(f"File not found: {csv_path}")
                raise FileNotFoundError(f"File not found: {csv_path}")
            logger.info(f"Loading CSV file from: {csv_path}")
            df = pd.read_csv(csv_path)
            logger.info(f"CSV file loaded successfully with {len(df)} records.")

            # Clean Column Names
            df.columns = df.columns.str.strip()

            # Validate schema
            if not check_validate_csv(df, REQUIRED_COLUMNS):
                logger.error("CSV validation failed. Missing required columns.")
                raise ValueError("CSV validation failed. Missing required columns.")
            logger.info("CSV validation passed.")
            return df
        except Exception as e:
            logger.exception(f"An error occurred while extracting CSV: {e}")
            raise
