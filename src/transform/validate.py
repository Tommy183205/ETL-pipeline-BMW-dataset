import pandas as pd
from config.log_config import logger_config
import psycopg2 as ps
import hashlib as hl
from config.config import DB_CONFIG
from pandas.api.types import is_integer_dtype, is_float_dtype, is_string_dtype
from pathlib import Path

# Đây là nơi sẽ chứa các hàm để validate dữ liệu, nó sẽ khác với cleaner ở chỗ cleaner là để làm sạch dữ liệu
# Còn validate là để kiểm tra dữ liệu đã sạch hay chưa, có hợp lệ để load vào DB hay không


logger = logger_config('flow.etl')


# Tạo hàm check validate của file csv đối với code ETL trước khi load vào DB
def check_validate_csv(df:pd.DataFrame, requirements_column: list) -> bool:
    # Check file csv xem có đủ cột cần thiết không ?

    df_columns = set(df.columns.str.strip().str.lower())
    required_set = set( col.lower() for col in requirements_column)
    
    missing_column = required_set - df_columns

    if missing_column:
        logger.error(f"Missing required column: {missing_column}")
        logger.error(f"Available column are: {df_columns}")
        return False

    return True

# Tạo hàm check validate của dataframe trước khi load vào bảng clean_bmw_sales
def check_validate_dataframe(df:pd.DataFrame) -> bool:
    try:
        # Check schema sau khi rename
        required_column = {'model',
                           'year',
                           'price',
                           'transmission',
                           'mileage',
                           'fuel_type',
                           'tax',
                           'mpg',
                           'engine_size'}
        missing_column = required_column - set(df.columns)
        if missing_column:
            logger.error(f"Missing required column: {missing_column}")
            return False

        # Check null cho các cột
        for col in ['model',
                    'year',
                    'price',
                    'transmission',
                    'mileage',
                    'fuel_type',
                    'tax',
                    'mpg',
                    'engine_size']:
            count_null = df[col].isnull().sum() # Dòng này để đếm số giá trị null trong từng cột
            if count_null > 0:
                logger.error(f"Column {col} has {count_null} null values.")
                return False
        
        # Check data type
        if not is_integer_dtype(df['year']):
            logger.error("Column 'year' is not of integer type.")
            return False
        if not is_integer_dtype(df['price']):
            logger.error("Column 'price' is not of integer type.")
            return False
        if not is_integer_dtype(df['mileage']):
            logger.error("Column 'mileage' is not of integer type.")
            return False
        if not is_integer_dtype(df['tax']):
            logger.error("Column 'tax' is not of integer type.")
            return False
        if not is_float_dtype(df['mpg']):
            logger.error("Column 'mpg' is not of float type.")
            return False
        if not is_float_dtype(df['engine_size']):
            logger.error("Column 'engine_size' is not of float type.")
            return False
        
        # Check validate data ranges
        if(df['year'] < 1990).any():
            logger.error("Column 'year' has values less than 1990.")
            return False
        if(df['price'] < 0).any():
            logger.error("Column 'price' has negative values.")
            return False    
        if(df['mileage'] < 0).any():
            logger.error("Column 'mileage' has negative values.")
            return False
        

        #  Check empty df
        if (len(df) == 0):
            logger.error("DataFrame is empty after validation.")
            return False
        
        logger.info("DataFrame passed all validation checks.")
        return True

    except Exception as e:
        logger.exception(f"An error occurred during DataFrame validation: {e}")
        return False
    
# Tạo hàm tính hash của file csv để so sánh xem file với những record đã load vào DB chưa
# Và cũng để check xem các record với giá trị có bị load trùng lặp vào DB không
def cal_hash_file(file_path: str) -> str:
    # Tính MD5 hash của file csv, MD5 là một hàm băm phổ biến được sử dụng để kiểm tra tính toàn vẹn của dữ liệu.
    # Hàm đó được sử dụng để tạo ra một chuỗi ký tự duy nhất đại diện cho nội dung của file.
    # Và nếu nội dung của file thay đổi, thì giá trị hash cũng sẽ thay đổi.
    # Khi đó thì ta có thể sử dụng giá trị hash này để kiểm tra xem file cùng nội dung đã được load vào DB chưa.
    try:
        hash_md5 = hl.md5()
        # Mở file ở chế độ đọc nhị phân 
        # sau đó đọc từng khối dữ liệu và cập nhật giá trị hash 
        # rồi cuối cùng trả về giá trị hash dưới dạng chuỗi hex
        # Dùng with để tự động đóng file sau khi đọc xong

        with open(file_path, "rb") as f: # rb là read binary
            for chunk in iter(lambda: f.read(4096), b""): 
                # chunk là từng phần nhỏ của file, 
                # iter đọc từng phần 4096 byte, b"" là giá trị kết thúc
                # 4096 byte là kích thước khối phổ biến để đọc file lớn
                # Dùng lambda để tạo hàm không tên để đọc file
                hash_md5.update(chunk) # Cập nhật giá trị hash với khối dữ liệu hiện tại
        return hash_md5.hexdigest() # Trả về giá trị hash dưới dạng chuỗi hex
    except Exception as e:
        logger.error(f"An error occurred while calculating file hash: {e}")
        return ""

def check_data_exist (file_path: str, table: str) -> bool:
    # Kiểm tra xem file csv này đã được load vào DB chưa
    try:
        conn = ps.connect(**DB_CONFIG)
        if conn is None:
            logger.error("Failed to connect to the database.")
            return False
        cur = conn.cursor()
     
        # Lấy hash cũ của file csv
        cur.execute(f"""
            SELECT COUNT(*) 
            FROM {table}
            WHERE src_file = %s
            LIMIT 1;
        """, (file_path,))

        result = cur.fetchone()

        if result and result[0]:
            return(True, result[0]) # result[0] là số lượng record có hash trùng với file csv
        else:
            return (False, "") # Không có record nào có hash trùng với file csv
        
    except Exception as e:
        logger.exception(f"An error occurred while checking data existence: {e}")
        return False
    finally:
        conn.close()
        cur.close()


