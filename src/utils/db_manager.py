import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from config.config import DB_CONFIG
from config.log_config import logger_config

logger = logger_config('utils.db_manager')

# Idea ở trong class này là tạo một connection pool để quản lý kết nối DB
# Connection pool là một kỹ thuật quản lý kết nối DB hiệu quả hơn
# Sẽ làm step by step sau:
# 1. Tạo một connection pool khi khởi tạo class DBManager, 
# 2. Tạo một phương thức để lấy kết nối từ pool, nó giống như psycopg2.connect() nhưng thực chất là lấy từ pool
# 3. Tạo một phương thức để trả kết nối về pool, nó giống như conn.close() nhưng thực chất là trả về pool chứ không phải đóng kết nối

class DBManager: # Tạo class để quản lý kết nối DB

    _connection_pool = None

    @classmethod 
    # classmethod dùng để gọi hàm mà không cần tạo instance của class 
    # có nghĩa là không cần tạo db_manager = DBManager() rồi mới gọi hàm
    # Mà có thể gọi trực tiếp DBManager.init_pool()
    def init_pool(cls, minconn = 1, maxconn = 10): # cls là tham số đại diện cho class DBManager
        # minconn: số kết nối tối thiểu trong pool, có nghĩa là khi khởi tạo pool thì sẽ tạo sẵn minconn kết nối chứ không phải đợi đến khi có yêu cầu mới tạo
        # maxconn: số kết nối tối đa trong pool
        if cls._connection_pool is None: # Có nghĩa là nếu không có pool thì mới tạo pool
            cls._connection_pool = pool.SimpleConnectionPool(
                minconn,
                maxconn,
                **DB_CONFIG
            )
            logger.info("Database connection pool initialized.")
    
    @classmethod
    @contextmanager
    # contextmanager là một decorator để tạo context manager từ một hàm
    #  Có nghĩa là ta có thể sử dụng hàm này trong một khối with
    # ví dụ như sau:
    # with DBManager.get_connection() as conn:
    def get_connection(cls):
        if cls._connection_pool is None:
            cls.init_pool()
        
        conn = cls._connection_pool.getconn() # Lấy kết nối từ pool, giống như psycopg2.connect()
         # Sử dụng try...finally để đảm bảo rằng kết nối sẽ được trả về pool dù có lỗi xảy ra hay không
        try:
            yield conn 
            # yield giống như return nhưng nó sẽ trả về một generator - là một iterator có thể lặp lại được 
            # ví dụ như list, tuple, ...
        finally:
            cls._connection_pool.putconn(conn) # Trả kết nối về pool, giống như conn.close() nhưng thực chất là trả về pool chứ không phải đóng kết nối
            logger.info("Database connection returned to pool.")
    

    @classmethod
    @contextmanager
    def get_cursor(cls, commit: bool = True):
        with cls.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
                if commit:
                    conn.commit()
                    logger.info("Transaction committed.")
            except Exception as e:
                conn.rollback()
                logger.error("Transaction rolled back due to error: %s", e)
                raise
            finally:
                cursor.close()
                logger.info("Database cursor closed.")

            