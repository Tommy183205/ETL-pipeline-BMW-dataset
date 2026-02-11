from config.log_config import logger_config
from config.constants import TABLE_RAW, TABLE_CLEAN
from src.utils.db_manager import DBManager

logger = logger_config('utils.data_profiler')

# Ở phân đoạn này, cần làm report để báo cáo chất lượng của report

class DataProfiler:

    @staticmethod
    def generated_quantity_report()->dict:

        try:
            with DBManager.get_cursor() as cur:

                # count all rows in table raw and clean
                cur.execute(f"SELECT COUNT(*) FROM {TABLE_RAW};")
                raw_count = cur.fetchone()[0]

                cur.execute(f"SELECT COUNT(*) FROM {TABLE_CLEAN}")
                clear_count = cur.fetchone()[0]

                 # Unique models
                cur.execute(f"SELECT COUNT(DISTINCT model) FROM {TABLE_CLEAN};")
                unique_models = cur.fetchone()[0]

                # Price statistics
                cur.execute(f"""
                    SELECT 
                        MIN(price) as min_price,
                        MAX(price) as max_price,
                        AVG(price) as avg_price,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price
                    FROM {TABLE_CLEAN};
                """)
                price_stats = cur.fetchone()
                
                # Year distribution
                cur.execute(f"""
                    SELECT 
                        MIN(year) as oldest_year,
                        MAX(year) as newest_year
                    FROM {TABLE_CLEAN};
                """)
                year_stats = cur.fetchone()
                
                # Calculate metrics
                records_dropped = raw_count - clear_count
                drop_rate = 0
                if raw_count > 0:
                    drop_rate = (records_dropped / raw_count * 100)
                


                report = {
                    'raw_record' : raw_count,
                    'clean_record' : clear_count,
                    'record_dropped' : records_dropped,
                    'drop_rate' : drop_rate,
                    'unique_model' : unique_models,
                    'price_stat' : {
                        'min' : price_stats[0],
                        'max' : price_stats[1],
                        'avg' : round(price_stats[2], 2) if price_stats[2] else 0, # round ở đây là làm tròn 2 chữ số thập phân, còn nếu price_stats[2] là None thì sẽ trả về 0
                        'median' : price_stats[3]
                    }
                }
                logger.info("Data profiling report generated successfully")
                return report
        except Exception as e:
            logger.exception(f"Failed to generate data profiling report: {e}")
            raise