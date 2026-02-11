from config.log_config import logger_config
from src.extract.csv_extractor import ExtractorCSV
from src.transform.cleaner import DataCleaner
from src.load.db_loader import DBLoader
from src.utils.data_profiler import DataProfiler

logger = logger_config('flow.pipeline')

class ETLPipeline:
    """Main ETL Pipeline orchestrator"""
    
    # Tạo hàm __init__ để khởi tạo pipeline với đường dẫn csv 
    # Khi gọi tới class ETLPipeline thì sẽ phải truyền vào đường dẫn csv để pipeline biết được nguồn dữ liệu ở đâu
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
    
    def run(self):
        """Execute full ETL pipeline"""
        try:
            logger.info("=" * 60)
            logger.info("🚀 Starting ETL Pipeline")
            logger.info("=" * 60)
            
            # Step 1: Setup tables
            logger.info("Step 1: Creating tables...")
            DBLoader.create_raw_and_clean_table()
            logger.info("✅ Tables ready")
            
            # Step 2: Extract
            logger.info("Step 2: Extracting data...")
            df_raw = ExtractorCSV.extract(self.csv_path)
            logger.info(f"✅ Extracted {len(df_raw)} rows")
            
            # Step 3: Load raw
            logger.info("Step 3: Loading raw data...")
            DBLoader.load_to_raw_table(df_raw, self.csv_path, skip_if_exist=True)
            logger.info("✅ Raw data loaded")
            logger.info("=="*60)

            # Step 4: Transform
            logger.info("Step 4: Transforming data...")
            df_clean = DataCleaner.clean_data(df_raw)
            logger.info(f"✅ Cleaned to {len(df_clean)} rows")
            logger.info("=="*60)
            
            # Step 5: Load clean
            logger.info("Step 5: Loading clean data...")
            DBLoader.load_to_clean_table(df_clean, self.csv_path, skip_if_exist=True)
            logger.info("✅ Clean data loaded")
            logger.info("=="*60)
            
            # Step 6: Generate report
            logger.info("Step 6: Generating quality report...")
            report = DataProfiler.generated_quantity_report()
            logger.info("=" * 60)

            logger.info("✅ ETL Pipeline Completed Successfully!")
            logger.info("=" * 60)
            logger.info("📈 Quality Report:")
            logger.info(f"  Raw Records: {report['raw_record']}")
            logger.info(f"  Clean Records: {report['clean_record']}")
            logger.info(f"  Records Dropped: {report['record_dropped']} ({report['drop_rate']})")
            logger.info(f"  Drop Rate: {report['drop_rate']:.2f}%")
            logger.info(f"  Unique Models: {report['unique_model']}")
            logger.info(f"  Price Range: ${report['price_stat']['min']:,} - ${report['price_stat']['max']:,}")
            logger.info("=" * 60)
            
            return report
            
        except Exception as e:
            logger.exception(f"❌ ETL Pipeline failed: {e}")
            raise