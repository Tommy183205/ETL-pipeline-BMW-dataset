from anyio import Path
from flow.pipeline import ETLPipeline

# Gọi hàm tạo DB
# etl.create_db()

# Gọi hàm load data from csv to raw
# etl.load_csv_to_raw('archive/bmw.csv')


# # Gọi hàm để chuyển đổi dữ liệu từ raw sang clean
# df_clean = etl.preprocess_dataframe('archive/bmw.csv')
# print(df_clean.head())

# Gọi hàm để tạo bảng clean_bmw_sales
# etl.create_clean_table()
# Gọi hàm để load dữ liệu đã làm sạch vào bảng clean_bmw_sales
# etl.load_clean_data('archive/bmw.csv')

# Lấy cột model từ bảng clean_bmw_sales
# print(etl.get_unique_models())

if __name__ == '__main__':
    csv_path = Path(__file__).parent / 'archive' / 'bmw.csv'
    
    pipeline = ETLPipeline(str(csv_path))
    report = pipeline.run()