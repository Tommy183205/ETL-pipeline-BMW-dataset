import pandas as pd
import numpy as np

df = pd.read_csv('archive/bmw.csv')



print(df.head()) # -> in ra 5 dòng đầu tiên 
print(df.tail()) # -> in ra 5 dòng cuối

print(df.info()) # -> in ra kiểu dữ liệu + null
print(df.describe()) # -> in ra thống kê, mô tả


print(df.shape) # -> số dòng, số cột
print(df.columns)# -> in ra các cột
print(df.dtypes) # -> in ra kiểu dữ liệu cột