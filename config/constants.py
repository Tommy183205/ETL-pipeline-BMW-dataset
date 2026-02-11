TABLE_RAW = 'raw_bmw_sales'
TABLE_CLEAN = 'clean_bmw_sales'

REQUIRED_COLUMNS = {
    'model',
    'year', 
    'price',
    'transmission',
    'mileage',
    'fuelType',
    'tax',
    'mpg',
    'engineSize'                   
}

COLUMNS_MAPPING = {
    'fuelType' : 'fuel_type',
    'engineSize' : 'engine_size'
}

DATA_TYPES = {
    'year': 'Int64',
    'price': 'Int64',
    'mileage': 'Int64',
    'tax': 'Int64',
    'mpg': 'float',
    'engine_size': 'float'
}