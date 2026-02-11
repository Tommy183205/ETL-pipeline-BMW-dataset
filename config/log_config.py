import logging as log
import os

def logger_config(name_file:str) -> log.Logger: # name_file là tên file log muốn tạo chứ không phải file data
    os.makedirs('logs', exist_ok=True) # Tạo thư mục logs nếu chưa tồn tại

    logger = log.getLogger(name_file) # Này là để tạo logger với tên file
    logger.setLevel(log.DEBUG) # Set mức độ log là DEBUG để ghi lại tất cả các mức độ log

    # Viết điều kiện để tránh việc thêm nhiều handler giống nhau khi gọi hàm nhiều lần
    if logger.handlers: 
        return logger # Nếu đã có handler rồi thì trả về luôn
    
    # Tạo handler để ghi log vào file có nghĩa là khi dùng nó thì log sẽ được ghi vào file kiểu logs/name_file.log
    file_handler = log.FileHandler(f'logs/{name_file}.log', mode='a', encoding='utf-8') # mode 'a' là append, 'w' là ghi đè
    file_handler.setLevel(log.ERROR) # Set mức độ log cho file handler là ERROR

    # Tạo handler để in log ra console chỉ khi có mức độ INFO trở lên
    console_handler = log.StreamHandler() # Hàm này để in log ra console
    console_handler.setLevel(log.INFO) # Set mức độ log cho console handler là INFO
    
    # Tạo formatter để định dạng log
    formatter = log.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Thêm các handler vào logger là để logger biết có những handler nào để xử lý log, tức lúc này file_handler và console_handler 
    # sẽ được thêm vào logger và lưu ở file logger.handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    
    return logger