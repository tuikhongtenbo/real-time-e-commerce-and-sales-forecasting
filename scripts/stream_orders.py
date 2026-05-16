import json
import time
import random
import uuid
from datetime import datetime, timezone
import pandas as pd
from kafka import KafkaProducer
import os
from dotenv import load_dotenv

# load biến môi trường từ file .env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

print("Đang tải dữ liệu (Customers & Products)...")
customers_df = pd.read_csv("../data/dim_customers.csv")
products_df = pd.read_csv("../data/dim_products.csv")

# trích xuất danh sách ID để Random nhanh hơn
customer_ids = customers_df['customer_id'].tolist()
product_ids = products_df['product_id'].tolist()

# cấu hình trọng số để dữ liệu có tính phân phối thực tế
PAYMENT_METHODS = ["COD", "Momo", "ZaloPay", "VNPay", "Thẻ tín dụng"]
PAYMENT_WEIGHTS = [0.5, 0.15, 0.15, 0.1, 0.1]

DELIVERY_STATUS = ["Chờ xác nhận", "Đang giao", "Thành công", "Đã hủy", "Hoàn trả"]
STATUS_WEIGHTS = [0.1, 0.2, 0.6, 0.05, 0.05]

# even hub credentials
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS')
EVENT_HUB_CONNECTION_STRING = os.getenv('EVENT_HUB_CONNECTION_STRING')
EVENT_HUB_NAME = os.getenv('EVENT_HUB_NAME')

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='$ConnectionString',
    sasl_plain_password=EVENT_HUB_CONNECTION_STRING,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_order():
    # 1. Bốc ngẫu nhiên ID hợp lệ
    customer_id = random.choice(customer_ids)
    product_id = random.choice(product_ids)
    
    # 2. Lấy thông tin chi tiết (Denormalization - làm phẳng dữ liệu cho Streaming)
    # Lấy row đầu tiên match ID
    customer_info = customers_df[customers_df['customer_id'] == customer_id].iloc[0]
    product_info = products_df[products_df['product_id'] == product_id].iloc[0]
    
    quantity = random.randint(1, 3)
    price = int(product_info['price'])
    
    # 3. Tạo Order Event (JSON)
    order = {
        "order_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(), # Dùng chuẩn ISO 8601 dễ parse ở Databricks
        "customer_id": customer_id,
        "product_id": product_id,
        "category": product_info['category'],
        "price": price,
        "quantity": quantity,
        "total_amount": price * quantity,
        "payment_method": random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0],
        "region": customer_info['region'],
        "city_province": customer_info['city_province'],
        "latitude": float(customer_info['latitude']),
        "longitude": float(customer_info['longitude']),
        "delivery_status": random.choices(DELIVERY_STATUS, weights=STATUS_WEIGHTS)[0]
    }
    return order

if __name__ == "__main__":
    print(f"--- BẮT ĐẦU STREAM ĐƠN HÀNG ---")
    try:
        while True:
            order_data = generate_order()
            
            # In ra màn hình để quan sát
            print(f"[{order_data['timestamp']}] Order: {order_data['order_id']} | "
                  f"Khu vực: {order_data['city_province']} | "
                  f"Tổng tiền: {order_data['total_amount']:,} VND")
            
            # Gửi vào Kafka (nếu producer tồn tại)
            if producer:
                producer.send(EVENT_HUB_NAME, value=order_data)
            
            # Giả lập thời gian giữa các đơn hàng: Từ 0.5 giây đến 2.5 giây có 1 đơn
            time.sleep(random.uniform(0.5, 2.5))
            
    except KeyboardInterrupt:
        print("\nĐã dừng stream do người dùng ngắt!")
    finally:
        if producer:
            producer.close()