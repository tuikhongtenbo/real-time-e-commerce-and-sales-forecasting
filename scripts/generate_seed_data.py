import csv 
import random
import uuid
from faker import Faker


fake = Faker('vi_VN')

num_customers = 1000
num_products = 50

CATEGORIES = {
    "Điện thoại": (5000000, 30000000),
    "Laptop": (10000000, 50000000),
    "Thời trang": (100000, 2000000),
    "Mỹ phẩm": (200000, 3000000),
    "Đồ gia dụng": (500000, 10000000)
}

# payment_methods = ['COD', 'VNPay', 'Momo', 'ZaloPay', 'Thẻ tín dụng']

LOCATIONS = [
    {"region": "Miền Bắc", "city": "Hà Nội", "lat": 21.0285, "lon": 105.8542},
    {"region": "Miền Bắc", "city": "Hải Phòng", "lat": 20.8449, "lon": 106.6881},
    {"region": "Miền Trung", "city": "Đà Nẵng", "lat": 16.0544, "lon": 108.2022},
    {"region": "Miền Trung", "city": "Nghệ An", "lat": 18.6796, "lon": 105.6813},
    {"region": "Miền Nam", "city": "TP. Hồ Chí Minh", "lat": 10.7626, "lon": 106.6602},
    {"region": "Miền Nam", "city": "Cần Thơ", "lat": 10.0452, "lon": 105.7469},
    {"region": "Miền Nam", "city": "Bình Dương", "lat": 11.0667, "lon": 106.6667}
]


def generate_customers(num_customers, filename="../data/dim_customers.csv"):
    print(f"Đang tạo {num_customers} khách hàng...")
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["customer_id", "name", "email", "phone", "region", "city_province", "latitude", "longitude"])
        
        for _ in range(num_customers):
            loc = random.choice(LOCATIONS)
            writer.writerow([
                str(uuid.uuid4()),
                fake.name(),
                fake.ascii_email(), 
                fake.phone_number(),
                loc["region"],
                loc["city"],
                loc["lat"],
                loc["lon"]
            ])
    print(f"Hoàn tất! Đã lưu tại {filename}")


def generate_products(num_products, filename="../data/dim_products.csv"):
    print(f"Đang tạo {num_products} sản phẩm...")
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["product_id", "product_name", "category", "price"])
        
        for _ in range(num_products):
            category = random.choice(list(CATEGORIES.keys()))
            min_price, max_price = CATEGORIES[category]
            price = random.randint(min_price // 1000, max_price // 1000) * 1000 
            product_name = f"{category} {fake.catch_phrase()}"
            
            writer.writerow([
                str(uuid.uuid4()),
                product_name,
                category,
                price
            ])
    print(f"Hoàn tất! Đã lưu tại {filename}")
if __name__ == "__main__":
    generate_customers(100000)
    generate_products(10000)