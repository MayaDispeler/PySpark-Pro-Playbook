#!/usr/bin/env python3
"""
Enterprise PySpark Data Generator
--------------------------------
Generate sample datasets for testing PySpark operations.

This script creates multiple test datasets for PySpark:
1. Employee data
2. Sales transactions
3. Customer interactions
4. Product catalog
5. Web logs

Usage:
    python data_generator.py --output=./sample_data

Requirements:
    - pyspark
    - pandas
    - numpy
    - faker
"""

import os
import sys
import argparse
import random
import json
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from faker import Faker

# Initialize faker
fake = Faker()

def ensure_dir(directory):
    """Create directory if it doesn't exist"""
    if not os.path.exists(directory):
        os.makedirs(directory)

def generate_employee_data(output_dir, num_records=1000):
    """Generate employee dataset"""
    print(f"Generating {num_records} employee records...")
    
    departments = ["Engineering", "Marketing", "Sales", "Finance", "HR", "Operations", "IT", "Customer Support"]
    job_titles = {
        "Engineering": ["Software Engineer", "Data Scientist", "DevOps Engineer", "QA Engineer", "Engineering Manager"],
        "Marketing": ["Marketing Specialist", "Content Creator", "SEO Specialist", "Marketing Manager", "Brand Strategist"],
        "Sales": ["Sales Representative", "Account Executive", "Sales Manager", "Business Development", "Sales Analyst"],
        "Finance": ["Financial Analyst", "Accountant", "Controller", "Finance Manager", "Auditor"],
        "HR": ["HR Coordinator", "Recruiter", "HR Manager", "Talent Acquisition", "HR Director"],
        "Operations": ["Operations Manager", "Project Manager", "Business Analyst", "Operations Analyst", "COO"],
        "IT": ["IT Support", "System Administrator", "Network Engineer", "IT Manager", "Security Specialist"],
        "Customer Support": ["Support Agent", "Customer Success", "Support Manager", "Customer Experience", "Technical Support"]
    }
    
    # Generate data
    employees = []
    
    for i in range(1, num_records + 1):
        department = random.choice(departments)
        hire_date = fake.date_between(start_date='-10y', end_date='today')
        years_of_service = (datetime.now().date() - hire_date).days / 365
        
        # Base salary by department
        dept_salary = {
            "Engineering": 90000,
            "Marketing": 75000,
            "Sales": 80000,
            "Finance": 85000,
            "HR": 70000,
            "Operations": 72000,
            "IT": 88000,
            "Customer Support": 65000
        }
        
        # Adjust salary by experience
        base_salary = dept_salary[department]
        experience_multiplier = 1 + min(years_of_service * 0.03, 0.6)  # Cap at 60% increase
        salary = int(base_salary * experience_multiplier)
        
        # Add random variation (±10%)
        salary = int(salary * random.uniform(0.9, 1.1))
        
        employee = {
            "id": i,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone_number": fake.phone_number(),
            "hire_date": hire_date.strftime("%Y-%m-%d"),
            "job_title": random.choice(job_titles[department]),
            "department": department,
            "salary": salary,
            "manager_id": random.randint(1, 50) if i > 50 else None,
            "address": {
                "street": fake.street_address(),
                "city": fake.city(),
                "state": fake.state_abbr(),
                "zip": fake.zipcode(),
                "country": "USA"
            },
            "skills": random.sample(["Python", "SQL", "Java", "Scala", "R", "Spark", 
                                     "Hadoop", "Excel", "Tableau", "PowerBI", "AWS", 
                                     "Azure", "GCP", "Docker", "Kubernetes"], 
                                    random.randint(2, 5))
        }
        employees.append(employee)
    
    # Write parquet and json files
    df = pd.DataFrame(employees)
    df.to_parquet(f"{output_dir}/employees.parquet")
    df.to_csv(f"{output_dir}/employees.csv", index=False)
    
    # Write a sample as JSON for easier viewing
    with open(f"{output_dir}/employees_sample.json", "w") as f:
        json.dump(employees[:10], f, indent=2)
    
    print(f"Employee data saved to {output_dir}/employees.parquet, {output_dir}/employees.csv, and {output_dir}/employees_sample.json")

def generate_sales_data(output_dir, num_records=10000):
    """Generate sales transaction dataset"""
    print(f"Generating {num_records} sales records...")
    
    products = {
        "Laptop": {"category": "Electronics", "price_range": (800, 2500)},
        "Smartphone": {"category": "Electronics", "price_range": (400, 1200)},
        "Tablet": {"category": "Electronics", "price_range": (200, 800)},
        "Monitor": {"category": "Electronics", "price_range": (150, 700)},
        "Keyboard": {"category": "Electronics", "price_range": (20, 200)},
        "Mouse": {"category": "Electronics", "price_range": (10, 150)},
        "Headphones": {"category": "Electronics", "price_range": (30, 350)},
        "Desk": {"category": "Furniture", "price_range": (100, 800)},
        "Chair": {"category": "Furniture", "price_range": (50, 500)},
        "Bookshelf": {"category": "Furniture", "price_range": (80, 400)},
        "Sofa": {"category": "Furniture", "price_range": (300, 2000)},
        "Bed Frame": {"category": "Furniture", "price_range": (200, 1500)},
        "T-shirt": {"category": "Clothing", "price_range": (15, 50)},
        "Jeans": {"category": "Clothing", "price_range": (30, 100)},
        "Shoes": {"category": "Clothing", "price_range": (40, 200)},
        "Jacket": {"category": "Clothing", "price_range": (50, 300)},
        "Sweater": {"category": "Clothing", "price_range": (25, 120)},
        "Blender": {"category": "Appliances", "price_range": (30, 200)},
        "Toaster": {"category": "Appliances", "price_range": (20, 100)},
        "Coffee Maker": {"category": "Appliances", "price_range": (25, 300)}
    }
    
    regions = ["North", "South", "East", "West"]
    payment_methods = ["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Google Pay", "Cash"]
    
    # Generate data
    sales = []
    
    start_date = datetime(2020, 1, 1)
    end_date = datetime.now()
    days_between = (end_date - start_date).days
    
    for i in range(1, num_records + 1):
        product_name = random.choice(list(products.keys()))
        product_info = products[product_name]
        category = product_info["category"]
        price_range = product_info["price_range"]
        
        unit_price = round(random.uniform(*price_range), 2)
        quantity = random.randint(1, 5)
        discount = round(random.uniform(0, 0.3), 2) if random.random() < 0.3 else 0
        
        # Calculate total with discount
        total = round(unit_price * quantity * (1 - discount), 2)
        
        # Generate random date
        random_days = random.randint(0, days_between)
        sale_date = (start_date + timedelta(days=random_days))
        
        # More recent sales are more likely
        if random.random() < 0.7:
            sale_date = (datetime.now() - timedelta(days=random.randint(0, 365)))
        
        sale = {
            "transaction_id": i,
            "product": product_name,
            "category": category,
            "quantity": quantity,
            "unit_price": unit_price,
            "discount": discount,
            "total": total,
            "transaction_date": sale_date.strftime("%Y-%m-%d"),
            "transaction_time": sale_date.strftime("%H:%M:%S"),
            "region": random.choice(regions),
            "store_id": random.randint(1, 50),
            "customer_id": random.randint(1, 5000),
            "payment_method": random.choice(payment_methods),
            "is_online": random.random() < 0.6,
        }
        sales.append(sale)
    
    # Write files
    df = pd.DataFrame(sales)
    df.to_parquet(f"{output_dir}/sales.parquet")
    df.to_csv(f"{output_dir}/sales.csv", index=False)
    
    # Write a sample as JSON
    with open(f"{output_dir}/sales_sample.json", "w") as f:
        json.dump(sales[:10], f, indent=2)
    
    print(f"Sales data saved to {output_dir}/sales.parquet, {output_dir}/sales.csv, and {output_dir}/sales_sample.json")

def generate_customer_data(output_dir, num_records=5000):
    """Generate customer dataset"""
    print(f"Generating {num_records} customer records...")
    
    segments = ["New", "Regular", "Premium", "VIP"]
    channels = ["Web Search", "Social Media", "Referral", "Direct", "Partner", "Advertisement"]
    
    # Generate data
    customers = []
    
    for i in range(1, num_records + 1):
        registration_date = fake.date_between(start_date='-5y', end_date='today')
        
        # Calculate customer lifetime value
        days_as_customer = (datetime.now().date() - registration_date).days
        base_clv = random.uniform(100, 5000)
        customer_value = base_clv * (days_as_customer / 365) * random.uniform(0.5, 1.5)
        
        customer = {
            "customer_id": i,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "registration_date": registration_date.strftime("%Y-%m-%d"),
            "last_activity_date": fake.date_between(start_date=registration_date, end_date='today').strftime("%Y-%m-%d"),
            "segment": random.choices(segments, weights=[0.3, 0.4, 0.2, 0.1])[0],
            "total_purchases": int(days_as_customer / 30 * random.uniform(0.1, 1)),
            "total_spent": round(customer_value, 2),
            "average_order_value": round(customer_value / max(1, days_as_customer / 30 * random.uniform(0.1, 1)), 2),
            "acquisition_channel": random.choice(channels),
            "is_active": random.random() < 0.8,
            "address": {
                "street": fake.street_address(),
                "city": fake.city(),
                "state": fake.state_abbr(),
                "zip": fake.zipcode(),
                "country": "USA"
            },
            "preferences": {
                "preferred_categories": random.sample(["Electronics", "Furniture", "Clothing", "Appliances"], random.randint(1, 3)),
                "communication_preferences": random.sample(["Email", "SMS", "Phone", "Mail"], random.randint(1, 3)),
                "favorite_products": random.sample(list(range(1, 100)), random.randint(0, 5))
            }
        }
        customers.append(customer)
    
    # Write files
    df = pd.DataFrame(customers)
    df.to_parquet(f"{output_dir}/customers.parquet")
    df.to_csv(f"{output_dir}/customers.csv", index=False)
    
    # Write a sample as JSON
    with open(f"{output_dir}/customers_sample.json", "w") as f:
        json.dump(customers[:10], f, indent=2)
    
    print(f"Customer data saved to {output_dir}/customers.parquet, {output_dir}/customers.csv, and {output_dir}/customers_sample.json")

def generate_web_logs(output_dir, num_records=50000):
    """Generate web log dataset"""
    print(f"Generating {num_records} web log records...")
    
    pages = [
        "/", "/products", "/about", "/contact", "/login", "/register",
        "/cart", "/checkout", "/profile", "/orders", "/wishlist",
        "/products/electronics", "/products/furniture", "/products/clothing", "/products/appliances",
        "/product/1", "/product/2", "/product/3", "/product/4", "/product/5",
        "/search", "/blog", "/support", "/faq", "/terms", "/privacy"
    ]
    
    actions = ["view", "click", "scroll", "hover", "submit", "play", "pause", "add_to_cart", "remove_from_cart", "checkout"]
    devices = ["desktop", "mobile", "tablet"]
    browsers = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]
    os_list = ["Windows", "MacOS", "iOS", "Android", "Linux"]
    
    # Generate data
    logs = []
    
    start_date = datetime.now() - timedelta(days=30)
    
    for i in range(1, num_records + 1):
        timestamp = start_date + timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        # 20% of logs have user_id (logged in users)
        user_id = random.randint(1, 5000) if random.random() < 0.2 else None
        
        # Generate session_id
        session_id = fake.uuid4()
        
        # Current page and referrer
        current_page = random.choice(pages)
        referrer = random.choice(pages + ["https://google.com", "https://facebook.com", 
                                          "https://twitter.com", "https://instagram.com", 
                                          "https://linkedin.com", None])
        
        # IP and location
        ip = fake.ipv4()
        
        log_entry = {
            "log_id": i,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "user_id": user_id,
            "session_id": session_id,
            "ip_address": ip,
            "action": random.choice(actions),
            "page": current_page,
            "referrer": referrer,
            "device": random.choice(devices),
            "browser": random.choice(browsers),
            "operating_system": random.choice(os_list),
            "duration_seconds": random.randint(1, 300),
            "status_code": random.choices([200, 301, 302, 404, 500], weights=[0.92, 0.03, 0.02, 0.02, 0.01])[0],
            "location": {
                "city": fake.city(),
                "state": fake.state_abbr(),
                "country": fake.country_code(),
                "latitude": float(fake.latitude()),
                "longitude": float(fake.longitude())
            }
        }
        logs.append(log_entry)
    
    # Write files
    df = pd.DataFrame(logs)
    df.to_parquet(f"{output_dir}/web_logs.parquet")
    df.to_csv(f"{output_dir}/web_logs.csv", index=False)
    
    # Write a sample as JSON
    with open(f"{output_dir}/web_logs_sample.json", "w") as f:
        json.dump(logs[:10], f, indent=2)
    
    print(f"Web logs saved to {output_dir}/web_logs.parquet, {output_dir}/web_logs.csv, and {output_dir}/web_logs_sample.json")

def generate_product_catalog(output_dir):
    """Generate product catalog"""
    print("Generating product catalog...")
    
    categories = {
        "Electronics": {
            "subcategories": ["Computers", "Phones", "Audio", "Video", "Accessories"],
            "brands": ["Apple", "Samsung", "Sony", "Dell", "Bose", "LG", "Microsoft", "HP", "Logitech", "Anker"]
        },
        "Furniture": {
            "subcategories": ["Living Room", "Bedroom", "Office", "Dining", "Outdoor"],
            "brands": ["IKEA", "Ashley", "Wayfair", "Herman Miller", "Steelcase", "West Elm", "Crate & Barrel", "CB2", "Pottery Barn", "Room & Board"]
        },
        "Clothing": {
            "subcategories": ["Men's", "Women's", "Children's", "Activewear", "Accessories"],
            "brands": ["Nike", "Adidas", "H&M", "Zara", "Levi's", "Gap", "Uniqlo", "North Face", "Calvin Klein", "Under Armour"]
        },
        "Appliances": {
            "subcategories": ["Kitchen", "Laundry", "Cleaning", "HVAC", "Small Appliances"],
            "brands": ["Whirlpool", "Samsung", "LG", "GE", "Bosch", "KitchenAid", "Dyson", "Shark", "Instant Pot", "Cuisinart"]
        }
    }
    
    # Generate products
    products = []
    product_id = 1
    
    for category, category_info in categories.items():
        for subcategory in category_info["subcategories"]:
            for brand in category_info["brands"]:
                # Create 2-3 products for each brand/subcategory combination
                for _ in range(random.randint(2, 3)):
                    # Set price range based on category
                    if category == "Electronics":
                        base_price = random.uniform(50, 2000)
                    elif category == "Furniture":
                        base_price = random.uniform(100, 1500)
                    elif category == "Clothing":
                        base_price = random.uniform(15, 200)
                    else:  # Appliances
                        base_price = random.uniform(30, 1000)
                    
                    # Generate product description
                    if category == "Electronics":
                        adjectives = ["Smart", "Wireless", "High-Performance", "Premium", "Ultra", "Pro", "Compact"]
                    elif category == "Furniture":
                        adjectives = ["Modern", "Classic", "Comfortable", "Elegant", "Sturdy", "Stylish", "Minimalist"]
                    elif category == "Clothing":
                        adjectives = ["Casual", "Formal", "Slim-fit", "Relaxed", "Athletic", "Vintage", "Essential"]
                    else:  # Appliances
                        adjectives = ["Smart", "Energy-Efficient", "Compact", "High-Capacity", "Portable", "Professional", "Quiet"]
                    
                    adjective = random.choice(adjectives)
                    
                    # Create product name
                    product_name = f"{brand} {adjective} {subcategory.rstrip('s')}"
                    
                    # Add model number or variant for uniqueness
                    model_number = f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.randint(100, 999)}"
                    product_name = f"{product_name} {model_number}"
                    
                    # Add random variation to price
                    price = round(base_price * random.uniform(0.9, 1.1), 2)
                    
                    # Generate SKU
                    sku = f"{category[:3].upper()}-{subcategory[:3].upper()}-{brand[:3].upper()}-{random.randint(1000, 9999)}"
                    
                    product = {
                        "product_id": product_id,
                        "name": product_name,
                        "description": fake.paragraph(),
                        "category": category,
                        "subcategory": subcategory,
                        "brand": brand,
                        "price": price,
                        "sku": sku,
                        "in_stock": random.choice([True, True, True, False]),  # 75% in stock
                        "inventory_count": random.randint(0, 100),
                        "rating": round(random.uniform(3, 5), 1),
                        "review_count": random.randint(0, 500),
                        "attributes": {
                            "color": random.choice(["Black", "White", "Silver", "Blue", "Red", "Green", "Brown", "Gray"]),
                            "weight": round(random.uniform(0.5, 20), 2),
                            "dimensions": {
                                "length": round(random.uniform(1, 100), 1),
                                "width": round(random.uniform(1, 100), 1),
                                "height": round(random.uniform(1, 100), 1)
                            }
                        },
                        "date_added": fake.date_between(start_date='-2y', end_date='today').strftime("%Y-%m-%d"),
                        "is_featured": random.random() < 0.1,  # 10% featured
                        "is_bestseller": random.random() < 0.2,  # 20% bestseller
                    }
                    products.append(product)
                    product_id += 1
    
    # Write files
    df = pd.DataFrame(products)
    df.to_parquet(f"{output_dir}/products.parquet")
    df.to_csv(f"{output_dir}/products.csv", index=False)
    
    # Write a sample as JSON
    with open(f"{output_dir}/products_sample.json", "w") as f:
        json.dump(products[:10], f, indent=2)
    
    print(f"Product catalog saved to {output_dir}/products.parquet, {output_dir}/products.csv, and {output_dir}/products_sample.json")

def main():
    parser = argparse.ArgumentParser(description="Generate sample datasets for PySpark testing")
    parser.add_argument("--output", default="./sample_data", help="Output directory for generated data")
    args = parser.parse_args()
    
    # Ensure output directory exists
    ensure_dir(args.output)
    
    # Generate datasets
    generate_employee_data(args.output)
    generate_sales_data(args.output)
    generate_customer_data(args.output)
    generate_web_logs(args.output)
    generate_product_catalog(args.output)
    
    print("\nData generation complete!")
    print(f"All datasets have been saved to {args.output}")
    print("\nTo load this data in PySpark, use:")
    print("""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Data Analysis").getOrCreate()

# Load datasets
employees_df = spark.read.parquet("sample_data/employees.parquet")
sales_df = spark.read.parquet("sample_data/sales.parquet")
customers_df = spark.read.parquet("sample_data/customers.parquet")
web_logs_df = spark.read.parquet("sample_data/web_logs.parquet")
products_df = spark.read.parquet("sample_data/products.parquet")

# Show data
employees_df.show(5)
    """)

if __name__ == "__main__":
    main() 