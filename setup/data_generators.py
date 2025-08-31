import random
from datetime import datetime, timedelta
import sqlite3

class DataGenerator:
    def __init__(self, db_connection):
        self.conn = db_connection
        self.cursor = db_connection.cursor()
        
        # Product tiers configuration with clean prices
        self.product_tiers = {
            1: {"production_cost": 20.00, "retail_cost": 25.00, "margin": 0.20},  # $20 cost, $25 retail
            2: {"production_cost": 80.00, "retail_cost": 120.00, "margin": 0.33},  # $80 cost, $120 retail
            3: {"production_cost": 200.00, "retail_cost": 400.00, "margin": 0.50}  # $200 cost, $400 retail
        }
        
        # Geographic regions
        self.regions = ["USA", "UK", "CA", "RU", "DE", "AM", "FR"]
        
        # Discount tiers
        self.discount_tiers = [0, 5, 10, 15, 20]
        
        # Product names by tier (just one product per tier)
        self.product_names = {
            1: "Basic T-Shirt",
            2: "Premium Headphones", 
            3: "Laptop"
        }
    
    def generate_products(self):
        """Generate 3 products (1 per tier)"""
        products = []
        
        for tier in [1, 2, 3]:
            tier_config = self.product_tiers[tier]
            production_cost = tier_config["production_cost"]
            retail_cost = tier_config["retail_cost"]
            name = f"{self.product_names[tier]} (Tier {tier})"
            
            products.append((tier, name, production_cost, retail_cost, tier))
        
        # Insert products
        self.cursor.executemany('''
            INSERT INTO products (product_id, name, production_cost, retail_cost, tier)
            VALUES (?, ?, ?, ?, ?)
        ''', products)
        
        self.conn.commit()
        print(f"Generated {len(products)} products")
    
    def generate_users(self):
        """Generate 50 users (7-8 per region)"""
        users = []
        user_id = 1
        
        for region in self.regions:
            # Generate 7-8 users per region
            num_users = 8 if region in ["USA", "UK"] else 6
            
            for _ in range(num_users):
                name = f"User_{user_id:03d}_{region}"
                age = random.randint(18, 65)
                discount_tier = random.choice(self.discount_tiers)
                
                users.append((user_id, name, age, region, discount_tier))
                user_id += 1
        
        # Insert users
        self.cursor.executemany('''
            INSERT INTO users (user_id, name, age, geo, discount_tier)
            VALUES (?, ?, ?, ?, ?)
        ''', users)
        
        self.conn.commit()
        print(f"Generated {len(users)} users")
    
    def generate_transactions(self, num_transactions=1000):
        """Generate smart transactions that demonstrate discount effectiveness"""
        transactions = []
        
        # Get all products and users
        self.cursor.execute("SELECT product_id, retail_cost, tier FROM products")
        products = self.cursor.fetchall()
        
        self.cursor.execute("SELECT user_id, discount_tier FROM users")
        users = self.cursor.fetchall()
        
        # Date range: last 6 months
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180)
        
        for transaction_id in range(1, num_transactions + 1):
            # Select random product and user
            product = random.choice(products)
            user = random.choice(users)
            
            product_id, retail_cost, tier = product
            user_id, user_discount_tier = user
            
            # Smart discount logic: ensure discounts don't exceed margin and create profit increase
            if user_discount_tier > 0:
                # Get product margin to ensure we don't lose money
                product_config = self.product_tiers[tier]
                margin_percentage = product_config["margin"] * 100
                
                # Only apply discount if it doesn't exceed the margin
                if user_discount_tier <= margin_percentage:
                    # Higher probability of applying discount for higher tiers
                    # This ensures more premium products get discounted, increasing total profit
                    discount_probability = 0.2 + (tier * 0.25)  # 45% for tier 1, 70% for tier 2, 95% for tier 3
                    
                    if random.random() < discount_probability:
                        applied_discount = user_discount_tier
                    else:
                        applied_discount = 0
                else:
                    # If discount would exceed margin, don't apply it
                    applied_discount = 0
            else:
                applied_discount = 0
            
            # Calculate costs
            initial_cost = retail_cost
            final_cost = initial_cost * (1 - applied_discount / 100)
            
            # Random date within range
            days_between = (end_date - start_date).days
            random_days = random.randint(0, days_between)
            transaction_date = start_date + timedelta(days=random_days)
            
            transactions.append((
                transaction_id,
                product_id,
                user_id,
                transaction_date.date(),
                round(initial_cost, 2),
                applied_discount,
                round(final_cost, 2)
            ))
        
        # Insert transactions
        self.cursor.executemany('''
            INSERT INTO transactions 
            (transaction_id, product_id, user_id, transaction_date, 
             initial_cost, applied_discount, final_cost)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', transactions)
        
        self.conn.commit()
        print(f"Generated {len(transactions)} transactions")
