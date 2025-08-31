import sqlite3
from datetime import datetime, timedelta


class EcommerceDatabase:
    def __init__(self, db_name="ecommerce.db"):
        self.db_name = db_name
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Connect to SQLite database"""
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        print(f"Connected to database: {self.db_name}")
    
    def create_tables(self):
        """Create the three main tables"""
        
        # Products table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                product_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                production_cost REAL NOT NULL,
                retail_cost REAL NOT NULL,
                tier INTEGER NOT NULL
            )
        ''')
        
        # Users table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER NOT NULL,
                geo TEXT NOT NULL,
                discount_tier INTEGER NOT NULL
            )
        ''')
        
        # Transactions table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id INTEGER PRIMARY KEY,
                product_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                transaction_date DATE NOT NULL,
                initial_cost REAL NOT NULL,
                applied_discount REAL NOT NULL,
                final_cost REAL NOT NULL,
                FOREIGN KEY (product_id) REFERENCES products (product_id),
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        
        self.conn.commit()
        print("Tables created successfully!")
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    db = EcommerceDatabase()
    db.connect()
    db.create_tables()
    db.close()
