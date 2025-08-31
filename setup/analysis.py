import sqlite3
import pandas as pd
import os

class EcommerceAnalyzer:
    def __init__(self, db_name="ecommerce.db"):
        # Check if database exists
        if not os.path.exists(db_name):
            print(f"Database {db_name} not found!")
            print("Please run 'python main.py' first to create the database.")
            return
        
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name)
    
    def analyze_discount_effectiveness(self):
        """Analyze how discounts affect revenue"""
        query = """
        SELECT 
            CASE 
                WHEN applied_discount = 0 THEN 'No Discount'
                WHEN applied_discount <= 10 THEN 'Low Discount (5-10%)'
                ELSE 'High Discount (15-20%)'
            END as discount_category,
            COUNT(*) as transaction_count,
            SUM(final_cost) as total_revenue,
            AVG(final_cost) as avg_transaction_value,
            SUM(final_cost) / COUNT(*) as revenue_per_transaction
        FROM transactions
        GROUP BY discount_category
        ORDER BY total_revenue DESC
        """
        
        df = pd.read_sql_query(query, self.conn)
        print("=== Discount Effectiveness Analysis ===")
        print(df)
        return df
    
    def analyze_product_popularity(self):
        """Analyze most popular products"""
        query = """
        SELECT 
            p.name,
            p.tier,
            COUNT(t.transaction_id) as sales_count,
            SUM(t.final_cost) as total_revenue,
            AVG(t.final_cost) as avg_price
        FROM products p
        JOIN transactions t ON p.product_id = t.product_id
        GROUP BY p.product_id, p.name, p.tier
        ORDER BY sales_count DESC
        """
        
        df = pd.read_sql_query(query, self.conn)
        print("\n=== Product Popularity Analysis ===")
        print(df)
        return df
    
    def analyze_geo_performance(self):
        """Analyze performance by geography"""
        query = """
        SELECT 
            u.geo,
            COUNT(t.transaction_id) as transaction_count,
            SUM(t.final_cost) as total_revenue,
            AVG(t.final_cost) as avg_transaction_value,
            AVG(t.applied_discount) as avg_discount_applied
        FROM users u
        JOIN transactions t ON u.user_id = t.user_id
        GROUP BY u.geo
        ORDER BY total_revenue DESC
        """
        
        df = pd.read_sql_query(query, self.conn)
        print("\n=== Geographic Performance Analysis ===")
        print(df)
        return df
    
    def analyze_discount_tier_profitability(self):
        """Analyze which discount tiers are most profitable"""
        query = """
        SELECT 
            u.discount_tier,
            COUNT(t.transaction_id) as transaction_count,
            SUM(t.final_cost) as total_revenue,
            AVG(t.final_cost) as avg_transaction_value,
            SUM(t.applied_discount * t.initial_cost / 100) as total_discount_given
        FROM users u
        JOIN transactions t ON u.user_id = t.user_id
        WHERE t.applied_discount > 0
        GROUP BY u.discount_tier
        ORDER BY total_revenue DESC
        """
        
        df = pd.read_sql_query(query, self.conn)
        print("\n=== Discount Tier Profitability Analysis ===")
        print(df)
        return df
    
    def run_all_analyses(self):
        """Run all analyses"""
        if not hasattr(self, 'conn'):
            return
            
        self.analyze_discount_effectiveness()
        self.analyze_product_popularity()
        self.analyze_geo_performance()
        self.analyze_discount_tier_profitability()
    
    def close(self):
        """Close database connection"""
        if hasattr(self, 'conn'):
            self.conn.close()

def main():
    """Run analysis examples"""
    analyzer = EcommerceAnalyzer()
    analyzer.run_all_analyses()
    analyzer.close()

if __name__ == "__main__":
    main()
