import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json
import uuid

class DataGenerator:
    def __init__(self, sample_data_dir: str):
        """
        Initialize DataGenerator
        
        Parameters:
        -----------
        sample_data_dir : str
            Directory containing sampled data files
        """
        self.sample_data_dir = sample_data_dir
        self.load_sample_data()
        
    def load_sample_data(self):
        """Load sample data for reference"""
        self.products = pd.read_csv(f"{self.sample_data_dir}/product_sampled.csv")
        self.customers = pd.read_csv(f"{self.sample_data_dir}/customer_sampled.csv")
    
    def generate_click_event(self, customer_id: int, timestamp: datetime) -> dict:
        """Generate a synthetic click event"""
        event_types = ['HOMEPAGE', 'SEARCH', 'ITEM_DETAIL', 'ADD_TO_CART', 'CHECKOUT']
        event_type = random.choice(event_types)
        
        event = {
            'session_id': str(uuid.uuid4()),
            'event_name': event_type,
            'event_time': timestamp.isoformat(),
            'event_id': str(uuid.uuid4()),
            'traffic_source': random.choice(['mobile', 'web']),
            'customer_id': customer_id
        }
        
        # Add event specific metadata
        if event_type == 'SEARCH':
            event['event_metadata'] = json.dumps({
                'search_keywords': random.choice(['shirt', 'shoes', 'pants', 'dress'])
            })
        elif event_type in ['ITEM_DETAIL', 'ADD_TO_CART']:
            product = self.products.sample(1).iloc[0]
            event['event_metadata'] = json.dumps({
                'product_id': int(product['id']),
                'product_name': product['productDisplayName']
            })
            
        return event
    
    def generate_transaction(self, customer_id: int, timestamp: datetime) -> dict:
        """Generate a synthetic transaction"""
        products = self.products.sample(random.randint(1, 3))
        items = []
        total_amount = 0
        
        for _, product in products.iterrows():
            quantity = random.randint(1, 3)
            price = random.randint(50000, 500000)
            items.append({
                'product_id': int(product['id']),
                'quantity': quantity,
                'item_price': price
            })
            total_amount += quantity * price
        
        transaction = {
            'created_at': timestamp.isoformat(),
            'customer_id': customer_id,
            'booking_id': str(uuid.uuid4()),
            'session_id': str(uuid.uuid4()),
            'product_metadata': json.dumps(items),
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'OVO', 'Gopay']),
            'payment_status': 'Success',
            'total_amount': total_amount
        }
        
        return transaction
    
    def generate_batch(self, batch_size: int = 10) -> tuple:
        """Generate a batch of events and transactions"""
        timestamp = datetime.now()
        clicks = []
        transactions = []
        
        for _ in range(batch_size):
            customer = self.customers.sample(1).iloc[0]
            customer_id = int(customer['customer_id'])
            
            # Generate 3-7 clicks per customer
            for _ in range(random.randint(3, 7)):
                clicks.append(
                    self.generate_click_event(
                        customer_id,
                        timestamp + timedelta(seconds=random.randint(0, 3600))
                    )
                )
            
            # 30% chance of transaction
            if random.random() < 0.3:
                transactions.append(
                    self.generate_transaction(
                        customer_id,
                        timestamp + timedelta(seconds=random.randint(0, 3600))
                    )
                )
        
        return clicks, transactions

def main():
    # Example usage
    generator = DataGenerator("data/processed/sampled")
    clicks, transactions = generator.generate_batch(batch_size=10)
    
    print(f"Generated {len(clicks)} clicks and {len(transactions)} transactions")
    
    # Save to JSON for testing
    with open("data/processed/synthetic/sample_clicks.json", "w") as f:
        json.dump(clicks, f, indent=2)
    
    with open("data/processed/synthetic/sample_transactions.json", "w") as f:
        json.dump(transactions, f, indent=2)

if __name__ == "__main__":
    main()