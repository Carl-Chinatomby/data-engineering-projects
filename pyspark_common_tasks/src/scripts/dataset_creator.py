import json
import os
import random
from datetime import (
    datetime,
    timedelta,
)
import pandas as pd
import numpy as np

BASE_PATH = os.path.dirname(os.path.realpath(__file__))
# Generate 200,000 records
records = []
product_categories = ['Electronics', 'Clothing', 'Home & Garden', 'Books', 'Sports']
countries = ['USA', 'UK', 'Canada', 'Australia', 'Germany', 'France', 'Japan']
payment_methods = ['Credit Card', 'PayPal', 'Bank Transfer', 'Digital Wallet']

for i in range(200000):
    # Generate random date within last 2 years
    date = (datetime.now() - timedelta(days=random.randint(0, 730))).strftime('%Y-%m-%d')

    # Create nested JSON structure
    record = {
        'transaction_id': f'TRX-{i+1:06d}',
        'date': date,
        'product': {
            'id': f'PROD-{random.randint(1000, 9999)}',
            'name': f'Product-{random.randint(1, 1000)}',
            'category': random.choice(product_categories),
            'specifications': {
                'weight': round(random.uniform(0.1, 10.0), 2),
                'dimensions': {
                    'length': round(random.uniform(5, 50), 2),
                    'width': round(random.uniform(5, 50), 2),
                    'height': round(random.uniform(5, 50), 2)
                }
            }
        },
        'customer': {
            'id': f'CUST-{random.randint(1000, 9999)}',
            'location': {
                'country': random.choice(countries),
                'city': f'City-{random.randint(1, 100)}'
            }
        },
        'payment': {
            'method': random.choice(payment_methods),
            'amount': round(random.uniform(10, 1000), 2),
            'currency': 'USD',
            'status': random.choice(['completed', 'pending', 'failed']),
            'details': {
                'transaction_fee': round(random.uniform(1, 10), 2),
                'tax': round(random.uniform(5, 50), 2)
            }
        },
        'shipping': {
            'method': random.choice(['Standard', 'Express', 'Priority']),
            'cost': round(random.uniform(5, 50), 2),
            'estimated_days': random.randint(1, 10)
        }
    }
    records.append(record)

with open(f'{BASE_PATH}/../../data/sales_data.json', 'w') as f:
    json.dump(records, f)

print("Dataset created successfully with 200,000 records!")