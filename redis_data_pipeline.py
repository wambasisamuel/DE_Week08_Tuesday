import pandas as pd
import psycopg2
import redis

# Redis Cloud Instance Information
redis_host = 'redis-14010.c99.us-east-1-4.ec2.cloud.redislabs.com'
redis_port = 14010
redis_password =  'XBLD7yVcHgem98a3zfd6Dgz1rVRqDsqM'

# Postgres Database Information
pg_host = '104.131.120.201'
pg_database = 'call_log_db'
pg_user = 'postgres'
pg_password = 'pg_W33k8'

# Redis Client Object
redis_client = redis.Redis(host=redis_host, port=redis_port, password=redis_password)


def extract_data():
    # Extract data from CSV file using pandas
    csv_location = 'https://raw.githubusercontent.com/wambasisamuel/DE_Week08_Tuesday/main/customer_call_logs.csv'
    data = pd.read_csv(csv_location)
    
    # Cache data in Redis for faster retrieval
    redis_client.set('customer_call_logs', data.to_json())
    

def transform_data():
    # Retrieve data from Redis cache
    data = pd.read_json(redis_client.get('customer_call_logs'))

    # Transform data (clean, structure, format)
    # remove nulls and duplicates
    data = data.dropna()
    data = data.drop_duplicates()

    # Convert duration to minutes
    data['call_duration_min'] = data['call_duration'].str.split(':').apply(lambda x: float(x[0]) * 60 + float(x[1]) + float(x[2]) / 60).astype(float)

    # Format data - set column data types
    data['call_cost_usd'] = data['call_cost'].str.replace('$', '').astype(float)
    data['call_date'] = pd.to_datetime(data['call_date'])

    # Set destination to lowercase
    data['call_destination'] = data['call_destination'].str.lower()

    return data

def load_data(transformed_data):
    # Connect to Postgres database
    conn = psycopg2.connect(host=pg_host, database=pg_database, user=pg_user, password=pg_password)

    # Create a cursor object
    cur = conn.cursor()

    # Create a table to store the data
    cur.execute('CREATE TABLE IF NOT EXISTS customer_call_logs (\
                 customer_id INT,\
                 call_cost_usd FLOAT,\
                 call_destination VARCHAR,\
                 call_date TIMESTAMP,\
                 call_duration_min FLOAT\
                 )')

    # Insert the transformed data into the database
    for i, row in transformed_data.iterrows():
        cur.execute(f"INSERT INTO customer_call_logs (customer_id, call_cost_usd, call_destination, call_date, call_duration_min) VALUES ({row['customer_id']}, {row['call_cost_usd']}, '{row['call_destination']}', '{row['call_date']}', {row['call_duration_min']})")

    # Commit the changes
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()

def data_pipeline():
    # Data pipeline function
    extract_data()
    transformed_data = transform_data()
    load_data(transformed_data)

if __name__ == '__main__':
    # Run the data pipeline function
    data_pipeline()
