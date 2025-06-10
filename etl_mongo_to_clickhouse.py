from pymongo import MongoClient
from clickhouse_driver import Client as ClickHouseClient
import datetime
import time # Import the time module

def etl_mongo_to_clickhouse():
    print("Starting ETL process: MongoDB to ClickHouse...")

    try:
        # MongoDB Connection
        print("Connecting to MongoDB...")
        mongo_client = MongoClient('mongodb://admin:password@localhost:27017/')
        mongo_db = mongo_client['user_profiles_db']
        profiles_collection = mongo_db['profiles']
        print("Successfully connected to MongoDB.")

        # ClickHouse Connection
        print("Waiting a few seconds for ClickHouse to initialize...")
        time.sleep(15) # Increased delay to 15 seconds
        print("Connecting to ClickHouse...")
        # Ensure ClickHouse container 'clickhouse_poc' is accessible on 'localhost' from the script's perspective.
        # If running this script from outside Docker, 'localhost' should work.
        # If running this script from *inside* another Docker container on the same Docker network,
        # you would use the service name 'clickhouse_poc' as the host.
        ch_client = ClickHouseClient(
            host='localhost',
            port=9000,
            user='default',
            password='clickhouse_poc_password'  # Use the password set in docker-compose
        )
        print("Successfully connected to ClickHouse.")

        print(f"Checking if ClickHouse database 'olap_db' exists...")
        ch_client.execute('CREATE DATABASE IF NOT EXISTS olap_db')
        print(f"Database 'olap_db' ensured.")

        print(f"Checking if ClickHouse table 'olap_db.user_profiles_olap' exists...")
        ch_client.execute('''
            CREATE TABLE IF NOT EXISTS olap_db.user_profiles_olap (
                user_id String,
                first_name String,
                last_name String,
                email String,
                registration_date DateTime,
                last_login_date DateTime,
                country String,
                is_premium_user UInt8,
                age UInt8
            ) ENGINE = MergeTree()
            ORDER BY (registration_date, user_id)
        ''')
        print("Table 'olap_db.user_profiles_olap' ensured.")

        # Fetch data from MongoDB
        print("Fetching data from MongoDB collection 'profiles'...")
        mongo_documents = list(profiles_collection.find())
        print(f"Fetched {len(mongo_documents)} documents from MongoDB.")

        if not mongo_documents:
            print("No documents found in MongoDB to insert. ETL process finished.")
            return

        documents_to_insert = []
        for doc in mongo_documents:
            # Ensure datetime objects are naive or correctly timezone-aware for ClickHouse
            # For simplicity, assuming MongoDB dates are UTC and ClickHouse will store them as such.
            reg_date = doc.get("registration_date")
            last_login = doc.get("last_login_date")

            # ClickHouse driver expects datetime.datetime for DateTime fields
            if isinstance(reg_date, datetime.datetime):
                pass # Already a datetime object
            elif reg_date is not None: # Potentially a different type from BSON, though usually datetime
                print(f"Warning: registration_date for user {doc.get('user_id')} is not a datetime object: {type(reg_date)}")
                reg_date = None # Or attempt conversion

            if isinstance(last_login, datetime.datetime):
                pass # Already a datetime object
            elif last_login is not None:
                print(f"Warning: last_login_date for user {doc.get('user_id')} is not a datetime object: {type(last_login)}")
                last_login = None # Or attempt conversion


            documents_to_insert.append({
                "user_id": str(doc.get("user_id", "")), # Ensure string
                "first_name": doc.get("first_name"),
                "last_name": doc.get("last_name"),
                "email": doc.get("email"),
                "registration_date": reg_date,
                "last_login_date": last_login,
                "country": doc.get("country"),
                "is_premium_user": 1 if doc.get("is_premium_user") else 0,
                "age": doc.get("age")
            })

        # Insert data into ClickHouse
        if documents_to_insert:
            print(f"Inserting {len(documents_to_insert)} documents into ClickHouse table 'olap_db.user_profiles_olap'...")
            # It's good practice to clear the table if you're re-running the ETL for idempotency,
            # or handle updates/merges if it's incremental. For this POC, we'll just insert.
            # Consider: ch_client.execute('TRUNCATE TABLE olap_db.user_profiles_olap')
            
            ch_client.execute(
                'INSERT INTO olap_db.user_profiles_olap VALUES',
                documents_to_insert,
                types_check=True # Recommended for safety
            )
            print(f"Successfully inserted {len(documents_to_insert)} documents into ClickHouse.")
        else:
            print("No documents processed for ClickHouse insertion.")

    except Exception as e:
        print(f"An error occurred during the ETL process: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if 'mongo_client' in locals() and mongo_client:
            mongo_client.close()
            print("MongoDB connection closed.")
        if 'ch_client' in locals() and ch_client:
            # The clickhouse_driver client doesn't have an explicit close() method in the same way.
            # Connections are typically managed per query or within a context manager if needed.
            print("ClickHouse client operations finished.")
    
    print("ETL process finished.")

if __name__ == "__main__":
    etl_mongo_to_clickhouse()