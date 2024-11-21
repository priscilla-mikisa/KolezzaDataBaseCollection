import asyncio
import websockets
import json
import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# WebSocket and API constants
THINGSBOARD_TOKEN="eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ1bXVyZXJ3YXF1ZWVuYmVsbGFAZ21haWwuY29tIiwidXNlcklkIjoiYTE4NTYzNzAtOWZlNy0xMWVmLTk5MDQtOGZlNDE2NWM3MDg1Iiwic2NvcGVzIjpbIlRFTkFOVF9BRE1JTiJdLCJzZXNzaW9uSWQiOiJmYWU1MmQ0Zi1mMDU1LTQ0NTgtOWIyNC00ZDk5ODM3OTIxN2IiLCJleHAiOjE3MzE5ODM2ODYsImlzcyI6InRoaW5nc2JvYXJkLmNsb3VkIiwiaWF0IjoxNzMxOTU0ODg2LCJmaXJzdE5hbWUiOiJVbXVyZXJ3YSIsImxhc3ROYW1lIjoiUXVlZW4gYmVsbGEiLCJlbmFibGVkIjp0cnVlLCJpc1B1YmxpYyI6ZmFsc2UsImlzQmlsbGluZ1NlcnZpY2UiOmZhbHNlLCJwcml2YWN5UG9saWN5QWNjZXB0ZWQiOnRydWUsInRlcm1zT2ZVc2VBY2NlcHRlZCI6dHJ1ZSwidGVuYW50SWQiOiJhMTM4NTRlMC05ZmU3LTExZWYtOTkwNC04ZmU0MTY1YzcwODUiLCJjdXN0b21lcklkIjoiMTM4MTQwMDAtMWRkMi0xMWIyLTgwODAtODA4MDgwODA4MDgwIn0.eWXrZQmompayDm71-nAlh5hU0G9LlVAzu3LLmMPSbYmlLYYaxa3kprwyQ_f06PYI3P0p2qgm334WATX2Ynyf4w"
ENTITY_ID = "c5348530-9fe7-11ef-9904-8fe4165c7085"
WEBSOCKET_URL = f"wss://thingsboard.cloud/api/ws/plugins/telemetry?token={THINGSBOARD_TOKEN}"


def connect_to_db():
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return connection
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")
        return None

def insert_data_to_db(connection, device_id, telemetry_data):
    if connection is None:
        logger.error("Database connection is unavailable")
        return
    
    try:
        cursor = connection.cursor()
        logger.debug(f"Received telemetry data: {json.dumps(telemetry_data, indent=2)}")
        
        for key, value_array in telemetry_data.items():
            if not value_array or not isinstance(value_array, list):
                logger.warning(f"Skipping {key} - invalid data format")
                continue
                
            timestamp = value_array[0][0]
            value = value_array[0][1]
            
            logger.info(f"Inserting - Key: {key}, Value: {value}, Timestamp: {timestamp}")
            
            query = """
                INSERT INTO thingsboard_data (device_id, key, value, timestamp)
                VALUES (%s, %s, %s, %s)
            """
            
            cursor.execute(query, (device_id, key, str(value), timestamp))
            logger.info(f"Successfully inserted {key} data")
        
        connection.commit()
        cursor.close()
        
    except Exception as e:
        logger.error(f"Error inserting data into the database: {e}")
        if connection:
            connection.rollback()

async def fetch_telemetry():
    logger.info(f"Connecting to WebSocket URL: {WEBSOCKET_URL}")
    
    try:
        async with websockets.connect(WEBSOCKET_URL, ssl=True) as websocket:
            logger.info("Connected to WebSocket")
            
            # Subscribe to telemetry
            subscribe_msg = {
                "tsSubCmds": [{
                    "entityType": "DEVICE",
                    "entityId": ENTITY_ID,
                    "scope": "LATEST_TELEMETRY",
                    "cmdId": 10
                }],
                "historyCmds": [],
                "attrSubCmds": []
            }
            
            logger.info("Sending subscription message")
            await websocket.send(json.dumps(subscribe_msg))
            
            while True:
                try:
                    response = await websocket.recv()
                    logger.debug(f"Raw response: {response}")
                    data = json.loads(response)
                    
                    if 'data' in data:
                        logger.info("Processing received data")
                        connection = connect_to_db()
                        if connection:
                            try:
                                insert_data_to_db(connection, ENTITY_ID, data['data'])
                            finally:
                                connection.close()
                    else:
                        logger.debug("Received message without data")
                    
                    await asyncio.sleep(1)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding JSON: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue
                
    except websockets.exceptions.ConnectionClosed as e:
        logger.error(f"WebSocket connection closed: {e}")
    except Exception as e:
        logger.error(f"Error: {e}")

def reset_database():
    connection = connect_to_db()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS thingsboard_data (
                    id SERIAL PRIMARY KEY,
                    device_id VARCHAR(255),
                    key VARCHAR(255),
                    value TEXT,
                    timestamp BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            connection.commit()
            logger.info("Database table created/verified")
        except Exception as e:
            logger.error(f"Error creating database table: {e}")
        finally:
            connection.close()

if __name__ == "__main__":
    # Create the table if it doesn't exist
    reset_database()
    
    # Start the telemetry fetch loop
    logger.info("Starting telemetry fetch...")
    try:
        asyncio.get_event_loop().run_until_complete(fetch_telemetry())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")