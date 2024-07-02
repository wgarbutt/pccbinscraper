import json
import os
import time
from datetime import datetime, timedelta
import requests
import paho.mqtt.client as mqtt
import schedule
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Get configuration from environment variables
address = os.environ.get('ADDRESS', 'YOUR ADDRESS')
mqtt_broker = os.environ.get('MQTT_BROKER', 'IP OF MQTT SERVER')
mqtt_port = int(os.environ.get('MQTT_PORT', 1883))
mqtt_username = os.environ.get('MQTT_USERNAME', 'username')
mqtt_password = os.environ.get('MQTT_PASSWORD', 'password')
run_frequency = os.environ.get('RUN_FREQUENCY', 'daily')

logging.info(f"Configuration loaded. Address: {address}, MQTT Broker: {mqtt_broker}, Run Frequency: {run_frequency}")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT broker.")
    else:
        logging.error(f"Failed to connect to MQTT broker. Error code: {rc}")

def on_publish(client, userdata, mid):
    logging.info("Message published to MQTT broker.")

def get_current_week():
    return datetime.now().isocalendar()[1]

def get_next_collection_date(collection_day):
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    today = datetime.now()
    collection_day_index = days.index(collection_day)
    days_until_collection = (collection_day_index - today.weekday() + 7) % 7
    next_collection = today + timedelta(days=days_until_collection)
    return next_collection.strftime("%A, %d %B %Y")

def main_job():
    logging.info("Starting main job")
    logging.info(f"Current time: {datetime.now()}")
    
    base_url = 'https://maps.poriruacity.govt.nz/server/rest/services/Property/PropertyAdminExternal/MapServer/5/query'
    params = {
        'where': f"lower(address) LIKE '%{address.lower()}%'",
        'f': 'pjson',
        'outFields': 'Address,OBJECTID,Collection_Day,Collection_Zone',
        'returnGeometry': 'false',
        'resultRecordCount': '20',
        'orderByFields': 'Address'
    }

    try:
        logging.info("Making API request")
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        data = response.json()

        if not data['features']:
            logging.warning("Address not found")
            return

        first_address = data['features'][0]['attributes']
        collection_day = first_address['Collection_Day']
        collection_zone = first_address['Collection_Zone']

        logging.info(f"Collection Day: {collection_day}")
        logging.info(f"Collection Zone: {collection_zone}")

        next_collection = get_next_collection_date(collection_day)
        logging.info(f"Next collection date: {next_collection}")

        recycling_data = {
            next_collection: ["General Waste", "Recycling"]  # This is a placeholder. Adjust based on actual data.
        }

        mqtt_client = mqtt.Client()
        mqtt_client.username_pw_set(mqtt_username, mqtt_password)

        mqtt_client.on_connect = on_connect
        mqtt_client.on_publish = on_publish

        logging.info("Connecting to MQTT broker...")
        mqtt_client.connect(mqtt_broker, mqtt_port, 60)

        for date, keywords in recycling_data.items():
            keyword_list = ", ".join(keywords)
            message = {
                "Date": date,
                "Contains": keyword_list
            }
            topic = "recycling_schedule"
            json_message = json.dumps(message)
            logging.info(f"Publishing message: {json_message} to topic: {topic}")
            mqtt_client.publish(topic, json_message, retain=True)

        mqtt_client.disconnect()
        logging.info("Disconnected from MQTT broker.")

    except requests.RequestException as e:
        logging.error(f"API request failed: {str(e)}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}", exc_info=True)

    logging.info("Main job completed")

def run_scheduler():
    logging.info(f"Setting up scheduler to run {run_frequency}")
    
    if run_frequency == 'hourly':
        schedule.every().hour.do(main_job)
    elif run_frequency == 'daily':
        schedule.every().day.at("00:00").do(main_job)
    elif run_frequency == 'weekly':
        schedule.every().monday.at("00:00").do(main_job)
    else:
        logging.warning(f"Invalid run frequency: {run_frequency}. Using daily as default.")
        schedule.every().day.at("00:00").do(main_job)

    logging.info("Scheduler set up complete. Running initial job and starting main loop.")
    main_job()  # Run the job immediately
    
    while True:
        schedule.run_pending()
        time.sleep(10)
        logging.info("Scheduler is running. Waiting for next job execution.")


if __name__ == "__main__":
    logging.info("Script started")
    try:
        run_scheduler()
    except KeyboardInterrupt:
        logging.info("Script terminated by user")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}", exc_info=True)
    finally:
        logging.info("Script ended")