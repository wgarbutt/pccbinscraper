import json
import os
import time
from datetime import datetime, timedelta
import requests
import paho.mqtt.client as mqtt
import schedule

# Get configuration from environment variables
address = os.environ.get('ADDRESS', 'YOUR ADDRESS')
mqtt_broker = os.environ.get('MQTT_BROKER', 'IP OF MQTT SERVER')
mqtt_port = int(os.environ.get('MQTT_PORT', 1883))
mqtt_username = os.environ.get('MQTT_USERNAME', 'username')
mqtt_password = os.environ.get('MQTT_PASSWORD', 'password')
run_frequency = os.environ.get('RUN_FREQUENCY', 'daily')  # New parameter

# ... (keep existing function definitions: on_connect, on_publish, get_current_week, get_next_collection_date)

def main_job():
    print(f"Running job at {datetime.now()}")
    
    # URL of the API endpoint
    base_url = 'https://maps.poriruacity.govt.nz/server/rest/services/Property/PropertyAdminExternal/MapServer/5/query'

    # Prepare the query parameters
    params = {
        'where': f"lower(address) LIKE '%{address.lower()}%'",
        'f': 'pjson',
        'outFields': 'Address,OBJECTID,Collection_Day,Collection_Zone',
        'returnGeometry': 'false',
        'resultRecordCount': '20',
        'orderByFields': 'Address'
    }

    # Make the API request
    response = requests.get(base_url, params=params)
    data = response.json()

    if not data['features']:
        print("Address not found")
        return

    # Get the first address result
    first_address = data['features'][0]['attributes']
    collection_day = first_address['Collection_Day']
    collection_zone = first_address['Collection_Zone']

    print(f"Collection Day: {collection_day}")
    print(f"Collection Zone: {collection_zone}")

    next_collection = get_next_collection_date(collection_day)

    recycling_data = {
        next_collection: ["General Waste", "Recycling"]  # This is a placeholder. Adjust based on actual data.
    }

    # Initialize the MQTT client
    mqtt_client = mqtt.Client()
    mqtt_client.username_pw_set(mqtt_username, mqtt_password)

    # Connect to the MQTT broker
    print("Connecting to MQTT broker...")
    try:
        mqtt_client.connect(mqtt_broker, mqtt_port, 60)
    except Exception as e:
        print("Error connecting to MQTT broker:", e)
        return

    # Publish the recycling items to MQTT as JSON
    for date, keywords in recycling_data.items():
        keyword_list = ", ".join(keywords)
        message = {
            "Date": date,
            "Contains": keyword_list
        }
        topic = "recycling_schedule"
        json_message = json.dumps(message)
        print(f"Publishing message: {json_message} to topic: {topic}")
        try:
            mqtt_client.publish(topic, json_message, retain=True)
        except Exception as e:
            print("Error publishing message to MQTT:", e)

    # Disconnect from the MQTT broker
    mqtt_client.disconnect()
    print("Disconnected from MQTT broker.")

def run_scheduler():
    if run_frequency == 'hourly':
        schedule.every().hour.do(main_job)
    elif run_frequency == 'daily':
        schedule.every().day.at("00:00").do(main_job)
    elif run_frequency == 'weekly':
        schedule.every().monday.at("00:00").do(main_job)
    else:
        print(f"Invalid run frequency: {run_frequency}. Using daily as default.")
        schedule.every().day.at("00:00").do(main_job)

    print(f"Scheduler set to run {run_frequency}")
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    run_scheduler()