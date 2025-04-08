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
    datefmt='%Y-%m-%d %H:%M:%S' # Standard ISO format for logs
)

# Get configuration from environment variables
# Default address included for easier testing if ADDRESS env var isn't set
address = os.environ.get('ADDRESS', '168 Endeavour Drive Whitby, Porirua City 5024')
mqtt_broker = os.environ.get('MQTT_BROKER', 'IP OF MQTT SERVER')
mqtt_port = int(os.environ.get('MQTT_PORT', 1883))
mqtt_username = os.environ.get('MQTT_USERNAME', 'username')
mqtt_password = os.environ.get('MQTT_PASSWORD', 'password')
run_frequency = os.environ.get('RUN_FREQUENCY', 'daily')

logging.info(f"Configuration loaded. Address: {address}, MQTT Broker: {mqtt_broker}, Run Frequency: {run_frequency}")

def on_connect(client, userdata, flags, rc):
    """Callback function for when MQTT client connects."""
    if rc == 0:
        logging.info("Connected to MQTT broker.")
    else:
        logging.error(f"Failed to connect to MQTT broker. Error code: {rc}")

def on_publish(client, userdata, mid):
    """Callback function for when MQTT message is published."""
    logging.info("Message published successfully to MQTT broker.")

def main_job():
    """Fetches collection data, determines bins, and publishes to MQTT."""
    logging.info("Starting main job")
    logging.info(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}") # Log current time for context

    base_url = 'https://maps.poriruacity.govt.nz/server/rest/services/Property/PropertyAdminExternal/MapServer/5/query'
    params = {
        # Use the specific address format the API expects (lowercase comparison)
        'where': f"lower(address) LIKE '%{address.lower()}%'",
        'f': 'pjson', # Request JSON format
        'outFields': 'Address,OBJECTID,Collection_Day,Collection_Zone', # Specify needed fields
        'returnGeometry': 'false', # No map data needed
        'resultRecordCount': '1',  # Only need the most likely match
        'orderByFields': 'Address' # Consistent ordering
    }
    headers = {'User-Agent': 'PCCBinScraper/1.0 (github.com/wgarbutt/pccbinscraper)'} # Identify the script

    try:
        logging.info(f"Making API request for address: {address}")
        response = requests.get(base_url, params=params, headers=headers, timeout=15) # Added timeout
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        logging.info("API request successful.")

        # --- Check if the address was found ---
        if not data.get('features'):
            logging.warning(f"Address not found in PCC database: {address}")
            # Optional: Publish an error message to MQTT?
            # Example: publish_error("Address not found")
            return # Stop processing if address not found

        # --- Extract data from the first feature found ---
        # (We requested only 1 result, so features[0] should be the one)
        first_address_attributes = data['features'][0]['attributes']
        collection_day = first_address_attributes.get('Collection_Day')
        collection_zone = first_address_attributes.get('Collection_Zone')
        api_address_match = first_address_attributes.get('Address') # Log the address API matched

        # --- Validate received data ---
        if not collection_day or not collection_zone:
             logging.error(f"API response missing Collection_Day or Collection_Zone. Attributes: {first_address_attributes}")
             return # Stop if critical data is missing

        logging.info(f"API matched address: {api_address_match}")
        logging.info(f"Collection Day from API: {collection_day}")
        logging.info(f"Collection Zone from API: {collection_zone}")

        # --- Calculate next collection date and ISO week number ---
        days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        today = datetime.now()

        try:
            collection_day_index = days_of_week.index(collection_day)
        except ValueError:
            logging.error(f"Invalid collection day received from API: '{collection_day}'. Cannot calculate date.")
            return # Stop if day name is not recognized

        days_until_collection = (collection_day_index - today.weekday() + 7) % 7
        # Handle case where collection is today but script runs late
        # If days_until_collection is 0 and it's already past midday, assume next week's collection?
        # For simplicity, we'll assume if it's 0, it's today's collection or just passed.
        # Consider adding more robust logic if needed (e.g., check current time vs a cutoff time)

        next_collection_date_obj = today + timedelta(days=days_until_collection)
        # Use NZ standard format for the date string
        next_collection_str = next_collection_date_obj.strftime("%A, %d %B %Y") # e.g., Thursday, 10 April 2025

        # Get ISO week number (returns year, week, weekday) - we need week (index 1)
        iso_week = next_collection_date_obj.isocalendar()[1]
        is_even_week = (iso_week % 2 == 0)

        logging.info(f"Calculated next collection date: {next_collection_str} (ISO Week: {iso_week})")

        # --- Determine collection items based on Zone and Week ---
        collection_items = ["General Waste"] # General Waste bin is always collected

        # Apply the rules derived from the Zone 1 calendar (Odd=Recycling, Even=Glass)
        if collection_zone == 'Zone 1':
            if is_even_week:
                # EVEN weeks are GLASS for Zone 1
                collection_items.append("Glass")
                logging.info(f"Determined collection type for {collection_zone}, EVEN Week ({iso_week}): Adding Glass")
            else:
                # ODD weeks are RECYCLING for Zone 1
                collection_items.append("Recycling")
                logging.info(f"Determined collection type for {collection_zone}, ODD Week ({iso_week}): Adding Recycling")
        # Placeholder logic for Zone 2 (Assumed opposite of Zone 1)
        elif collection_zone == 'Zone 2':
            if is_even_week:
                # EVEN weeks assumed RECYCLING for Zone 2
                collection_items.append("Recycling")
                logging.info(f"Determined collection type for {collection_zone}, EVEN Week ({iso_week}): Adding Recycling")
            else:
                # ODD weeks assumed GLASS for Zone 2
                collection_items.append("Glass")
                logging.info(f"Determined collection type for {collection_zone}, ODD Week ({iso_week}): Adding Glass")
        else:
            # Handle unexpected Zone values
            logging.warning(f"Unknown or unhandled collection zone: '{collection_zone}'. Cannot determine Recycling/Glass accurately.")
            collection_items.append("Recycling/Glass Unknown") # Add a placeholder


        # --- Prepare data structure for MQTT ---
        # Using the calculated date string as the key
        bin_data_for_mqtt = {
            "Date": next_collection_str,
             # Join the list items (e.g., ["General Waste", "Recycling"]) into a string
            "Contains": ", ".join(collection_items)
        }

        # --- Connect and publish to MQTT ---
        mqtt_client = mqtt.Client(client_id=f"pcc_bin_scraper_{os.uname().nodename}") # Add hostname to client ID
        mqtt_client.username_pw_set(mqtt_username, mqtt_password)
        mqtt_client.on_connect = on_connect
        mqtt_client.on_publish = on_publish

        logging.info(f"Connecting to MQTT broker: {mqtt_broker}:{mqtt_port}")
        try:
            mqtt_client.connect(mqtt_broker, mqtt_port, 60) # 60 second keepalive
            mqtt_client.loop_start() # Start background network loop

            topic = "pcc/bin_schedule" # Consider a more descriptive topic
            json_message = json.dumps(bin_data_for_mqtt)

            logging.info(f"Publishing message to topic '{topic}': {json_message}")
            # Publish with retain=True so new subscribers get the last message
            result = mqtt_client.publish(topic, json_message, qos=1, retain=True) # Use QoS 1
            result.wait_for_publish(timeout=5) # Wait briefly for publish confirmation

            if result.is_published():
                 logging.info(f"Message successfully published (MID: {result.mid})")
            else:
                 logging.warning(f"Message publishing may have failed or timed out for MID: {result.mid}")

            mqtt_client.loop_stop() # Stop background loop
            mqtt_client.disconnect()
            logging.info("Disconnected from MQTT broker.")

        except mqtt.MQTTException as e:
             logging.error(f"MQTT Error: {str(e)}")
        except ConnectionRefusedError:
             logging.error(f"MQTT Connection Refused. Check broker address, port, and credentials.")
        except OSError as e: # Catch potential network errors during connect/publish
            logging.error(f"Network/OS Error during MQTT operation: {str(e)}")
        except Exception as e: # Catch any other unexpected errors during MQTT handling
            logging.error(f"An unexpected error occurred during MQTT operation: {str(e)}", exc_info=True)


    except requests.exceptions.Timeout:
        logging.error(f"API request timed out: {base_url}")
    except requests.exceptions.HTTPError as e:
        logging.error(f"API request failed with HTTP status code: {e.response.status_code} - {e}")
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {str(e)}")
    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON response from API. Response text was: {response.text}")
    except Exception as e:
        # Catch-all for any other unexpected errors during the job
        logging.error(f"An unexpected error occurred in main_job: {str(e)}", exc_info=True) # Log traceback

    logging.info("Main job finished")


def run_scheduler():
    """Sets up and runs the job scheduler based on RUN_FREQUENCY."""
    logging.info(f"Setting up scheduler to run '{run_frequency}'")

    # Schedule the job based on the frequency
    if run_frequency == 'hourly':
        schedule.every().hour.at(":00").do(main_job) # Schedule on the hour
        logging.info("Job scheduled to run every hour.")
    elif run_frequency == 'daily':
        # Run daily at a specific time (e.g., shortly after midnight)
        schedule.every().day.at("00:05").do(main_job) # 5 mins past midnight
        logging.info("Job scheduled to run daily at 00:05.")
    elif run_frequency == 'weekly':
        # Run weekly (e.g., Monday morning)
        schedule.every().monday.at("00:05").do(main_job)
        logging.info("Job scheduled to run every Monday at 00:05.")
    elif run_frequency == 'debug':
        # Run frequently for debugging
        schedule.every(1).minutes.do(main_job)
        logging.info("DEBUG MODE: Job scheduled to run every 1 minute.")
    else:
        # Default to daily if frequency is invalid
        logging.warning(f"Invalid run frequency: '{run_frequency}'. Defaulting to daily at 00:05.")
        schedule.every().day.at("00:05").do(main_job)

    # --- Run the job once immediately on startup ---
    logging.info("Running initial job execution...")
    main_job()
    logging.info("Initial job execution complete. Starting scheduler loop.")

    # --- Scheduler Loop ---
    while True:
        schedule.run_pending()
        time.sleep(10) # Check schedule every 10 seconds

# --- Main Execution Guard ---
if __name__ == "__main__":
    logging.info("Script starting up.")
    try:
        run_scheduler()
    except KeyboardInterrupt:
        logging.info("Script interrupted by user (Ctrl+C). Exiting.")
    except Exception as e:
        # Catch unexpected errors in the scheduler setup/loop itself
        logging.critical(f"A critical error occurred in the scheduler: {str(e)}", exc_info=True)
    finally:
        logging.info("Script finished.")
        schedule.clear() # Clear schedule on exit
