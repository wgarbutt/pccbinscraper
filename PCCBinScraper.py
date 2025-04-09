# -*- coding: utf-8 -*-
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

# --- Configuration from Environment Variables ---
# Use a default address for testing if the ADDRESS environment variable isn't set
# Ensure this address exists in the PCC system for testing.
address = os.environ.get('ADDRESS', '168 Endeavour Drive Whitby, Porirua City 5024')
mqtt_broker = os.environ.get('MQTT_BROKER', '192.168.90.201') # Default to IP if not set
mqtt_port = int(os.environ.get('MQTT_PORT', 1883))
mqtt_username = os.environ.get('MQTT_USERNAME', 'username') # Default username
mqtt_password = os.environ.get('MQTT_PASSWORD', 'password') # Default password
run_frequency = os.environ.get('RUN_FREQUENCY', 'daily') # Default run frequency

logging.info(f"Configuration loaded. Address: {address}, MQTT Broker: {mqtt_broker}, Run Frequency: {run_frequency}")

# --- MQTT Callback Functions ---
def on_connect(client, userdata, flags, rc):
    """Callback function for when MQTT client connects."""
    if rc == 0:
        logging.info("Successfully connected to MQTT broker.")
    else:
        logging.error(f"Failed to connect to MQTT broker. Return code: {rc}")

def on_publish(client, userdata, mid):
    """Callback function for when MQTT message is published."""
    # Note: This confirms the message was sent from the client,
    # not necessarily received by the broker (especially for QoS 0).
    # For QoS 1 or 2, it's more reliable.
    logging.info(f"Message publish initiated successfully (MID: {mid})")

# --- Main Job Function ---
def main_job():
    """Fetches collection data from PCC API, determines bins based on zone/week, and publishes to MQTT."""
    logging.info("--- Starting main job ---")
    logging.info(f"Current system time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Porirua Council API endpoint for property/collection info
    base_url = 'https://maps.poriruacity.govt.nz/server/rest/services/Property/PropertyAdminExternal/MapServer/5/query'
    params = {
        'where': f"lower(address) LIKE '%{address.lower()}%'", # Case-insensitive address search
        'f': 'pjson', # Request JSON format
        'outFields': 'Address,OBJECTID,Collection_Day,Collection_Zone', # Specify fields needed
        'returnGeometry': 'false', # We don't need map coordinates
        'resultRecordCount': '1',  # Only fetch the single best match
        'orderByFields': 'Address' # Ensure consistent ordering if multiple partial matches occurred
    }
    # Identify the script making the request (good practice)
    headers = {'User-Agent': 'PCCBinScraper/1.1 (github.com/wgarbutt/pccbinscraper)'}

    try:
        # --- Step 1: Query the PCC API ---
        logging.info(f"Making API request to find collection details for address: {address}")
        response = requests.get(base_url, params=params, headers=headers, timeout=20) # Increased timeout
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        logging.info("API request successful.")

        # --- Step 2: Validate API Response and Extract Data ---
        if not data.get('features'):
            logging.error(f"Address not found in PCC API database: '{address}'. Please check the address format and ensure it's correct in the environment variable.")
            # Optional: Publish an error message to MQTT?
            return # Stop processing if address not found

        # Extract attributes from the first (and only requested) feature
        first_address_attributes = data['features'][0]['attributes']
        collection_day = first_address_attributes.get('Collection_Day')
        collection_zone = first_address_attributes.get('Collection_Zone')
        api_address_match = first_address_attributes.get('Address') # Log the address the API matched

        # Validate that we got the essential data
        if not collection_day or not collection_zone:
             logging.error(f"API response missing Collection_Day or Collection_Zone. Attributes received: {first_address_attributes}")
             return # Stop if critical data is missing

        logging.info(f"API matched address: '{api_address_match}'")
        logging.info(f"Collection Day from API: {collection_day}") # THIS is where day changes are handled
        logging.info(f"Collection Zone from API: {collection_zone}")

        # --- Step 3: Calculate Next Collection Date & ISO Week ---
        # List of days for indexing
        days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        today = datetime.now()

        try:
            # Find the index (0-6) of the collection day string received from the API
            collection_day_index = days_of_week.index(collection_day)
        except ValueError:
            # Handle case where API returns an unexpected day string
            logging.error(f"Invalid collection day string received from API: '{collection_day}'. Cannot calculate date.")
            return

        # Calculate days until the next occurrence of the collection day
        days_until_collection = (collection_day_index - today.weekday() + 7) % 7
        # Note: If days_until_collection is 0, it means the collection day is today.

        # Calculate the actual date object for the next collection
        next_collection_date_obj = today + timedelta(days=days_until_collection)
        # Format the date string for display/MQTT message (NZ format)
        next_collection_str = next_collection_date_obj.strftime("%A, %d %B %Y") # e.g., Thursday, 10 April 2025

        # Get the ISO week number tuple (year, week number, weekday)
        # We need the week number, which is at index 1
        iso_calendar = next_collection_date_obj.isocalendar()
        iso_week = iso_calendar[1]
        is_odd_week = (iso_week % 2 != 0) # Check if the ISO week number is odd

        logging.info(f"Calculated next collection date: {next_collection_str} (ISO Week: {iso_week})")

        # --- Step 4: Determine Collection Items based on Zone and Week (Corrected Logic) ---
        # Start with General Waste as it's always collected
        collection_items = ["General Waste"]

        # Apply rules based on the fetched collection_zone
        if collection_zone == 'Zone 1':
            # Logic derived from the provided Zone 1 calendar
            if is_odd_week:
                # ODD weeks have NO extra bins for Zone 1
                logging.info(f"Determined collection type for {collection_zone}, ODD Week ({iso_week}): General Waste only")
                # No other items added
            else:
                # EVEN weeks: Check if Recycling or Glass based on iso_week % 4
                if iso_week % 4 == 0:
                    # EVEN weeks where week % 4 == 0 are RECYCLING
                    collection_items.append("Recycling")
                    logging.info(f"Determined collection type for {collection_zone}, EVEN Week ({iso_week}, mod 4 = 0): Adding Recycling")
                elif iso_week % 4 == 2:
                    # EVEN weeks where week % 4 == 2 are GLASS
                    collection_items.append("Glass")
                    logging.info(f"Determined collection type for {collection_zone}, EVEN Week ({iso_week}, mod 4 = 2): Adding Glass")
                else:
                    # Fallback for unexpected even weeks (shouldn't happen in a standard year)
                    logging.warning(f"Unhandled EVEN week condition for {collection_zone}, Week {iso_week}. Defaulting to General Waste only.")

        elif collection_zone == 'Zone 2':
            # --- Placeholder for Zone 2 Logic ---
            # You would need the Zone 2 calendar to determine its specific pattern.
            # It might be the opposite of Zone 1, or shifted.
            # Example: Assume opposite (Odd=Recycling/Glass alternating, Even=GW only) - THIS IS JUST A GUESS!
            if is_odd_week:
                 # Guessing: Alternating based on week number mod 4
                 if iso_week % 4 == 1: # Example: Week 1, 5, 9... = Recycling
                     collection_items.append("Recycling")
                     logging.info(f"GUESSING collection type for {collection_zone}, ODD Week ({iso_week}, mod 4 = 1): Adding Recycling")
                 elif iso_week % 4 == 3: # Example: Week 3, 7, 11... = Glass
                     collection_items.append("Glass")
                     logging.info(f"GUESSING collection type for {collection_zone}, ODD Week ({iso_week}, mod 4 = 3): Adding Glass")
                 else: # Should not happen
                    logging.warning(f"Unhandled ODD week condition for {collection_zone}, Week {iso_week}. Defaulting to General Waste only.")
            else:
                 # Guessing: EVEN weeks are General Waste only for Zone 2
                 logging.info(f"GUESSING collection type for {collection_zone}, EVEN Week ({iso_week}): General Waste only")
            logging.warning(f"Using placeholder logic for {collection_zone}. Verify with official Zone 2 calendar.")

        else:
            # Handle unexpected Zone values returned by the API
            logging.error(f"Unknown or unhandled collection zone received from API: '{collection_zone}'. Cannot determine recycling/glass type accurately.")
            collection_items.append("Recycling/Glass Unknown") # Add a placeholder


        # --- Step 5: Prepare MQTT Message ---
        # Create the dictionary payload for the JSON message
        bin_data_for_mqtt = {
            "Date": next_collection_str, # The calculated date string
            # Join the list of items (e.g., ["General Waste", "Recycling"]) into a comma-separated string
            "Contains": ", ".join(collection_items)
        }

        # --- Step 6: Connect and Publish to MQTT ---
        # Use a unique client ID, potentially including hostname
        mqtt_client = mqtt.Client(client_id=f"pcc_bin_scraper_{os.uname().nodename}")
        mqtt_client.username_pw_set(mqtt_username, mqtt_password)
        # Assign callback functions
        mqtt_client.on_connect = on_connect
        mqtt_client.on_publish = on_publish

        logging.info(f"Connecting to MQTT broker: {mqtt_broker}:{mqtt_port}")
        try:
            mqtt_client.connect(mqtt_broker, mqtt_port, keepalive=60) # 60 second keepalive
            # Start a background thread to handle network traffic (publish/receive)
            mqtt_client.loop_start()

            # Define the MQTT topic
            topic = "pcc/bin_schedule" # Use a clear topic structure
            # Convert the dictionary to a JSON string
            json_message = json.dumps(bin_data_for_mqtt)

            logging.info(f"Publishing message to topic '{topic}': {json_message}")
            # Publish the message with QoS 1 (at least once delivery) and retain flag set
            # Retain=True means the broker keeps the last message for new subscribers
            result = mqtt_client.publish(topic, json_message, qos=1, retain=True)

            # Wait for the publish acknowledgement (for QoS 1)
            # Removed timeout=5 as it caused issues with older library versions
            result.wait_for_publish()

            # Check if the publish was successful (based on acknowledgement)
            if result.is_published():
                 logging.info(f"Message publish acknowledgement received (MID: {result.mid})")
            else:
                 # This might indicate a problem if QoS=1 was used
                 logging.warning(f"Message publishing confirmation NOT received (MID: {result.mid})")

            # Stop the background network loop
            mqtt_client.loop_stop()
            # Disconnect gracefully
            mqtt_client.disconnect()
            logging.info("Disconnected from MQTT broker.")

        # --- Refined MQTT Exception Handling ---
        except ConnectionRefusedError:
             logging.error(f"MQTT Connection Refused. Check broker address ({mqtt_broker}:{mqtt_port}) and credentials.")
        except OSError as e: # Catch potential network errors (e.g., host unreachable)
            logging.error(f"Network/OS Error during MQTT operation: {str(e)}")
        except Exception as e: # Catch other potential errors from paho-mqtt or MQTT logic
             # Catching general Exception here as mqtt.MQTTException caused issues
             logging.error(f"MQTT Operation Error: {str(e)}", exc_info=True) # Log traceback for unexpected MQTT errors


    # --- General Error Handling for the Job ---
    except requests.exceptions.Timeout:
        logging.error(f"API request timed out connecting to {base_url}. Check network or increase timeout.")
    except requests.exceptions.HTTPError as e:
        logging.error(f"API request failed with HTTP status code: {e.response.status_code} - {e}. Check API endpoint or parameters.")
    except requests.exceptions.RequestException as e:
        # Catch other requests-related errors (DNS, connection, etc.)
        logging.error(f"API request failed: {str(e)}")
    except json.JSONDecodeError:
        # Handle cases where the API response is not valid JSON
        logging.error(f"Failed to decode JSON response from API. Response text was: {response.text}")
    except Exception as e:
        # Catch-all for any other unexpected errors during the job execution
        logging.error(f"An unexpected error occurred in main_job: {str(e)}", exc_info=True) # Log full traceback

    logging.info("--- Main job finished ---")


# --- Scheduler Function ---
def run_scheduler():
    """Sets up and runs the job scheduler based on RUN_FREQUENCY environment variable."""
    logging.info(f"Setting up scheduler to run '{run_frequency}'")

    # Schedule the job based on the configured frequency
    if run_frequency == 'hourly':
        schedule.every().hour.at(":05").do(main_job) # Schedule 5 mins past the hour
        logging.info("Job scheduled to run hourly at 5 minutes past the hour.")
    elif run_frequency == 'daily':
        schedule.every().day.at("00:05").do(main_job) # Run daily at 5 mins past midnight
        logging.info("Job scheduled to run daily at 00:05.")
    elif run_frequency == 'weekly':
        schedule.every().monday.at("00:05").do(main_job) # Run weekly on Monday morning
        logging.info("Job scheduled to run every Monday at 00:05.")
    elif run_frequency == 'debug':
        schedule.every(1).minutes.do(main_job) # Run every minute for debugging
        logging.info("DEBUG MODE: Job scheduled to run every 1 minute.")
    else:
        # Default to daily if frequency setting is invalid
        logging.warning(f"Invalid run frequency: '{run_frequency}'. Defaulting to daily at 00:05.")
        schedule.every().day.at("00:05").do(main_job)

    # --- Run the job once immediately on startup ---
    logging.info("Running initial job execution on startup...")
    main_job()
    logging.info("Initial job execution complete. Starting scheduler main loop.")

    # --- Scheduler Main Loop ---
    while True:
        # Check if any scheduled jobs are pending execution
        schedule.run_pending()
        # Wait for a short interval before checking again
        time.sleep(30) # Check every 30 seconds

# --- Main Execution Guard ---
if __name__ == "__main__":
    logging.info("Script starting up...")
    try:
        # Start the scheduler function
        run_scheduler()
    except KeyboardInterrupt:
        # Handle graceful shutdown on Ctrl+C
        logging.info("Script interrupted by user (Ctrl+C). Exiting gracefully.")
    except Exception as e:
        # Catch unexpected errors during scheduler setup or loop itself
        logging.critical(f"A critical error occurred in the main execution/scheduler loop: {str(e)}", exc_info=True)
    finally:
        logging.info("Script shutting down.")
        schedule.clear() # Clear any pending scheduled jobs
