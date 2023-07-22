import json
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import EdgeOptions
from bs4 import BeautifulSoup
import paho.mqtt.client as mqtt


# Address to search for
address = "YOUR ADDRESS"  #Enter your address here

# Function to handle connection to MQTT broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker.")
    else:
        print("Failed to connect to MQTT broker. Error code:", rc)

# Function to handle publishing messages
def on_publish(client, userdata, mid):
    print("Message published.")

# Function to get the current week number
def get_current_week():
    return datetime.now().isocalendar()[1]

# URL of the recycling calendar webpage
url = 'https://poriruacity.govt.nz/services/rubbish-and-recycling/recycling-calendar/'



# Initialize the Edge WebDriver with options
options = EdgeOptions()
#options.add_argument("--headless")  # Optional: Run the WebDriver in headless mode (without a visible browser window)
options.add_argument("--guest")  # Add the --guest option
driver = webdriver.Edge(options=options)

# Open the webpage
driver.get(url)

# Find the input field for the address by class name
input_element = driver.find_element(By.CLASS_NAME, 'vue-dropdown-input')
input_element.send_keys(address)

# Wait for the dropdown suggestions to appear (wait for a few seconds)
time.sleep(2)

# Find the address suggestion div element and click on it
try:
    suggestion = driver.find_element(By.XPATH, "//div[@class='vue-dropdown-content']//div[@class='vue-dropdown-item']")
    suggestion.click()
except Exception as e:
    print("Error handling dropdown:", e)

# Wait for the page to load after selecting the suggestion (adjust the sleep time if needed)
time.sleep(2)

# Extract the page source after selecting the address suggestion
page_source = driver.page_source

# Close the browser
driver.quit()

# Parse the page source using BeautifulSoup
soup = BeautifulSoup(page_source, 'html.parser')

# Find all div elements with class 'col-sm-2 col-xs-3' that contain the recycling items
recycling_items = soup.find_all("div", class_="col-sm-2 col-xs-3")

# Create a dictionary to store the recycling items grouped by date
recycling_data = {}

# Get the current day of the week
current_day = datetime.now().weekday()

# Determine the target week based on the current day of the week
if current_day < 3:  # If it's before Thursday (Monday, Tuesday, Wednesday)
    target_week = get_current_week()
else:  # If it's Thursday or later (Thursday, Friday, Saturday, Sunday)
    target_week = get_current_week() + 1

# Loop through each recycling item and extract the date and keyword
for item in recycling_items:
    date_element = item.find_previous("h5", class_="pcc-recycle-date")
    date_str = date_element.get_text(strip=True) if date_element else "Date not found"

    # Convert the date string to a datetime object
    date = datetime.strptime(date_str, "%A, %d %B %Y")

    # Check if the date is within the target week
    if date.isocalendar()[1] == target_week:
        keyword_element = item.find("p", class_="pcc-recycle-label")
        keyword = keyword_element.get_text(strip=True) if keyword_element else "Keyword not found"

        # Append the keyword to the list of keywords for the corresponding date in the dictionary
        if date_str in recycling_data:
            recycling_data[date_str].append(keyword)
        else:
            recycling_data[date_str] = [keyword]

# Initialize the MQTT client
mqtt_client = mqtt.Client()

# MQTT broker credentials
mqtt_broker = "IP OF MQTT SERVER"
mqtt_port = 1883
mqtt_username = "username"
mqtt_password = "password"

# Set the MQTT username and password
mqtt_client.username_pw_set(mqtt_username, mqtt_password)

# Set the callback functions
mqtt_client.on_connect = on_connect
mqtt_client.on_publish = on_publish

# Connect to the MQTT broker
print("Connecting to MQTT broker...")
try:
    mqtt_client.connect(mqtt_broker, mqtt_port, 60)
except Exception as e:
    print("Error connecting to MQTT broker:", e)
    exit()

# Start the MQTT network loop
mqtt_client.loop_start()

# Publish the grouped recycling items to MQTT as JSON
for date, keywords in recycling_data.items():
    keyword_list = ", ".join(keywords)
    message = {
        "Date": date,
        "Contains": keyword_list
    }
    topic = "recycling_schedule"
    json_message = json.dumps(message)  # Convert the dictionary to a JSON string
    print(f"Publishing message: {json_message} to topic: {topic}")
    try:
        mqtt_client.publish(topic, json_message, retain=True)
    except Exception as e:
        print("Error publishing message to MQTT:", e)

# Stop the MQTT network loop
mqtt_client.loop_stop()

# Disconnect from the MQTT broker
mqtt_client.disconnect()
print("Disconnected from MQTT broker.")
