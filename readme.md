Documented in my blog post https://stuffabout.cloud/posts/bin-scraper/ 

# Recycling Schedule Scraper and MQTT Publisher #
This code is a Python script that scrapes a recycling calendar webpage, extracts the recycling items for a specific address, groups them by date, and publishes the grouped information to an MQTT broker.

## Required Dependencies ##
* `json`: Used for JSON serialization and deserialization.
* `time`: Provides functions for time-related operations like waiting.
* `datetime`,`timedelta`: Enables working with dates and times.
* `selenium`: A web automation tool used for web scraping.
* `bs4 (BeautifulSoup)`: A library for parsing HTML and XML data.
* `paho.mqtt.client`: A client library for MQTT communication.
  
## Configuration ##
Set the `address` variable to the desired address for recycling schedule lookup.
Set the `url` variable to the URL of the recycling calendar webpage.
Configure the `EdgeOptions` according to your requirements (e.g., headless mode).

## Functions ##
* `on_connect(client, userdata, flags, rc)`: Callback function executed when connecting to the MQTT broker. It prints a success or failure message based on the connection result.
* `on_publish(client, userdata, mid)`: Callback function executed after publishing a message to the MQTT broker. It prints a message published notification.
* `get_current_week()`: Returns the current week number using the 
* `datetime.now().isocalendar()` function.

## Execution Steps ##
Initialize the Edge WebDriver with options specified in `EdgeOptions`.

Open the recycling calendar webpage using the WebDriver.

Find the input field for the address by class name and enter the desired address.

Wait for the dropdown suggestions to appear.

Find the address suggestion div element and click on it.

Wait for the page to load after selecting the suggestion.

Extract the HTML source code of the page.

Close the WebDriver.

Parse the HTML source code using BeautifulSoup.

Find all div elements with class `col-sm-2 col-xs-3` that contain recycling items.

Create a dictionary to store the recycling items grouped by date.

Determine the target week based on the current day of the week.

Loop through each recycling item and extract the date and keyword.

Append the keyword to the list of keywords for the corresponding date in the dictionary.

Initialize the MQTT client.

Set the MQTT broker credentials (broker IP, port, username, and password).

Set the callback functions for connection and message publishing.

Connect to the MQTT broker.

Start the MQTT network loop.

Publish the grouped recycling items to MQTT as JSON messages.

Stop the MQTT network loop.

Disconnect from the MQTT broker.

## Example Usage ##
Before running the script, make sure to install the required dependencies (`selenium`,`paho-mqtt`,`beautifulsoup4`) using pip:
``` python 
pip install selenium paho-mqtt beautifulsoup4
```

After installing the dependencies, you can execute the Python script to scrape the recycling schedule webpage, extract the information for a specific address, and publish it to an MQTT broker.