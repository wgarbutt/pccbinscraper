Documented in my blog post https://stuffabout.cloud/posts/bin-scraper/ 

# Porirua City Council Recycling Schedule Scraper and MQTT Publisher #

I made this project as a practical outlet for everything I've learned with my Python and Docker studies.

This script queries the public API of the Porirua City Council (Wellington, New Zealand) website to find out when and what type of recycling is due, and then sends that information via MQTT.

The Python script takes these variables as input, which can be picked up from an environment file or system variables:
- `ADDRESS` (Your Address)
- `MQTT_BROKER` (IP of MQTT Server)
- `MQTT_PORT` (Typically 1883)
- `MQTT_USERNAME` (Username of MQTT Server)
- `MQTT_PASSWORD` (Password of MQTT Server)
- `RUN_FREQUENCY` (Hourly, Daily, Weekly)


I've also containerized this so the script can run on any system. The dockerfile and requirements.txt allow for the creation of the docker image.
