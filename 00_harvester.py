import requests
import json
import os
import logging
import time
import boto3
from datetime import datetime

# configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        # logging.FileHandler("harvester.log"),
        logging.StreamHandler()
    ]
)

# Get a logger instance
logger = logging.getLogger(__name__)
    

def api_request(url, lat, long, hourly):
    logger.info("Initiating API call")
    params = {
	"latitude": lat,
	"longitude": long,
    "hourly": hourly,
    "timezone": "Asia/Kolkata"
    }

    for attempt in range(1,4):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt}:\nHTTP Error fetching {url}: {e}")
            if attempt == 3:
                logger.error(f"Max attempts reached for {url}")
                return None
            time.sleep(5)
    return None

# def save_to_local(data, path, filename):
#     os.makedirs(path, exist_ok=True)
#     file_path = os.path.join(path, filename)
#     with open(file_path, "w") as f:
#         json.dump(data, f, indent=4)
#     logger.info(f"Saved data to {file_path}")

def save_to_s3(data, key, bucket="heliosgrid"):
    # aws_key = os.getenv("AWS_ACCESS_KEY")
    # aws_secret = os.getenv("AWS_SECRET_KEY")
    # s3 = boto3.client('s3', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
    s3 = boto3.client('s3')
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data),
            ContentType="application/json"
        )
        logger.info(f"Successfully uploaded to s3://{bucket}/{key}")
    except Exception as e:
        logger.error(f"Failed to upload {key} to S3: \n{e}")
    
def lambda_handler(event, context):
    coordinates = [
        ("Bhadla Solar Park, Rajasthan", 27.5384, 71.9151), 
        ("Pavagada Solar Park, Karnataka", 14.1001, 77.2798), 
        ("Charanka Solar Park, Gujarat", 23.9000, 71.2000), 
        ("Indo-Gangetic Station, Delhi NCR", 28.6139, 77.2090)]

    logger.info("Initiating harvester")

    forecast_url = "https://api.open-meteo.com/v1/forecast"
    forecast_params = ["shortwave_radiation", "direct_normal_irradiance", "diffuse_radiation", "temperature_2m", "relative_humidity_2m", "cloud_cover", "wind_speed_10m"]
    
    aq_url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    aq_params = ["aerosol_optical_depth", "pm2_5", "pm10", "dust"]

    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d_%H-%M")

    prefix = os.environ.get("VOLUME_PREFIX")
    
    for target in coordinates:
        target_area = target[0].split(", ")[-1].strip().replace(" ", "_").lower()

        forecast_json = api_request(forecast_url, target[1], target[2], forecast_params)
        if forecast_json is not None:
            # save_to_local(forecast_json, "data/forecast/", f"{target_area}_fc_{now_str}.json")
            save_to_s3(forecast_json, f"{prefix}forecast/{target_area}_fc_{now_str}.json")
        else:
            logger.error(f"No forecast data received for {target_area}")

        aq_json = api_request(aq_url, target[1], target[2], aq_params)
        if aq_json is not None:
            # save_to_local(aq_json, "data/air_quality/", f"{target_area}_aq_{now_str}.json")
            save_to_s3(aq_json, f"{prefix}air_quality/{target_area}_aq_{now_str}.json")
        else:
            logger.error(f"No AQ data received for {target_area}")
