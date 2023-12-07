from os import environ
import csv
import logging
import io
import json

import requests

from simple_el_tool.extracts.base import BaseExtract

from datetime import datetime

logging.basicConfig(level=logging.INFO)

class MockarooExtract(BaseExtract):
    
    def __init__(self):
        self.url = "https://my.api.mockaroo.com/marathons_events.json"
        self.session = requests.Session()
        self.session.headers.update({
            "X-API-Key": environ.get("MOCKAROO_API_KEY"),
            
            "Content-Type": "text/html",
            "charset": "utf-8",
        })
        
        super().__init__()
        
        self.extract_time = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        
    def upload_json_to_s3(self, json_file):
        pass
        
    def extract(self, url: str = None):
        if not url:
            url = self.url
        
        try:
            r = self.session.get(url,timeout=5, )
            r.encoding = "utf-8"
            logging.info(f"Calling {url}")
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            logging.error(f"Http Error: {errh}, status: {r.status_code}")
            raise errh
        except requests.exceptions.ConnectionError as errc:
            logging.error(f"Error Connecting: {errc}, status: {r.status_code}")
            raise errc
        except requests.exceptions.Timeout as errt:
            logging.error(f"Timeout Error: {errt}, status: {r.status_code}")
            raise errt
        except requests.exceptions.RequestException as err:
            logging.error(f"OOps: Something Else {err}, status: {r.status_code}")
            raise err
        
        logging.info(f"Successfully extracted data from an API")
        return r.text
    
    def transform_csv_stream_to_json(self, data):
        csv_data = io.StringIO(data)
        reader = csv.DictReader(csv_data)
        
        output_file_name = f"{self.extract_time}.json"
        with open(output_file_name, "w", encoding="utf-8") as file:
            for item in reader:
                item.update({"upload_timestamp": int(datetime.utcnow().timestamp())})

                json_line = json.dumps(item, ensure_ascii=False)
                file.write(json_line + '\n')
        logging.info(f"Uploaded data to {output_file_name}")
        
        return output_file_name
    
    def run(self):
        extracted_data = self.extract()
        transformed_data_file_location = self.transform_csv_stream_to_json(extracted_data)
        
        self.upload_json_to_s3(transformed_data_file_location)
        

if __name__ == "__main__":
    extractor = MockarooExtract()
    extractor.run()