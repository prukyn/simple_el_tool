import csv
import io
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import requests
from google.api_core.exceptions import NotFound as GoogleNotFoundException
from google.cloud import bigquery, storage

from simple_el_tool.extracts.base import BaseExtract

logging.basicConfig(level=logging.INFO)

class MockarooExtract(BaseExtract):
    
    def __init__(self):
        self.url = "https://my.api.mockaroo.com/marathons_events.json"
        self.session = requests.Session()
        self.session.headers.update({
            "X-API-Key": os.environ.get("MOCKAROO_API_KEY"),
            
            "Content-Type": "text/html",
            "charset": "utf-8",
        })
        
        self._storabe_bucket_name = "mockaroo_data"
        self._project_name = os.environ.get("GOOGLE_CLOUD_PROJECT_NAME")
        
        super().__init__()
        
        self.extract_time = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    
    def clean(self, file_name):
        if Path(file_name).exists():
            os.remove(file_name)
            logging.info("Successfully removed file from disk")
    
    def upload_to_bq(self, s3_location):
        client = bigquery.Client()
        
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("upload_timestamp", "INT64"),
                bigquery.SchemaField("event_name", "STRING"),
                bigquery.SchemaField("gender", "STRING"),
                bigquery.SchemaField("phone_number", "STRING"),
                bigquery.SchemaField("email", "STRING"),
                bigquery.SchemaField("age", "INT64"),
                bigquery.SchemaField("webinar_title", "STRING"),
                bigquery.SchemaField("registration_date", "INT64"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("id", "STRING"),
            ],
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        
        uri = f"gs://{s3_location}"
        table_id = f"{self._project_name}.raw.mockaroo"
        
        dataset = bigquery.Dataset(".".join(table_id.split(".")[:-1]))
        dataset.location = "europe-north1"
        
        client.create_dataset(
            dataset,
            exists_ok=True,   
        )
        
        load_job = client.load_table_from_uri(
            uri,
            destination=table_id,
            job_config=job_config,
        )
        
        load_job.result()
    
    def upload_json_to_s3(self, json_file):
        client = storage.Client(self._project_name)
        bucket = client.bucket(self._storabe_bucket_name)
        
        try:
            bucket.exists()
        except GoogleNotFoundException:
            bucket.create(location="europe-north1")

        blob = bucket.blob(json_file)
        
        blob.upload_from_filename(json_file, if_generation_match=0)
        
        logging.info(f"Successfully uploaded {json_file} to the Cloud Storage")
        
        return f"{self._storabe_bucket_name}/{json_file}"
        
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
        
        s3_location = self.upload_json_to_s3(transformed_data_file_location)
        self.clean(transformed_data_file_location)
        
        self.upload_to_bq(s3_location)

if __name__ == "__main__":
    extractor = MockarooExtract()
    extractor.run()