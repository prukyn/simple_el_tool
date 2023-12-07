import unittest
from os import remove
from pathlib import Path

from google.api_core.exceptions import NotFound as GoogleNotFoundException
from google.cloud import bigquery, storage

from simple_el_tool.extracts.simple_extract import MockarooExtract


class TestMockarooExtract(unittest.TestCase):
    
    def setUp(self) -> None:
        self.extractor = MockarooExtract()

    def test_extract(self):
        content = self.extractor.extract()
        self.assertGreater(len(content), 1)

    def test_transform_csv_stream_to_json(self):
        content = self.extractor.extract()
        output_file_location = self.extractor.transform_csv_stream_to_json(content)
        
        self.assertTrue(Path(output_file_location).exists())
        
        with open(output_file_location, 'r', encoding='utf-8') as file:
            self.assertGreater(len(list(file.readlines())), 1)
        
        self.extractor.clean(output_file_location)
        self.assertTrue(not Path(output_file_location).exists())

    def test_upload_json_to_s3(self):
        file_name = "test_file.txt"
        with open(file_name, "w") as file:
            file.write(" ")
        
        self.extractor.upload_json_to_s3(file_name)
        
        self.extractor.clean(file_name)
        
        client = storage.Client(self.extractor._project_name)
        bucket = client.bucket(self.extractor._bucket_name)
        
        
        self.assertTrue(bucket.get_blob(file_name))
        if bucket.get_blob(file_name):
            bucket.delete_blob(file_name)
        
        self.assertIsNone(bucket.get_blob(file_name))
    
    def test_cleaning(self):
        file_name = "test_file.txt"
        with open(file_name, "w") as file:
            file.write(" ")
        
        remove(file_name)
        self.assertTrue(not Path(file_name).exists())
    
    def test_upload_to_bq(self):
        extracted_data = self.extractor.extract()
        transformed_data_file_location = self.extractor.transform_csv_stream_to_json(extracted_data)
        
        s3_location = self.extractor.upload_json_to_s3(transformed_data_file_location)
        self.extractor.clean(transformed_data_file_location)
        
        self.extractor.upload_to_bq(s3_location, table="test_table")
        
        client = bigquery.Client()
        
        table_name = "test_table"
        table_id = f"{self.extractor._project_name}.raw.{table_name}"
        
        self.assertTrue(client.get_table(table_id))
        
        client.delete_table(table_id)
        
        with self.assertRaises(GoogleNotFoundException):
            client.get_table(table_id)
        
    
    
    
    
            

if __name__ == '__main__':
    unittest.main()