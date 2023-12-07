import unittest
from pathlib import Path

from simple_el_tool.extracts.simple_extract import MockarooExtract


class TestStringMethods(unittest.TestCase):
    
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

if __name__ == '__main__':
    unittest.main()