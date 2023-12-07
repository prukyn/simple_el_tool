
class BaseExtract:
    def __init__(self):
        pass
    
    def extract(self):
        raise NotImplementedError

    def upload_json_to_s3(self, json_file):
        pass