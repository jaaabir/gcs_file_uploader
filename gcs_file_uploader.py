from google.cloud import storage
from google.oauth2 import service_account
import json
import os

class GcsFileUploader:
    def __init__(self, bucket_key_path : str, bucket_name : str = None, region : str = 'us', verbose = False) -> None:
        credentials = service_account.Credentials.from_service_account_file(bucket_key_path)
        self.project_id = self.read_json(bucket_key_path)['project_id']
        self.BUCKET_NAME = self.project_id if not bucket_name else bucket_name
        self.region = region
        self.verbose = verbose

        self.gcs = storage.Client(credentials = credentials, project = self.project_id)
        self.validate_bucket_name()

        if self.BUCKET_NAME:
            self.bucket = self.gcs.bucket(bucket_name = self.BUCKET_NAME)

    def upload_file(self, local_fname : str, destination_fname : str = None, class_index : int = -1, path_split_delimiter : str = '/') -> None:
        
        if destination_fname is None:
            destination_fname = self.get_destination_fname(local_fname, class_index, path_split_delimiter)
        
        if not self.validate_file(destination_fname):
            blob = self.bucket.blob(blob_name = destination_fname)
            blob.upload_from_filename(local_fname)
            if self.verbose:
                print(f'Uploaded the file to gs://{self.BUCKET_NAME}/{destination_fname}')
        else:
            print(f'{destination_fname} is already exits in the bucket {self.BUCKET_NAME}')

    def delete_file(self, filename : str, bypass_password : bool = False) -> None:
        if not bypass_password:
            password = input('Enter the bucket name : ')
            if password == self.BUCKET_NAME:
                bypass_password = True

        if bypass_password:
            if self.validate_file(filename):
                blob = self.bucket.blob(filename)
                blob.delete()

    def delete_files(self, filenames : list[str]) -> None:
        password = input('Enter the bucket name : ')
        allow_deletion = False
        if password == self.BUCKET_NAME:
            allow_deletion = True
        
        if allow_deletion:
            for filename in filenames:
                self.delete_file(filename, bypass_password = allow_deletion)

    def validate_bucket_name(self) -> None:
        if self.gcs.lookup_bucket(self.BUCKET_NAME):
            print(f'BUCKET : {self.BUCKET_NAME} exists ...')
        else:
            print(f'BUCKET : {self.BUCKET_NAME} is not found ...')
            create_bucket_choice = input('Creat new bucket (y/n) : ')
            create_bucket_choice = True if create_bucket_choice.lower()[0] == 'y' else False
            if create_bucket_choice:
                print('Creating new bucket ...')
                print(f'[INFO] : Leaving empty would name it {self.project_id if self.BUCKET_NAME is None else self.BUCKET_NAME}')
                new_bucket_name = input('Bucker Name : ')
                self.create_new_bucket(new_bucket_name)
            else:
                self.BUCKET_NAME = None
                print('')

    def validate_file(self, fname : str) -> bool:
        return storage.Blob(bucket = self.bucket, name = fname).exists(self.gcs)
    
    def create_new_bucket(self, bucket_name : str) -> None:
        self.gcs.create_bucket(bucket_name, project = self.project_id, location = self.region)
        self.BUCKET_NAME = bucket_name
        print(f'New Bucket : {self.BUCKET_NAME} created ...')

    ### utils ###
    @staticmethod
    def read_json(fpath : str) -> dict:
        with open(fpath, 'r') as jfile:
            data = json.load(jfile)
        return data
    
    @staticmethod
    def get_destination_fname(local_fname : str, class_index : int, path_split_delimiter : str) -> str:
        filename = os.path.basename(local_fname)
        class_name = os.path.dirname(local_fname).split(path_split_delimiter)[class_index]
        destination_fname : str = GcsFileUploader.merge_root_fname(class_name, filename)
        return destination_fname
    
    @staticmethod
    def merge_root_fname(root_name : str, filename : str) -> str:
        return f'{root_name}/{filename}'
    