from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime

import pandas as pd
import sentencepiece as spm

import os
import argparse
import glob
import yaml

ap = argparse.ArgumentParser()
ap.add_argument('--env', type=str, default='development', help='production | development')
args = ap.parse_args()

load_dotenv(f'./.env.{args.env}')

class MongoDBController:
    def __init__(self, host, port, username, password, db_name=None):
        self.client = MongoClient(host, port=port, username=username, password=password, connect=False)
        if db_name:
            self.db = self.client[db_name]
        else:
            self.db = None

        self.collection = None
        
    def create_database(self, db_name):
        self.db = self.client[db_name]

        return self.db

    def create_collection(self, collection_name):
        # create collection if it does not exist, else retrieve
        self.collection = self.db[collection_name]

        return self.collection

    def insert_many(self, data_dict):
        response = self.collection.insert_many(data_dict)

        return response.inserted_ids
        
    def insert_one(self, data):
        response = self.collection.insert_one(data)

        return response.inserted_id
        
    def find_all(self):
        return pd.DataFrame(list(self.collection.find()))

    def delete_many(self, query):
        response = self.collection.delete_many(query)
        return response.deleted_count
       


if __name__ == '__main__':
    db_name = f'{args.env}_prompts'
    mongodb = MongoDBController(
        host=os.environ['mongodb_url'],
        port=int(os.environ['mongodb_port']), 
        username=os.environ['mongodb_user'], 
        password=os.environ['mongodb_password'],
        db_name=db_name
    )

    sp = spm.SentencePieceProcessor(model_file=os.environ['tokenizer'])

    prompt_folders = sorted(glob.glob('prompts/*'))
    for folder in prompt_folders:
        company = os.path.split(folder)[-1]
        mongodb.create_collection(company)
        df = mongodb.find_all()

        prompt_files = sorted(glob.glob(os.path.join(folder, '*')))
        for prompt_file in prompt_files:
            with open(prompt_file, 'r') as f:
                prompt = yaml.safe_load(f)
                prefix = prompt['prefix']
                suffix = prompt['suffix']

            if df.shape[0] > 0:
                if prefix in df['prefix'].tolist() and suffix in df['suffix'].tolist():
                    continue

            prefix_token_count = len(sp.encode(prefix))
            suffix_token_count = len(sp.encode(suffix))

            input_id = mongodb.insert_one({
                'prefix': prefix,
                'suffix': suffix,
                'prefix_token_count': prefix_token_count,
                'suffix_token_count': suffix_token_count,
                'date': datetime.now()
            })

            print(f'[*] Inserted {prompt_file} to {db_name}.{company}: {input_id}')