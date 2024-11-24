import os
import json
import datetime
import pandas as pd
from sqlalchemy import create_engine, text


class SecretManager():
    """
    secret_db_id        : Determines which DB to use
    """


    def __init__(
                self,
                secret_db_id = 'talenta_company_1',
                *args,
                **kwargs
                ):

        self.secret_db_id   = secret_db_id
        self.host           = None
        self.port           = None
        self.username       = None
        self.password       = None
        self.dbname         = None


    def access_secret_by_id(self):
        TOP_LEVEL_DIR     = os.path.dirname(os.path.abspath(__file__))
        secret_file_url = os.path.abspath(f"{TOP_LEVEL_DIR}/../../config/secret.json")
        with open(secret_file_url, 'r') as file:
            data = json.load(file)

        for item in data:
            if item["secret_db_id"] == self.secret_db_id:
                self.host       = item["db_data"]["host"]
                self.port       = item["db_data"]["port"]
                self.username   = item["db_data"]["username"]
                self.password   = item["db_data"]["password"]
                self.dbname     = item["db_data"]["dbname"]
                break

        return self.host, self.port, self.username, self.password, self.dbname

