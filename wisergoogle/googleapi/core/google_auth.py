import csv
import io
import json
import os

import google_auth_oauthlib.flow
import pandas as pd
import psycopg2
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

file_ids = [
    "First file ID",
    "Second file ID",
    "Third file ID"
]
file_names = [
    "First file name.json",
    "Second file name.json",
    "Third file name.json"
]


class Auth:
    def __init__(self, client_secret_filename, scopes):
        self.client_secret = client_secret_filename
        self.scopes = scopes
        self.flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
            self.client_secret, self.scopes)
        self.flow.redirect_uri = 'http://localhost:8080/'
        self.creds = None
        self.credentials = self.get_credentials()
        self.download_to_csv()
        self.to_database()
        self.insert_database()

    def get_credentials(self):
        flow = InstalledAppFlow.from_client_secrets_file(self.client_secret,
                                                         self.scopes)
        self.creds = flow.run_local_server(port=8080)
        return self.creds

    def download_to_csv(self):
        drive_service = build('drive', 'v3', credentials=self.credentials)
        for file_id, file_name in zip(file_ids, file_names):
            request = drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print("Download %d%%." % int(status.progress() * 100))
            fh.seek(0)
            with open(os.path.join(file_name), 'wb') as f:
                f.write(fh.read())
                f.close()

        for file_name in file_names:
            with open(f"{file_name}") as file_read:  # noqa
                data = json.load(file_read)
                dt = data.get("_default")
                df = pd.DataFrame.from_dict(dt, dtype=str, orient="index")
                name = file_name[:-5]
                df = df.head(300)
                df.to_csv(f"{name}.csv", index=True)

    @staticmethod
    def to_database():
        conn = psycopg2.connect(
            "host=localhost dbname=renan user=renan password=renan")
        cur = conn.cursor()
        create_table = """CREATE TABLE wiser(
        _id SERIAL PRIMARY KEY,
        tipoDocumento VARCHAR(256) NOT NULL,
        facet_tipoDocumento VARCHAR(256) NOT NULL,
        data TEXT NOT NULL,
        urn VARCHAR(256) NOT NULL,
        url VARCHAR(256) NOT NULL,
        localidade VARCHAR(256),
        facet_localidade VARCHAR(256),
        autoridade VARCHAR(256),
        facet_autoridade VARCHAR(256),
        title VARCHAR(256) NOT NULL,
        description TEXT,
        type VARCHAR(256) NOT NULL,
        identifier integer NOT NULL
        )
        """
        cur.execute(create_table)
        conn.commit()

    @staticmethod
    def insert_database():
        file_names = [
            "acervo_1556_1899.csv",
            "acervo_1900_1979.csv",
            "acervo_1980_1989.csv"
        ]
        conn = psycopg2.connect(
            "host=localhost dbname=renan user=renan password=renan")
        cur = conn.cursor()
        for file_name in file_names:
            with open(file_name, "r", encoding="utf8") as f:
                reader = csv.reader(f)
                next(reader)
                for row in reader:
                    cur.execute(
                        "INSERT INTO wiser VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        row
                    )
            conn.commit()


SCOPES = "https://www.googleapis.com/auth/drive.readonly"
CLIENT_SECRET_FILE = "credentials.json"
Auth(client_secret_filename=CLIENT_SECRET_FILE, scopes=SCOPES)
