import requests
from bs4 import BeautifulSoup as soup
import logging
import pandas as pd
from io import StringIO
import boto3
import datetime

headers = {
    'User-Agent': 'Mozilla/5.0 (Linux; Android 10; SM-A205U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.53 Mobile Safari/537.36'
}

def Logger(filename):
    logging.basicConfig(filename=filename,
                        filemode="a",
                        format= "%(asctime)s, %(msecs)d %(name)s | %(levelname)s | [ %(filename)s-%(module)s-%(lineno)d ]  : %(message)s",
                        datefmt="%d %H:%M:%S")

    return logging.getLogger()

def get_page(url):
        response = requests.get(url, headers=headers)

        if not response.ok:  # response.ok -> response.status_code == 200
            log = Logger('../../errorLog.log')
            log.info(msg=f"The {url} return a different error code: {response.status_code}")
            print(f"The {url} return a different error code: {response.status_code}")
            return None

        return soup(response.text, "html.parser")

def upload_to_s3(results, filename):
    df = pd.DataFrame(results)

    localstack_endpoint = 'http://localhost:4566/' #localstack S3 endpoint
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    today = datetime.datetime.now()
    date_time = today.strftime("%d_%m_%Y_%H:%M")

    filename = filename + date_time + '.csv'

    s3 = boto3.resource('s3', endpoint_url=localstack_endpoint)
    bucket = s3.Bucket('raw-data')
    bucket.put_object(Key=filename, Body=csv_buffer.getvalue())

