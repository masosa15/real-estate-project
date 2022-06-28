import boto3
import pandas as pd
endpoint = 'http://localhost:4566'


def main():
    s3 = boto3.client('s3', endpoint_url=endpoint)
    bucket='raw-data'
    #result = s3.list_buckets()['Buckets']

    response = s3.list_objects(Bucket=bucket)

    for r in response['Contents']:
        print (r)

    df = pd.read_csv( s3.download_file(Filename='scrapped_idealista_28_06_2022_17:16.csv',Bucket=bucket,Key='scrapped_idealista_28_06_2022_17:16.csv'))


    print(df)

if __name__ == '__main__':
    main()




