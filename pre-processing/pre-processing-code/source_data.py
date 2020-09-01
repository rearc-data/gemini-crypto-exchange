import os
import boto3
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from multiprocessing.dummy import Pool
import ssl



def data_to_s3(frmt):
    # throws error occured if there was a problem accessing data
    # otherwise downloads and uploads to s3

    source_dataset_url = 'https://www.cryptodatadownload.com/cdd/'
    if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
    getattr(ssl, '_create_unverified_context', None)):
        ssl._create_default_https_context = ssl._create_unverified_context

    try:
        response = urlopen(source_dataset_url + frmt)

    except HTTPError as e:
        raise Exception('HTTPError: ', e.code, frmt)

    except URLError as e:
        raise Exception('URLError: ', e.reason, frmt)

    else:
      #  data_set_name = os.environ['DATA_SET_NAME']
        data_set_name = 'AVtest'
        filename = data_set_name + frmt
        file_location = '/tmp/' + filename 

        with open(file_location, 'wb') as f:
            print('hi')
            f.write(response.read())
            f.close()

        # variables/resources used to upload to s3
        # s3_bucket = os.environ['S3_BUCKET']
        # new_s3_key = data_set_name + '/dataset/'
        # s3 = boto3.client('s3')

        # s3.upload_file(file_location, s3_bucket, new_s3_key + filename)

        print('Uploaded: ' + filename)

        # deletes to preserve limited space in aws lamdba
        # os.remove(file_location)

        # dicts to be used to add assets to the dataset revision
        # return {'Bucket': s3_bucket, 'Key': new_s3_key + filename}


def source_dataset():

    # list of enpoints to be used to access data included with product
    data_endpoints = [
     'Gemini_BTCUSD_d.csv',
     'Gemini_ETHUSD_d.csv',
     'Gemini_LTCUSD_d.csv',
     'Gemini_ETHBTC_d.csv',
     'Gemini_BTCUSD_d.csv',
     'gemini_ZECUSD_day.csv',
     'gemini_ZECBTC_day.csv',
     'gemini_ZECETH_day.csv',

     'gemini_BTCUSD_1hr.csv',
     'gemini_ETHUSD_1hr.csv',
     'gemini_LTCUSD_1hr.csv',
     'gemini_ETHBTC_1hr.csv',
     'gemini_ZECUSD_1hr.csv',
     'gemini_ZECBTC_1hr.csv',
     'gemini_ZECETH_1hr.csv'




    ]

    # multithreading speed up accessing data, making lambda run quicker
    with (Pool(2)) as p:
        asset_list = p.map(data_to_s3, data_endpoints)

    # asset_list is returned to be used in lamdba_handler function
    return asset_list


source_dataset()