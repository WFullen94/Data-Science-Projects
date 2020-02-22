import requests
import numpy as np
import pandas as pd
import json
import xml.etree.ElementTree as ET
from lxml import etree
import re
import boto3
import os

class IRS990Parser():

    def __init__(self, file_990, bucket_name):
        self.file_990 = file_990
        self.df_filings = None
        # self.df_990_details = None
        self.bucket_name = bucket_name
        self.num_batches = None

    def parse_file_to_df(self):
        req = requests.get(self.file_990)
        filings = json.loads(req.text)
        self.df_filings = pd.DataFrame(filings["Filings2018"])
        self.num_batches = int(self.df_filings.shape[0] / 1000)
        return self.df_filings

    def upload_file_to_s3(self, file_name, bucket, object_name=None):

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        # Upload the file
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def parse_990_data(self, url_column):
        columns = []
        data = []

        print(self.num_batches)

        batches = np.array_split(self.df_filings,
                                 self.num_batches)

        batch_index = 1

        for batch in batches:
            for index, row in batch.iterrows():

                if index % 100 == 0:
                    print("Processed " + str(index) + " records.")

                details_990 = row[url_column]

                # Send out the get request for the XML
                data_990_req = requests.get(details_990)
                data_990_req.encoding = "utf-8"
                data_990 = str(data_990_req.text)
                data_990 = re.sub(r'\sxmlns="[^"]+"',
                                  '',
                                  data_990,
                                  count=1)

                e = ET.ElementTree(ET.fromstring(data_990))

                filing = {}
                for elt in e.iter():
                    filing[elt.tag] = elt.text

                data.append(filing)

            df = pd.DataFrame(data)

            df.rename(columns=lambda x: x.strip())
            df.drop(columns=['Return', 'ReturnHeader'], inplace=True)

            # Write the df to a temporary data location
            temp_990_source = "/tmp/parsed_irs_990_"+str(batch_index)+".csv"
            temp_990_s3 = "parsed_irs_990_"+str(batch_index)+".csv"

            df.to_csv(temp_990_source, index=False)
            self.upload_file_to_s3(temp_990_source,
                                   self.bucket_name,
                                   temp_990_s3)

            # Remove the file once it's uploaded and empty data_frame
            os.remove(temp_990_source)
            df = []
            data = []

            # Iterate batch_index
            batch_index=batch_index+1
            if batch_index % 10 == 0:
                print("Processed " + str(batch_index) + " batches")

if __name__ == "__main__":
    index = "https://s3.amazonaws.com/irs-form-990/index_2018.json"
    url_col = "URL"
    bucket_name = "parsed-irs-990"

    xml_parser = IRS990Parser(index, bucket_name)
    irs_data = xml_parser.parse_file_to_df()

    irs_990_details = xml_parser.parse_990_data(url_col)
