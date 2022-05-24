import datetime
import json
import logging
import uuid
import math

import apache_beam as beam
from apache_beam import pvalue

from utils.constants import JSON_ERROR
from utils.constants import TIMESTAMP_FORMAT


class PreProcessElement(beam.DoFn):
    UNPARSABLE_ROW = 'unparsable_row'
    PARSED_ROW = 'parsed_row'

    # recursive_flag is set default to 'true'. except delivery_receipt it
    def __init__(self, partition_col, recursive_flag=False, with_attributes=False):
        self.partition_col = partition_col
        self.recursive_flag = recursive_flag
        self.with_attributes = with_attributes

    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        try:
            # if with_attributes is false-> element is a byte-string, message record will be defined with this logic
            if not self.with_attributes:
                timestamp_utc = datetime.datetime.utcfromtimestamp(
                    float(timestamp))
                published_timestamp = timestamp_utc.strftime(TIMESTAMP_FORMAT)
                subscribed_timestamp = datetime.datetime.utcnow().strftime(TIMESTAMP_FORMAT)
                published_message_id = str(uuid.uuid4())
                message_size = math.ceil(
                    int(len(element.decode('utf-8'))) / 1024)
                record = {
                    'published_timestamp': published_timestamp,
                    'subscribed_timestamp': subscribed_timestamp,
                    'published_message_id': published_message_id,
                    'size': message_size,
                    'payload': element.decode('utf-8'),
                    # Decoding the byte message to string here, will be loaded into json based on the recursive flag
                    'raw_payload': element.decode('utf-8')
                }

            else:
                # ELSE if with_attributes is True: the element is a dict object
                record = element.copy()
                # raw string value of pubsub message data
                record['raw_payload'] = element['payload']

            
            
            element_decoded = record['payload']
            json_loads_result = json.loads(element_decoded)

            logging.debug(
                f"--element decoded utf-8 {element_decoded} , Type : {type(element_decoded)}")
            logging.debug(
                f"-json loads= {json_loads_result} , Type: {type(json_loads_result)}")

            record['payload'] = json.loads(record['payload'])
            yield record
        except json.decoder.JSONDecodeError:
            rejected_record = \
                {
                    'rejected_record': record['payload'],
                    'load_timestamp': 'AUTO',
                    'published_timestamp': record['published_timestamp'],
                    'subscribed_timestamp': record['subscribed_timestamp'],
                    self.partition_col: str(datetime.datetime.utcnow().date()),
                    'reject_reason': [{'description': JSON_ERROR}],
                    'published_message_id': record['published_message_id']
                }
            yield pvalue.TaggedOutput(self.UNPARSABLE_ROW, rejected_record)
