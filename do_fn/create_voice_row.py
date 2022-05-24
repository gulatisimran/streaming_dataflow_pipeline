import copy
import logging

import apache_beam as beam
from utils.bq_schema import VOICE_BQ_ROW


class CreateVoiceRow(beam.DoFn):

    def process(self, element):
        logging.debug(f'Creating bigquery row from {element}')
        # Use BigQuery schema and fill out with values if any
        bq_row = copy.deepcopy(VOICE_BQ_ROW)
        payload = element.get('payload')
        # iterate over the payload
        for k in payload:
            if k in bq_row:
                bq_row[k] = payload.get(k)

        bq_row['load_timestamp'] = 'AUTO'
        # added raw_payload in bq_row for getting this data on preprocess_element.py when record rejected by any reason
        bq_row['raw_payload'] = element.get('raw_payload')
        bq_row['published_timestamp'] = element.get('published_timestamp')
        bq_row['subscribed_timestamp'] = element.get('subscribed_timestamp')
        bq_row['published_message_id'] = element.get('published_message_id')

        yield bq_row
