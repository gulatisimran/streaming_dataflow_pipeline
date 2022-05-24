import logging

import apache_beam as beam
from apache_beam import pvalue

from utils.bq_schema import VOICE_BQ_SCHEMA
from utils import util


class ValidateVoiceRows(beam.DoFn):
    # This tag will be used to tag the reject rows outputs of this do_fn.
    OUTPUT_REJECT_ROWS = 'reject_rows_tag'
    OUTPUT_TARGET_ROWS = 'target_rows_tag'

    def __init__(self, timestamp_column, partition_column, past_data_threshold, post_data_threshold):
        self.timestamp_column = timestamp_column
        self.partition_column = partition_column
        self.past_data_threshold = past_data_threshold
        self.post_data_threshold = post_data_threshold

    def process(self, element):
        '''
        This method takes a dictionary, validates if it will be accepted or rejected, updates fields accordingly.
        :param element : PCollection from the create BQ row do function"

        :return: dict - element
        '''
        reject_reason = []
        ignore_cols = ['load_timestamp', 'subscribed_timestamp', 'published_timestamp']
        # Populating the reject reason column
        element, reject_reason = util.populate_reject_reason(element, reject_reason,
                                                              VOICE_BQ_SCHEMA, ignore_cols, self.past_data_threshold, self.post_data_threshold)

        # Populating the partition key
        element = util.populate_partition_key(
            element, self.timestamp_column, self.partition_column, self.past_data_threshold, self.post_data_threshold)

        if reject_reason != []:
            # populating reject reason, if list not empty.
            element['reject_reason'] = reject_reason
            # Assigning value to rejected_record and removing raw_payload because we don't need this value for
            # rejected record and it does not exist in voice_billing and voice_billing_rejected table
            element['rejected_record'] = element.pop('raw_payload')
            logging.debug(f'Reject table BQ Row: {element}')
            yield pvalue.TaggedOutput(self.OUTPUT_REJECT_ROWS, element)
        else:
            element.pop('raw_payload')
            logging.debug(f'Target table BQ Row: {element}')
            yield element
