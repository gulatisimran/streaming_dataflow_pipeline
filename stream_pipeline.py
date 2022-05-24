import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

from do_fn.create_voice_row import CreateVoiceRow
from do_fn.validate_voice_row import ValidateVoiceRows
from do_fn.pre_process_element import PreProcessElement
from p_transforms.read_from_pubsub import ReadFromPubSub
from p_transforms.write_to_bq import WriteToBigQuery
from utils.user_options import UserOptions


def run():
    # `save_main_session` is set to true because some do_fn's rely on
    # globally imported modules.

    pipeline_options = PipelineOptions()
    options = pipeline_options.view_as(UserOptions)

    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = True

    pipeline = beam.Pipeline(options=options)

    logging.getLogger().setLevel(getattr(logging, options.log_level))
    logging.debug('Starting Streaming ETL Pipeline')

    # When with_attributes = True,
    # it is to read the custom pubsub message including attributes and message_id and pubslish timestamp
    pubsub_messages = pipeline | 'Read PubSub Subscription' >> ReadFromPubSub(
        options.input_subscription, with_attributes=True)


    # pass with_attributes = True, when custom pubsub message with attributes is read from pubsub
    parsed, unparsable = pubsub_messages | 'Preprocess messages' >> beam.ParDo(
        PreProcessElement(partition_col=options.partition_column, with_attributes=True)) \
        .with_outputs(PreProcessElement.UNPARSABLE_ROW, main=PreProcessElement.PARSED_ROW)

    bq_rows = parsed | 'Create BigQuery Row' >> beam.ParDo(CreateVoiceRow())

    # Validating BiqQuery Rows
    accept_rows, reject_rows = bq_rows | 'Validate BigQuery Rows' >> beam.ParDo(
        ValidateVoiceRows(options.timestamp_column, options.partition_column, options.past_data_threshold, options.post_data_threshold)).with_outputs(
        ValidateVoiceRows.OUTPUT_REJECT_ROWS,
        main=ValidateVoiceRows.OUTPUT_TARGET_ROWS)

    accept_rows | 'Write to target table' >> WriteToBigQuery(
        options.target_table)

    unparsable | 'Write unparsable to reject table' >> WriteToBigQuery(
        options.reject_table)
    reject_rows | 'Write to reject table' >> WriteToBigQuery(
        options.reject_table)

    if 'direct' in options.view_as(StandardOptions).runner.lower():
        pipeline.run().wait_until_finish()
    else:
        pipeline.run()


if __name__ == "__main__":
    run()
