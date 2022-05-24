from apache_beam.options.pipeline_options import PipelineOptions


class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_subscription',
                            required=True,
                            help="The Cloud Pub/Sub subscription to read from.\n"
                                 '"projects/<PROJECT_NAME>/subscriptions/<SUBSCRIPTION_NAME>".')
        parser.add_argument('--target_table',
                            required=True,
                            help="The BigQuery target table.\n"
                                 '"project-id:dataset-name.table-name".')
        parser.add_argument('--reject_table',
                            required=True,
                            help="The BigQuery reject table.\n"
                                 '"project-id:dataset-name.table-name".')
        parser.add_argument('--timestamp_column',
                            required=True,
                            help="Timestamp column from which partion column is derived")
        parser.add_argument('--partition_column',
                            required=True,
                            help="Partition column to be used in the table")
        parser.add_argument('--past_data_threshold',
                            required=False,
                            type=int,
                            help="Threshold for ingesting old data into target table in hours",
                            default=72)
        parser.add_argument('--post_data_threshold',
                            required=False,
                            help="Threshold for ingesting future data into target table in hours",
                            default=24)
        parser.add_argument('--log_level',
                            default="DEBUG",
                            choices=["DEBUG", "INFO",
                                     "WARNING", "ERROR", "CRITICAL"],
                            help="Set Log Level")
