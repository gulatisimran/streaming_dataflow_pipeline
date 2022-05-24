import apache_beam as beam


class WriteToBigQuery(beam.PTransform):
    def __init__(self, table_name):
        self.table_name = table_name

    def expand(self, pcoll):
        pcoll | 'Write to table' >> beam.io.WriteToBigQuery(
            table=self.table_name,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )
