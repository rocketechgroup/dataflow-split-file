import argparse
import json
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

message_types = []


class UnknownMessageError(ValueError):
    pass


def partition_by_message_type(element, number_of_partitions):
    for index, message_type in enumerate(message_types):
        if message_type['unique_field_name'] in element:
            return index
    raise UnknownMessageError('message type cannot be identified')


def filter_by_known_message_types(element):
    for index, message_type in enumerate(message_types):
        if message_type['unique_field_name'] in element:
            return True

    return False


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input file to process.')

    parser.add_argument(
        '--schema_identifiers',
        dest='schema_identifiers',
        required=True,
        help='Schema identifiers using a unique column belongs to a schema. '
             'Format: [unique field name]|[partition key 2]^[unique field name 2]|[partition key 2]^...')

    parser.add_argument(
        '--output_bigquery_dataset',
        dest='output_bigquery_dataset',
        required=True,
        help='Output BigQuery ProjectId:DatasetId')
    known_args, pipeline_args = parser.parse_known_args(argv)

    for schema_identifier in known_args.schema_identifiers.split('^'):
        unique_field_name, partition_key = schema_identifier.split('|')
        message_types.append({'partition_key': partition_key, 'unique_field_name': unique_field_name})

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as pipeline:
        partitioned_messages = (
                pipeline
                | 'Reading Mixed JSONL' >> ReadFromText(known_args.input)
                | 'Filter by know message types' >> beam.Filter(filter_by_known_message_types)
                | 'Convert to dict' >> beam.Map(lambda x: json.loads(x))
                | 'Partition by message type' >> beam.Partition(partition_by_message_type, len(message_types))
        )
        for index, messages in enumerate(partitioned_messages):
            messages | f'{index}' >> WriteToBigQuery(
                table=f"{known_args.output_bigquery_dataset}.{message_types[index]['partition_key']}",
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_NEVER
            )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
