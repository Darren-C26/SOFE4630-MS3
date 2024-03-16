import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from beam_nuggets.io import relational_db

class FilterMissingMeasurements(beam.DoFn):
    def process(self, element):
        if all(val is not None for val in element.values()):
            yield element

class ConvertMeasurements(beam.DoFn):
    def process(self, element):
        element['Pressure_psi'] = element['pressure'] / 6.895
        element['Temperature_F'] = element['temperature'] * 1.8 + 32
        yield element

def format_for_mysql(element):
    return {
        'Pressure_psi': element['Pressure_psi'],
        'Temperature_F': element['Temperature_F']
    }

def run(argv=None):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--input', dest='input', required=True, help='Input Pub/Sub topic to read from.')
    parser.add_argument('--output', dest='output', required=True, help='Output Pub/Sub topic to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    source_config = relational_db.SourceConfiguration(
        drivername='mysql+pymysql',
        host='35.203.68.56',
        port=3306,
        username='usr',
        password='sofe4630u',
        database='Readings'
    )

    table_config = relational_db.TableConfiguration(
        name='SMART_METER_READINGS',
        create_if_missing=True,
        primary_key_columns=[]
    )

    with beam.Pipeline(options=pipeline_options) as p:
        # Read from Pub/Sub
        measurements = (p | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(topic=known_args.input)
                         | 'Parse JSON' >> beam.Map(lambda x: json.loads(x)))

        # Filter out records with missing measurements
        filtered_measurements = measurements | 'Filter missing measurements' >> beam.ParDo(FilterMissingMeasurements())

        # Convert measurements
        converted_measurements = filtered_measurements | 'Convert measurements' >> beam.ParDo(ConvertMeasurements())

        # Write to MySQL
        converted_measurements | 'Write to MySQL' >> relational_db.Write(
            source_config=source_config,
            table_config=table_config
        )

        # Write back to Pub/Sub
        (converted_measurements | 'Convert to JSON' >> beam.Map(lambda x: json.dumps(x))
                                | 'Write to Pub/Sub' >> beam.io.WriteToPubSub(topic=known_args.output))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
