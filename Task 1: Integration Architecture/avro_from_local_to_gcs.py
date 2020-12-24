from apache_beam.io import ReadFromAvro
from apache_beam.io import WriteToAvro
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from avro.datafile import DataFileReader
from avro.io import DatumReader
import argparse
import pprint


input_file = "data/test-dataset.avro"
output_file = "gs://big-data-training-julio/test-dataset-2.avro"


def get_schema(file_name):
	reader = DataFileReader(open(file_name, "rb"), DatumReader())
	schema = reader.datum_reader.writer_schema
	return schema.to_json()
	

def get_args(argv=None):
	parser = argparse.ArgumentParser()
	parser.add_argument('--input',
                        dest='input',
                        default=input_file,
                        help='Input file to process.')
	parser.add_argument('--output',
                        dest='output',
                        default=output_file,
                        help='Output file to write results to.')
	return parser.parse_known_args(argv)



if __name__ == "__main__":
	# Method 1:
	# with open("data/test-dataset.avro", "rb") as f:
	# 	avro_file = f.read()
	
	# gcs_client = beam.io.gcp.gcsio.GcsIO()
	# with gcs_client.open("gs://big-data-training-julio/test-dataset.avro", mode='w', mime_type='application/x-avro') as buffer:
	# 	buffer.write(avro_file)


	# Method 2:
	known_args, pipeline_args = get_args()
	pipeline_args.extend([
        # Your project ID is required in order to run your pipeline on
        # the Google Cloud Dataflow Service.
        '--project=Big Data Training',
        # Your Google Cloud Storage path is required for staging local
        # files.
        '--staging_location=gs://big-data-training-julio/staging',
        # Your Google Cloud Storage path is required for temporary
        # files.
        '--temp_location=gs://big-data-training-julio/temp',
        '--job_name=parse-avro',
    ])
	pipeline_options = PipelineOptions(pipeline_args)

	# Debug
	# pp = pprint.PrettyPrinter(indent=4)
	# pp.pprint(get_schema(input_file))
	
	with Pipeline(options=pipeline_options) as p:
		(
			p
			| 'Read Avro' >> ReadFromAvro(known_args.input)
			| 'Write Avro to GCS' >> WriteToAvro(known_args.output, schema=get_schema(input_file))
		)
