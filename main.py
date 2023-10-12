import os
import argparse
import time
import logging
import json
# import typing
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import DataflowRunner, DirectRunner

'''
    NEEDED RESOURCES IN THE PROJECT:
    There must be a bucket in the project with the name 'project-name-circle2sql',
    and also a BigQuery dataset with the same name; replace 'project-name' with the name of your project.
    You place the .WLD files you wish to ingest into the bucket.
'''

# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load .WLD data from files in a GCS bucket into tables in BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--staging_location', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--temp_location', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--input_path', required=True, help='Path to *.WLD files')
    parser.add_argument('--locations_table_name', required=True, help='BigQuery table name for locations')
    parser.add_argument('--exits_table_name', required=True, help='BigQuery table name for exits')
    parser.add_argument('--elements_table_name', required=True, help='BigQuery table name for elements')

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(save_main_session=True)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.staging_location
    options.view_as(GoogleCloudOptions).temp_location = opts.temp_location
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('batch-user-traffic-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    input_path = opts.input_path
    temp_location = opts.temp_location
    locations_table_name = opts.locations_table_name
    exits_table_name = opts.exits_table_name
    elements_table_name = opts.elements_table_name


    def capture_command_output(command):
        stream = os.popen(command)
        output = stream.read().strip()
        return output

    project_ID = capture_command_output("gcloud config get project")

    class SplitTables(beam.DoFn):
        def __init__(self):
            pass

        def process(self, path_to_file):
            from apache_beam import pvalue
            #for word in text.split(self.delimiter):
            #    yield word
            state = 0
            #   0: read location ID if it starts with # character, otherwise if $ EoF character then we're done
            #   1: read location name
            #   2: check whether to add a line to the location description, or end the description.
            #   3: read the 3 extra location variables
            #   4: check whether it we're going to read an exit record, an examinable element, or if we're ending the location record
            #   5: check whether to add a line to the exit description, or end the description.
            #   6: read the exit keywords
            #   7: read the 3 extra exit variables
            #   8: read the examinable elements keywords
            #   9: check whether to add a line to the element description, or end the description.

            def blank_location():
                return {
                    'ID': 0,
                    'name': "",
                    'description': "",
                    'zone_number': 0,
                    'room_bitvector': "",
                    'sector_type': 0
                }

            def blank_exit():
                return {
                    'location_ID': 0,
                    'direction': 0,
                    'description': "",
                    'keyword_list': "",
                    'door_flag': 0,
                    'key_ID': 0,
                    'destination_ID': 0
                }

            def blank_element():
                return {
                    'location_ID': 0,
                    'keyword_list': "",
                    'description': ""
                }

            location_dict = blank_location()
            exit_dict = blank_exit()
            element_dict = blank_element()

            locations_txt = ""
            exits_txt = ""
            elements_txt = ""

            locations_list = []
            exits_list = []
            elements_list = []

            # --we replace this with fetching the contents of the file directly from Beam
            #with open("gs://+" project_ID + path_to_file) as f:
            #    contents = f.readlines()
            # path_to_file = bytes(path_to_file, 'utf-8')
            contents = path_to_file.split("\n")

            for t in contents:
                t = t.strip()
                if t == "":
                    t = "\n"
                # print(str(state) + " -- " + str(len(t)) + " -- " + t.strip())
                #match state:
                #   0: read location ID if it starts with # character, otherwise if $ EoF character then we're done
                #case 0:
                if state == 0:
                    if t[0] == "$":
                        break
                    if t[0] == "#":
                        print(t)
                        location_dict['ID'] = int(t[1:])
                        state = 1
                #   1: read location name
                elif state == 1:
                    if t[-1] == '~':
                        location_dict['name'] = t[0:-1]
                        state = 2
                #   2: check whether to add a line to the location description, or end the description.
                elif state == 2:
                    if t == '~':
                        state = 3
                    else:
                        location_dict['description'] = location_dict['description'] + t + "\n"
                #   3: read the 3 extra location variables
                elif state == 3:
                    tmp = t.split(' ')
                    location_dict['zone_number'] = int(tmp[0])
                    location_dict['room_bitvector'] = tmp[1]
                    location_dict['sector_type'] = int(tmp[2])
                    state = 4
                #   4: check whether it we're going to read an exit record, an examinable element, or if we're ending the location record
                elif state == 4:
                    if t[0] == "D":
                        exit_dict['location_ID'] = location_dict['ID']
                        exit_dict['direction'] = t[1]
                        state = 5
                    elif t[0] == "E":
                        element_dict['location_ID'] = location_dict['ID']
                        state = 8
                    elif t[0] == "S":
                        locations_txt = locations_txt + json.dumps(location_dict) + "\n"
                        #locations_list.append(location_dict.copy())
                        yield pvalue.TaggedOutput('locations', location_dict.copy())
                        location_dict = blank_location()
                        state = 0
                #   5: check whether to add a line to the exit description, or end the description.
                elif state == 5:
                    if t == '~':
                        state = 6
                    else:
                        exit_dict['description'] = exit_dict['description'] + t + "\n"
                #   6: read the exit keywords
                elif state == 6:
                    if t[-1] == '~':
                        exit_dict['keyword_list'] = t[0:-1]
                        state = 7
                #   7: read the 3 extra exit variables
                elif state == 7:
                    # print("-- " + t)
                    tmp = t.split(' ')
                    exit_dict['door_flag'] = int(tmp[0])
                    exit_dict['key_ID'] = int(tmp[1])
                    exit_dict['destination_ID'] = int(tmp[2])
                    exits_txt = exits_txt + json.dumps(exit_dict) + "\n"
                    #exits_list.append(exit_dict.copy())
                    yield pvalue.TaggedOutput('exits', exit_dict.copy())
                    exit_dict = blank_exit()
                    state = 4
                #   8: read the examinable elements keywords
                elif state == 8:
                    if t[-1] == '~':
                        element_dict['keyword_list'] = t[0:-1]
                        state = 9
                #   9: check whether to add a line to the element description, or end the description.
                elif state == 9:
                    if t == '~':
                        elements_txt = elements_txt + json.dumps(element_dict) + "\n"
                        #elements_list.append(element_dict.copy())
                        yield pvalue.TaggedOutput('elements', element_dict.copy())
                        element_dict = blank_element()
                        state = 4
                    else:
                        element_dict['description'] = element_dict['description'] + t + "\n"

    # locations_table_name = project_ID + ":circle2sql.locations"
    locations_table_schema = {
        "fields": [
            {
                "name": "ID",
                "type": "INTEGER"
            },
            {
                "name": "name",
                "type": "STRING"
            },
            {
                "name": "description",
                "type": "STRING"
            },
            {
                "name": "zone_number",
                "type": "INTEGER"
            },
            {
                "name": "room_bitvector",
                "type": "STRING"
            },
            {
                "name": "sector_type",
                "type": "INTEGER"
            }
        ]
    }

    # exits_table_name = project_ID + ":circle2sql.exits"
    exits_table_schema = {
        "fields": [
            {
                "name": "location_ID",
                "type": "INTEGER"
            },
            {
                "name": "direction",
                "type": "STRING"
            },
            {
                "name": "description",
                "type": "STRING"
            },
            {
                "name": "keyword_list",
                "type": "STRING"
            },
            {
                "name": "door_flag",
                "type": "INTEGER"
            },
            {
                "name": "key_ID",
                "type": "INTEGER"
            },
            {
                "name": "destination_ID",
                "type": "INTEGER"
            }
        ]
    }

    # elements_table_name = project_ID + ":circle2sql.elements"
    elements_table_schema = {
        "fields": [
            {
                "name": "location_ID",
                "type": "INTEGER"
            },
            {
                "name": "keyword_list",
                "type": "STRING"
            },
            {
                "name": "description",
                "type": "STRING"
            }
        ]
    }


    with beam.Pipeline(options=options) as pipeline:
        locations_PC, exits_PC, elements_PC = (
            pipeline
            | 'Load all files' >> beam.io.textio.ReadFromText(input_path + '/*.wld', delimiter = bytes("$", 'utf8'))
            | 'Split into tables' >> beam.ParDo(SplitTables()).with_outputs(
                'locations',
                'exits',
                'elements')
        )

        (
            locations_PC
            | 'output locations' >> beam.io.WriteToBigQuery(
                locations_table_name,
                schema=locations_table_schema,
                custom_gcs_temp_location =temp_location,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
                
        )

        (
            exits_PC
            | 'output exits' >> beam.io.WriteToBigQuery(
                exits_table_name,
                schema=exits_table_schema,
                custom_gcs_temp_location =temp_location,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

        (
            elements_PC
            | 'output elements' >> beam.io.WriteToBigQuery(
                elements_table_name,
                schema=elements_table_schema,
                custom_gcs_temp_location =temp_location,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

        logging.getLogger().setLevel(logging.INFO)
        logging.info("Building pipeline ...")

        pipeline.run()

if __name__ == '__main__':
    run()
