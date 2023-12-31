# circle2sql
Import CircleMUD .WLD files into GCP BigQuery datasets

    NEEDED RESOURCES IN THE PROJECT:
    There must be a bucket in the project with the name 'project-name-circle2sql',
    and also a BigQuery dataset with the same name; replace 'project-name' with the name of your project.
    You place the .WLD files you wish to ingest into the bucket.

Steps to run this project:

1> Create a GCP project <br />
2> Provision a Dataflow Workbench notebook and log into its Jupyter notebook <br />
3> Open its terminal and load this repo onto its local disk <br />
4> Run the init.sh script to setup the necessary GCS bucket (source) and BQ dataset (sink) <br />
    (OPTIONAL: Edit the init.sh script beforehand to modify any settings you wish to change) <br />
    NOTE:  You may be required to activate the Service Usage API before the init.sh script works properly.
5> Load the .WLD files you wish to ingest into the source GCS bucket <br />
6> Run the circle2sql.sh script <br />
7> If all goes well, you should now see the .WLD data in your BQ dataset's tables <br />
