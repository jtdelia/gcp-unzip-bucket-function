## GCS bucket unzip function
## Use with GCP Cloud Function & storage trigger set on an ingest bucket. 
## Once triggered, will unzip contents to a second bucket under a folder with
## the name of the original zip file.
##
import io
import os
import sys
from google.cloud import storage
from zipfile import ZipFile
from zipfile import is_zipfile


def zip_processing(event, context):

    ### Useful metadata
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))


    ### Variables for the source/destination and zip file.
    zip_source_bucket=event['bucket']
    zip_destination_bucket="DESTINATION-BUCKET-HERE"   #<----CHANGE THIS BUCKET NAME to where resulting files from unzip should go
    zip_filename=event['name']
    
    ## For the logs
    print('Event ID: {}'.format(context.event_id)) 
    print('Event type: {}'.format(context.event_type))
    print('Source Bucket: {}'.format(zip_source_bucket))
    print('Destination Bucket: {}'.format(zip_destination_bucket))
    print('Zip Filename: {}'.format(zip_filename))


    ### Unzip the source file in the destination bucket
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(zip_source_bucket)
    destination_bucket = storage_client.get_bucket(zip_destination_bucket)
    
    # Unzip folder desintation on the target bucket, defaults to zip file name
    folder = "/" + os.path.splitext(zip_filename)[0] + "/"
    
    blob = bucket.blob(zip_filename)
    zipbytes = io.BytesIO(blob.download_as_string())

    if is_zipfile(zipbytes):
        with ZipFile(zipbytes, 'r') as myzip:
            for contentfilename in myzip.namelist():
                contentfile = myzip.read(contentfilename)
                blob = destination_bucket.blob(folder+contentfilename)
                blob.upload_from_string(contentfile)
                print('Unziped and uploaded: {}'.format(contentfilename))

    print('Unzip completed')

    ###Clean up zip files
    bucket = storage_client.get_bucket(zip_source_bucket)
    blob = bucket.blob(zip_filename)
    blob.delete()
    print('Function completed.')
