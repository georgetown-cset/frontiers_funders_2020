#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/ir177/Documents/ID/GCP-CSET-Projects-49aa9c25f835_all_admin.json"
from helpers.gcs_storage import list_blobs, delete_blob, download_blob, upload_blob
from google.cloud import bigquery
import textwrap
import pandas as pd
import numpy as np
# import as pip install python-igraph
import igraph as ig
import csv
import os
from datetime import date
import multiprocessing as mp
def add_acc_message(s):
    acc_message = '\n' +  '\n'.join(textwrap.wrap(s,120))
    print('\n'.join(textwrap.wrap(s,120)))
    with open('debug.txt', 'a') as file:
        file.write(acc_message)

# create directory data
def create_data_dir():
    add_acc_message("Create directories")
    dir_list = ['data/input','data/text']
    for p in dir_list:
        try:
            os.makedirs(p)
        except OSError:
            print(f"Directory {p} exists")
        else:
            print(f"Created the {p} directory %s")

# delete data directory
def del_data_dir():
    path = "data"
    try:
        os.rmdir(path)
    except OSError:
        print(f"Deletion of the  data directory {path} failed")
    else:
        print(f"Successfully deleted the {path} directory")


# delete old debugging and start new
def start_debug():
    try:
        os.rmdir('debug.txt')
    except OSError:
        print(f"Start new debugging file.")
    else:
        print(f"Old debugging file is deleted. Started new debugging file")


# Load blob in GCP storage and save as BQ table
def bq_load_csv_in_gcs(dataset, table, link_to_blob):
    bigquery_client = bigquery.Client()
    project = 'gcp-cset-projects'
    table_id = f'{project}.{dataset}.{table}'
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job_config.autodetect = True
    job_config.skip_leading_rows = 1
    load_job = bigquery_client.load_table_from_uri(link_to_blob, table_id, job_config=job_config)
    assert load_job.job_type == 'load'
    load_job.result()  # Waits for table load to complete.
    assert load_job.state == 'DONE'
    add_acc_message(f"Copied blob {link_to_blob} to BQ table {dataset}.{table}")
    return


# add infomration for debugging
def add_acc_message(s):
    # This function create
    acc_message = '\n' +  '\n'.join(textwrap.wrap(s,120))
    print('\n'.join(textwrap.wrap(s,120)))
    with open('debug.txt', 'a') as file:
        file.write(acc_message)




def BQstorageQ(name):
    client = bigquery.Client()
    dellist = list_blobs('science_clustering', name)
    for h in range(0,len(dellist)):
        delete_blob('science_clustering', dellist[h])
        # export as JSON new line delimited
    job_config = bigquery.job.ExtractJobConfig( destination_format="CSV")
    table_ref = client.dataset("science_mapering").table(f"{name}")
    job_config.destination = table_ref
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    # Export table to GCS
    destination_uri = f"gs://science_clustering//input/{name}_*.csv"
    dataset_ref = client.dataset("science_map", project="gcp-cset-projects")
    table_ref = dataset_ref.table(f"{name}")
    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location='US', job_config=job_config)
    extract_job.result()  # Waits for job to complete
# extract a list of objects with a prefix




def bq_download(t):
    # create dict
    tab_dic = {t : f"select * from `gcp-cset-projects.science_map.{t}`"}
    file_list = {t : t + '.csv'}
    # The line above runs a BQ, comment if you need a fast run.
    print(f"Running querry for {t}")
    BQstorageQ(t)
    filelist = list_blobs('science_clustering', 'input/'+ t)
    print(filelist)
    print("Downloading filelist")
    for j in range(0,len(filelist)):
        download_blob('science_clustering', filelist[j], 'data/' + filelist[j])
    all_filenames = [w for w in filelist]
    # combine all files in the list
    print("Reading filelist")
    combined = pd.concat([pd.read_csv('data/'+f) for f in all_filenames])
    # delete downloaded files
    print('finished reading')
    print("saving combines")
    combined.to_csv('data/text/'+file_list[t], index=False)
