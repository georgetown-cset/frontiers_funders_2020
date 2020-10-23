# # screen
# git fetch --all
# git reset --hard
# git pull  'https://github.com/georgetown-cset/science_map'
# git reset --hard master
# python script_manager.py
#https://www.tecmint.com/fix-git-user-credentials-for-https/

#ir177$ source /Users/ir177/.virtualenvs/Documents/GitHub/science_map/bin/activate
import sqlvalidator
from datetime import date
from google.cloud import bigquery
import google.auth
import textwrap
import csv
import os
import pandas as pd
from helpers.gcs_storage import upload_blob
from helpers.functions import start_debug, add_acc_message
from helpers.BQ_dataset_update_functions import run_bq_queries

def add_acc_message(s):
    # This function create
    acc_message = '\n' +  '\n'.join(textwrap.wrap(s,120))
    print('\n'.join(textwrap.wrap(s,120)))
    with open('debug.txt', 'a') as file:
        file.write(acc_message)



credentials, project = google.auth.default(scopes=[
    'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/devstorage.full_control'])
client = bigquery.Client(project='gcp-cset-projects', credentials=credentials)



# report the total number of observation and the number distinct IDs
def bq_qc(tab, id):
    bq = bigquery.Client(project='gcp-cset-projects')
    query = f"SELECT count(*) as N, count(distinct {id}) as dist_cl_ids FROM science_map.{tab}"
    query_job = bq.query(query)
    data = query_job.result()
    rows = list(data)
    add_acc_message(f"Table {tab} has {rows[0][0]} rows and {rows[0][1]} unique {id}s")

# runs a sql querry query_dic[name] and saves results in private_ai_investment dataset.
def bq_job(bq_tname, query_dic, client, year):
    # set current data
    curdate = date.today().strftime('%Y%m%d')
    # extract querry text
    querry_text = query_dic[bq_tname]
    #The top journals run only once per year. It's the same for all variants.
        # Run querry. Save results for archive with a date and latest version in BQ and bucket
    table_ref = client.dataset('frontiers_forecasting').table(bq_tname)
    job_config = bigquery.QueryJobConfig()
    # add translation to the corpus, no new table is created in this step.
    job_config.destination=table_ref
    job_config.write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE
    query_job = client.query(querry_text,job_config=job_config,location='US')
    query_job.result()
    return query_job



# read query from file and insert variables in the text of the query
def read_sql(q, year):
    with open(f'sql/{q}.sql', 'r') as file:
        # read querry
        qtext = file.read()
        qtext = eval(f'f"""{qtext}"""')
        qtext = qtext.replace('\n', ' ')
        tab_name = q + '_' + str(year)
        return tab_name, {tab_name:qtext}

# Loop over the list of queris and process them
def eval_sql_q(qlist, year):
    query_dic = {}
    for q in qlist:
        q_name, sql_dic = read_sql(q[0], year)
        query_dic.update(sql_dic)
        # report dict: {query name: query text}
    return query_dic


def run_bq_queries(year):
# read the order or running querries
    add_acc_message("Load query order from query_list.csv")
    with open('query_order.csv',  encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        qlist = list(reader)
    # process SQL query with correct year and table version
    query_dic = eval_sql_q(qlist, year)
    # loop over SQL queries:
    for q in query_dic:
        # Run journal forecast only once per year
        add_acc_message("Read text of queries from .sql files. Create a dictionary {table name : query text}")
        query_dic = eval_sql_q(qlist, year)
        add_acc_message(f"Run query {q} for year {year}")
        # Don't save the version in the journal table name
        bq_job(q, query_dic, client, year)







if __name__ == "__main__":
    # start debuging file:
    start_debug()
    add_acc_message("Started running frontiers code.")
    # create data directories if nedeed:
    add_acc_message("Started running frontiers code.")
    add_acc_message("Updating BQ tables.")
    for year in [2014, 2016, 2019]:
        run_bq_queries(year)


