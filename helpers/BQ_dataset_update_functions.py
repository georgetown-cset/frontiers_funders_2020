import sqlvalidator
from datetime import date
from google.cloud import bigquery
import google.auth
import textwrap
import csv
import os
#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/ir177/Documents/ID/GCP-CSET Projects-49aa9c25f835 all admin.json'
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

# copy BQ tables:
def bq_copy_table(source_dataset, source_table, dest_dataset, dest_table, client):
    source_table_id = f'gcp-cset-projects.{source_dataset}.{source_table}'
    dest_table_id = f'gcp-cset-projects.{dest_dataset}.{dest_table}'
    # delete destinqtion table:
    client.delete_table(dest_table_id, not_found_ok=True)
    # copy source table to destination
    job = client.copy_table(source_table_id, dest_table_id)
    job.result()  # Wait for the job to complete.
    add_acc_message(f'Copied and replaced table {source_dataset}.{source_table} to {dest_dataset}.{dest_table}')

def get_child_descriptions(schema):
    if schema.fields is None:
        return None
    desc = {}
    for child in schema.fields:
        desc[child.name] = {
            "description": child.description,
            "child_descriptions": get_child_descriptions(child)
        }
    return desc

def map_fields(schema_fields, desc_map):
    if schema_fields is None:
        return None
    new_fields = []
    for schema in schema_fields:
        desc = schema.description if schema.name not in desc_map else desc_map[schema.name]["description"]
        fields = map_fields(schema.fields, {} if schema.name not in desc_map else desc_map[schema.name]["child_descriptions"])
        new_fields.append(bigquery.SchemaField(schema.name, schema.field_type, mode=schema.mode,
                fields=fields, policy_tags=schema.policy_tags, description=desc))
    return new_fields

def bq_copy_labels(source_dataset, source_table, dest_dataset, dest_table, client):
    source_table_id = f'gcp-cset-projects.{source_dataset}.{source_table}'
    dest_table_id = f'gcp-cset-projects.{dest_dataset}.{dest_table}'
    # get source table
    table_s = client.get_table(source_table_id)
    col_labels = {}
    # copy description
    table_desc = table_s.description
    # copy source column descriptions to a dictionary
    for schema_elt in table_s.schema:
        col_labels[schema_elt.name] = {
            "description": schema_elt.description,
            "child_descriptions": get_child_descriptions(schema_elt)
        }
    #get destination table
    table_d = client.get_table(dest_table_id)
    # save new column descriptions
    new_schema = []
    for schema_elt in table_d.schema:
        desc = schema_elt.description if schema_elt.name not in col_labels else col_labels[schema_elt.name]["description"]
        fields = map_fields(schema_elt.fields,
                        {} if schema_elt.name not in col_labels else col_labels[schema_elt.name]["child_descriptions"])
        new_schema.append(bigquery.SchemaField(schema_elt.name, schema_elt.field_type, mode=schema_elt.mode, 
                fields=fields, policy_tags=schema_elt.policy_tags, description=desc))
    table_d.schema = new_schema
    # save new table description
    table_d.description = table_desc
    # update the destination table
    client.update_table(table_d, ["schema", "description"])

# runs a sql querry query_dic[name] and saves results in private_ai_investment dataset.
def bq_job(name, query_dic, client, year):
    # set current data
    # extract querry text
    querry_text = query_dic[name]
    for bq_tname in [f'{name}_latest']:
        # Run querry. Save results for archive with a date and latest version in BQ and bucket
            table_ref = client.dataset("science_map").table(bq_tname)
            job_config = bigquery.QueryJobConfig()
            if name != 'add_trans_to_corp':
                job_config.destination=table_ref
                job_config.write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE
            query_job = client.query(querry_text,job_config=job_config,location="US")
            query_job.result()
            # check if Querry has error
            if query_job.errors != None:
                add_acc_message(f"Querry {name} produced errors: \r\n {query_job.errors}")
    # report the number of observations in a table and the number of unique cluster_ids. Don't for tables without
    # cluster IDs
    # Unique IDs are article id
    if name in ['dc5_need_to_translate', 'article_dates']:
        bq_qc(f'{name}_latest', 'merged_id')
    elif name in ['dc5_cit_links_clusters_top20', 'dc5_cit_links_clusters_top100']:
        bq_qc(f'{name}_latest', 'dc5_id')
        bq_qc(f'{name}_latest', 'dc5_id')
    elif name != 'add_trans_to_corp':
        bq_qc(f'{name}_latest', 'cluster_id')
    elif name == 'add_trans_to_corp':
        print('IDs are counted in the table: add_trans_to_corp')
    else:
        print('Error of bq_job. Check the table name')

def run_bq_queries(year):
# read the order or running querries
    add_acc_message("Load query order from query_order.csv")
    with open('query_order.csv',  encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        qlist = list(reader)
# for each querry create a dic:
    query_dic = {}
# look over the querres
    add_acc_message("Read text of queries from .sql files. Create a dictionary {table name : query text}")
    for q in qlist:
# extract the string from list. This would be name of table in science_map
        with open(f'sql/{q[0]}.sql', 'r') as file:
            # read querry
            qtext = file.read().replace('\n', ' ')
        query_dic.update({q[0]:qtext})
        # alert about the clustering solution that is used:
        if q[0] == 'core_clusters_leiden':
            add_acc_message(f'Core science clusters are read from {qtext}. Check that the table reference is correct.')
        add_acc_message(f'Running querry {q[0]}')
        # run the querry
        bq_job(q[0], query_dic, client)


# assign a stable version and copy labels from _latest version

def assign_stable_version(date, dataset):
    # read the order or running querries
    add_acc_message(f"Start assigning stable version to {date}. Load query order from query_order.csv")
    with open('query_order.csv', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        qlist = list(reader)
    # loop through the tables
    for t in qlist:
        print(t[0])
        if t[0] != 'dc5_need_to_translate' and t[0] != 'add_trans_to_corp':
            #copy table to stable
            bq_copy_table(dataset, t[0] + f'_{date}', dataset, t[0] + '_stable', client)
            #copy label from latest table to stable table
            bq_copy_labels(dataset, t[0] + '_latest', dataset, t[0] + '_stable', client)
        # copy the dated table and rename it with postfix '_latest'




    # print(qlist)






# read from querry list
# rename table
# copy table description
# copy column names




# from google.cloud import bigquery
# client = bigquery.Client()
# table_ref = client.dataset('my_dataset').table('my_table')
# table = client.get_table(table_ref)  # API request

# assert table.description == "Original description."
# table.description = "Updated description."
#
# table = client.update_table(table, ["description"])  # API request
#
# assert table.description == "Updated description."

# from google.cloud import bigquery
#
# # Construct a BigQuery client object.
# client = bigquery.Client()
#
# # TODO(developer): Set dataset_id to the ID of the dataset to fetch.
# # dataset_id = "your-project.your_dataset"
#
# dataset = client.get_dataset(dataset_id)  # Make an API request.
# dataset.labels = {"color": "green"}
# dataset = client.update_dataset(dataset, ["labels"])  # Make an API request.
#
# print("Labels added to {}".format(dataset_id))
