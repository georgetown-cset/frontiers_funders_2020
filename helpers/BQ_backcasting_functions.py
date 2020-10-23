import sqlvalidator
from datetime import date
from google.cloud import bigquery
import google.auth
import textwrap
import csv
import os
import pandas as pd
from helpers.gcs_storage import upload_blob

if os.path.exists('/Users/ir177/Documents/ID/GCP-CSET Projects-49aa9c25f835 all admin.json'):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/ir177/Documents/ID/GCP-CSET Projects-49aa9c25f835 all admin.json'
else:
    print("ilya's google application credentials not found, attempting to read from GOOGLE_APPLICATION_CREDENTIALS")

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
def bq_job(name, query_dic, client, v):
    # set current data
    curdate = date.today().strftime("%Y%m%d")
    # extract querry text
    querry_text = query_dic[name]
    print(name[:-5])
    #The top journals run only once per year. It's the same for all variants.
    if name[:-5] != 'forecast_250J':
        name = name + f'_var{v}'
    # create BQ tables with postfix 'latest'
    for bq_tname in [f'{name}_latest']:
        # Run querry. Save results for archive with a date and latest version in BQ and bucket
            table_ref = client.dataset("september_20_clustering_experiments").table(bq_tname)
            job_config = bigquery.QueryJobConfig()
            # add translation to the corpus, no new table is created in this step.
            if name != 'add_trans_to_corp':
                job_config.destination=table_ref
                job_config.write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE
            query_job = client.query(querry_text,job_config=job_config,location="US")
            query_job.result()
            # check if Query has error
            if query_job.errors != None:
                add_acc_message(f"Querry {name} produced errors: \r\n {query_job.errors}")
            return query_job



# read query from file and insert variables in the text of the query
def read_sql(q, first_year, for_year, fut_year, n_for_rest, v):
    with open(f'sql/backcasting/{q}.sql', 'r') as file:
        # read querry
        qtext = file.read()
        qtext = eval(f'f"""{qtext}"""')
        qtext = qtext.replace('\n', ' ')
        tab_name = q + '_' + str(for_year)
        return tab_name, {tab_name:qtext}

# Loop over the list of queris and process them
def eval_sql_q(qlist, first_year, for_year, fut_year, n_for_rest, v):
    query_dic = {}
    for q in qlist:
        q_name, sql_dic = read_sql(q[0], first_year, for_year, fut_year, n_for_rest, v)
        query_dic.update(sql_dic)
        # report dict: {query name: query text}
    return query_dic


def run_bq_queries(first_year, for_year, fut_year, n_for_rest):
# read the order or running querries
    add_acc_message("Load query order from forecasting_qlist.csv")
    with open('forecasting_qlist.csv',  encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        qlist = list(reader)
    for v in range(1,10):
        # process SQL query with correct year and table version
        query_dic = eval_sql_q(qlist, first_year, for_year, fut_year, n_for_rest, v)
        # loop over SQL queries:
        for q in query_dic:
            # Run journal forecast only once per year
            if (q == 'forecast_250J' and v == 1) | (q != 'forecast_250J'):
                add_acc_message("Read text of queries from .sql files. Create a dictionary {table name : query text}")
                query_dic = eval_sql_q(qlist, first_year, for_year, fut_year, n_for_rest, v)
                add_acc_message(f"Run query {q} for year {for_year} and map variant {v}")
                # Don't save the version in the journal table name
                bq_job(q, query_dic, client, v)






# Loop through the years of backasting data:
# n_for_rest minimum number of papers in forecasting year in a cluster. All clusters with less papers will be dropped */
def get_three_year_forecasts(start_loop, end_loop, first_year, n_for_rest):
    # back_cast year s
    for s in range(start_loop, end_loop+1):
        # backcast 3 years in advance
        end_yr = s + 3
        # loop through 10 maps version in a year:
        run_bq_queries(first_year, s, end_yr, n_for_rest)


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











def get_CSI(ai, first_year, for_year, fut_year, n_for_rest, v):
    if ai:
        model = 'backcasting_model_ai'
    if not ai:
        model = 'backcasting_model'
    q, query_dic = read_sql(model, first_year, for_year, fut_year, n_for_rest, v)
    qres = bq_job(q, query_dic, client, v)
    for row in qres:
        data = row[0:9]
    CSI = data[0]
    prec = data[1]
    rec = data[2]
    N_clust = data[3]
    paper_vit_std = data[4]
    growth_stage_std = data[5]
    log_N_top250_std = data[6]
    cit_vit_4thr_std = data[7]
    growth_per_year_since_peak = data[8]
    return prec, rec, CSI, N_clust, paper_vit_std, growth_stage_std, log_N_top250_std, cit_vit_4thr_std, growth_per_year_since_peak

def get_QC(ai, first_year, for_year, fut_year, n_for_rest, v):
    if ai:
        mod_model = 'modularity_ai'
        rev_model = 'review_qc_ai'
    if not ai:
        mod_model = 'modularity'
        rev_model = 'review_qc'
    # get modularity
    q, query_dic = read_sql(mod_model, first_year, for_year, fut_year, n_for_rest, v)
    qres = bq_job(q, query_dic, client, v)
    for row in qres:
        data = row[0:1]
    modularity = data[0]
    # get review_qc
    q, query_dic = read_sql(rev_model, first_year, for_year, fut_year, n_for_rest, v)
    qres = bq_job(q, query_dic, client, v)
    for row in qres:
        data = row[0:1]
    rev_qc = data[0]
    return modularity, rev_qc




def get_forecasts(start_loop, end_loop,  n_for_rest, ai, first_year):
    df = pd.DataFrame(columns=['year', 'Variant', 'Precision', 'Recall', 'CSI', 'N_clust', 'paper_vit_std',
                               'growth_stage_std', 'log_N_top250_std', 'cit_vit_4thr_std', 'growth_per_year_since_peak',
                               'modularity', 'rev_qc'])
    for year in range(start_loop, end_loop + 1):
        fut_year_loop = year + 3
        add_acc_message(f"Run backcasting for {year} - {fut_year_loop}. AI flag is {ai}")
        for v in range(1,10):
            # specify model:
            prec, rec, CSI, N_clust, paper_vit_std, growth_stage_std, log_N_top250_std, cit_vit_4thr_std, \
                growth_per_year_since_peak = get_CSI(ai, first_year, year, fut_year_loop , n_for_rest, v)
            # get QC measures:
            modularity, rev_qc = get_QC(ai, first_year, year, fut_year_loop, n_for_rest, v)
            # prepare data for export
            df = df.append({'year': year, 'Variant': v, 'Precision' : prec, 'Recall': rec, 'CSI': CSI, 'N_clust' : N_clust,
                            'paper_vit_std' : paper_vit_std, 'growth_stage_std': growth_stage_std, 'log_N_top250_std': log_N_top250_std,
                            'cit_vit_4thr_std':cit_vit_4thr_std, 'growth_per_year_since_peak': growth_per_year_since_peak,
                            'modularity' : modularity, 'rev_qc' : rev_qc},
                           ignore_index=True)
    df_name = {True : 'AI', False : 'Full'}
    df.to_csv(f'results_for_{df_name[ai]}.csv')
    # upload results to GCP
    upload_blob('science_clustering', f'results_for_{df_name[ai]}.csv',f'results/results_for_{df_name[ai]}.csv')






# SUmmary function



# def get_forecasts(start_loop, end_loop,  n_for_rest, model, first_year):
#
#     def get_CSI(model, first_year, for_year, fut_year, n_for_rest, v):
#         q, query_dic = read_sql(model, first_year, for_year, fut_year, n_for_rest, v)
#         qres = bq_job(q, query_dic, client, v)
#         for row in qres:
#             data = row[0:4]
#         pres = data[1]
#         rec = data[2]
#         CSI = data[0]
#         return pres, rec, CSI
#
#
#     for year in range(start_loop, end_loop + 1):
#         fut_year_loop = year + 3
#         df = pd.DataFrame(columns=['Year', 'Variant', 'Precision', 'Recall', 'CSI'])
#         for v in range(1,10):
#             prec, rec, CSI = get_CSI(model, first_year, year, fut_year_loop , n_for_rest, v)
#             df = df.append({'year': year, 'Variant': v, 'Precision' : prec, 'Recall' : rec, 'CSI': CSI },
#                            ignore_index=True)
#         for c in ['Precision', 'Recall', 'CSI']:
#             c_mean = df.groupby('year')[c].mean()
#             c_min = df.groupby('year')[c].min()
#             c_max = df.groupby('year')[c].max()
#             stat_df = c_mean.append(c_min).append(c_max)
#             if c == 'Precision':
#                 result_tab = stat_df.to_frame()
#                 result_tab.insert(0, "Stat", ['mean', 'min', 'max'], True)
#             else:
#                 result_tab = pd.concat([result_tab,stat_df], axis=1, sort=False)
#         del df
#         if year == start_loop:
#             for_results = result_tab
#         if year > start_loop:
#             for_results = for_results.append(result_tab)
#         for_results.to_csv(f'results_for_{model}.csv')





# import pandas as pd
# recall = pd.read_csv('Recall_stat.csv')
# pres = pd.read_csv('Precision_stat.csv')
# CSI = pd.read_csv('CSI_stat.csv')
#
#
#
# x = pd.concat([recall, pres[['Precision']]], axis=1, sort=False)
# x
# x.insert(1, "Stat", ['mean', 'max', 'min'], True)
# x
# x= x[['year', 'Recall']]


        # Test is 20 in the sql querry if it is raise an error








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


#with open(f'sql/backcasting/forecast_250J.sql', 'r') as file:
#    # read querry
#    qtext = file.read()
#    for_year = 2015
#    qtext = effify(qtext).replace('\n', ' ')
#    print(qtext)
