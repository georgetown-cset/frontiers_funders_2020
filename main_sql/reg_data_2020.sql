WITH
corp_0 as (
select year, merged_id, age, cluster_id from (
select year, merged_id,  (2020 - year) as age from gcp_cset_links_v2.corpus_merged where year <= 2020 and
/* data starts at 2005 */
year >=  2005
) c inner join (select article_id, cluster_id from
science_map.dc5_cluster_assignment_stable) cl ON c.merged_id =
cl.article_id
),
/* Calculate the number of papers in the cluster in forecasting year.   */
corp as (
select cluster_id, merged_id, year from corp_0 where cluster_id in
(select cluster_id from science_map.dc5_stat_clust_proc_stable where year_n >= 20)
),
/* calculate peak year */
pubs_per_year as (
select distinct year, count(distinct merged_id) as N_per_year, cluster_id from corp group by year, cluster_id
),
/* global publications per year */
global_pubs_per_year as (
/* glaobal publications are calculated for all clusters as a based, not just large clusters used in the regression */
select distinct year as y, count(distinct merged_id) as N_G_year from corp group by year
),
global_share_cal as (
/* left join because some clusters are missing pubs in some year, so we need to convert these missing values to zero */
select distinct y as year, 100000*IF(N_per_year is null, 0, N_per_year)/N_G_year as N_share,
IF(N_per_year is null, 0, N_per_year) as N_per_year, N_G_year, cluster_id from global_pubs_per_year left join
 pubs_per_year ON  global_pubs_per_year.y = pubs_per_year.year
),
/* cacluate peak publicaion year for each year */
/* fix 2020 and year laters for main forecasting */
peak_year_calc as (
select distinct peak_year,  1/(2020 - peak_year + 1) as growth_stage, cluster_id as id,  N_peak_share from (
select max(year) as peak_year, cluster_id,  N_peak_share from (
select year, cluster_id, N_share as N_peak_share , ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY  N_share DESC) AS
 cl_year_rank from global_share_cal ) where cl_year_rank = 1 and N_peak_share  > 0
group by cluster_id, N_peak_share
)),
/* aggregated citations by cluster. citation vitality is mean(1/age of citation) */
cit_cl as (
/* there will be some clusters with missing cit_vit that don't have any external citations */
select id, cit_vit_4thr from (
select id, POWER(cit_vit,0.25) as cit_vit_4thr from (
select distinct cluster_id as id, cit_vit from
science_map.dc5_stat_clust_proc_stable where  year_n >= 20)
)),
/* top 250 journals */
clust_top250 as (
select cluster_id as id, log(N) as  log_N_top250  from (
select cluster_id, count(distinct corp.merged_id) as N from
(select merged_id from frontiers_forecasting.Top250J_2019)
 t inner join corp ON t.merged_id = corp.merged_id group
 by cluster_id
)
),
merge_data as (
/* if a cluster did not have any pubs in top250 journals make it zero */
select * except(id, log_N_top250), IF(log_N_top250 is null, 0, log_N_top250) as log_N_top250  from (
select * except(id) from (
select * except(id) from
(select cluster_id, paper_vit,chinese_share, ai_pred, miss_ai_data_share, cl_pred, cv_pred, ro_pred, keyword, subject from science_map.dc5_stat_clust_proc_stable  where  year_n >= 20) m
left join (select id, log_N_top250 from clust_top250) j on m.cluster_id = j.id) m
left join (select id, cit_vit_4thr  from cit_cl) c on m.cluster_id = c.id) m
left join (select id, growth_stage from peak_year_calc) p ON m.cluster_id = p.id
),
/* merge with growth rate*/
/* standardize all variables */
std_calc as (
select * except(paper_vit_std,growth_stage_std,log_N_top250_std,cit_vit_4thr_std), IF(paper_vit_std > 3,3,
IF(paper_vit_std < -3 , -3, paper_vit_std)) as paper_vit_std,
IF(growth_stage_std > 3,3,IF(growth_stage_std < -3 , -3, growth_stage_std)) as growth_stage_std,
IF(log_N_top250_std > 3,3,IF(log_N_top250_std < -3 , -3, log_N_top250_std)) as log_N_top250_std,
IF(cit_vit_4thr_std > 3,3,IF(cit_vit_4thr_std < -3 , -3, cit_vit_4thr_std)) as cit_vit_4thr_std from
(
select * except(paper_vit,growth_stage,log_N_top250,cit_vit_4thr),
(paper_vit - AVG(paper_vit) OVER() ) /  NULLIF(STDDEV_POP(paper_vit) OVER(), 0) as paper_vit_std,
(growth_stage - AVG(growth_stage) OVER() ) /  NULLIF(STDDEV_POP(growth_stage) OVER(), 0) as growth_stage_std,
(log_N_top250 - AVG( log_N_top250) OVER() ) /  NULLIF(STDDEV_POP( log_N_top250) OVER(), 0) as  log_N_top250_std,
(cit_vit_4thr - AVG( cit_vit_4thr) OVER() ) /  NULLIF(STDDEV_POP( cit_vit_4thr) OVER(), 0) as  cit_vit_4thr_std
from merge_data
)
)
/* merge and export */
select * from std_calc