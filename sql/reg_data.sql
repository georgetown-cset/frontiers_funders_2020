/* Create model variables */
/* base year 3 years before forecasting year */
WITH
corp_0 as (
select year, merged_id, age, cluster_id from (
select year, merged_id,  ({year} - year) as age from gcp_cset_links_v2.corpus_merged where year <= {year} and
/* data starts at 2005 */
year >=  2005
) c inner join (select article_id, cluster_id from
frontiers_forecasting.assignment_{year}_latest) cl ON c.merged_id =
cl.article_id
),
/* Calculate the number of papers in the cluster in forecasting year.   */
N_for_t as
(
select * except(cluster_id) from (
/* current year pubs*/
select count(distinct merged_id) as N_for, cluster_id as id from corp_0 where year = {year} group by cluster_id
) cy inner join (
/* all year pubs */
select count(distinct merged_id) as N_total, cluster_id  from corp_0  group by cluster_id
) ay ON cy.id = ay.cluster_id  and N_for >= 20
) ,
/* Keep only papers with more than 19 in the forecasting year in the corpus */
corp as (select * except(id, N_for) , IF(N_for is null, 0, N_for) as N_for from corp_0
inner join N_for_t ON corp_0.cluster_id = N_for_t.id
),
/* Add the number of publications to the forecasting year. If a cluster has no publications in the forecasting year, it is dropped.
We will later set the future publications to zero for these papers */
/* corp */
age_clust as (
/* paper_vitality index */
select cluster_id, 1/(av_age+1) as paper_vit from (
select cluster_id,  avg(age) as av_age from corp group by cluster_id
)),
/* citations per paper and the citation age */
cit_st as (
select distinct id, avg(cit_age) as pap_cit_age_avg, count(cit_id) as pap_cit from
(
/* the age of each citation as of forecasting year */
select id, cit_id, year, {for_year} - year as  cit_age   from
(select ref_id as id, id as cit_id from gcp_cset_links_v2.mapped_references) c inner join corp ON
c.cit_id = corp.merged_id
/* drop all references from papers published after the forecasting year */
) where cit_age >= 0
group by id
),
/* aggregated citations by cluster. citation vitality is mean(1/age of citation) */
cit_cl as (
/* there will be some clusters with missing cit_vit that don't have any external citations */
select id, cit_vit_4thr from (
select id, POWER(cit_vit,0.25) as cit_vit_4thr from (
select distinct cluster_id as id, 1/(avg(pap_cit_age_avg)+1) as cit_vit from
cit_st inner join corp ON cit_st.id = corp.merged_id
group by cluster_id
))),
/* top 250 journals */
clust_top250 as (
select cluster_id as id, log(N) as  log_N_top250  from (
select cluster_id, count(distinct corp.merged_id) as N from
(select merged_id from frontiers_forecasting.Top250J_{year}_latest)
 t inner join corp ON t.merged_id = corp.merged_id group
 by cluster_id
)
),
/* forecasted growth */
/* merge all data for forecasting */
merge_data as (
select * except(id)  from (
select cluster_id, cit_vit_4thr, paper_vit, IF(log_N_top250 is null, 0, log_N_top250) as log_N_top250 from
/* merge vitalities */
(select cluster_id, cit_vit_4thr, paper_vit from age_clust inner join cit_cl ON age_clust.cluster_id = cit_cl.id) m
/* add top 250 */
left join clust_top250 ON m.cluster_id = clust_top250.id) m
/* add peak */
/* brinng peak year calculation and other stuff */
inner join (select * from frontiers_forecasting.backcasting_data_{year}_latest) p
ON m.cluster_id = p.id
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
select * except(id) from (
select * except(id) from
(select * from std_calc) f inner join (select cluster_id as id , IF(chinese_share > 0.5,1,0) as CH,
IF(ai_pred > 0.5, 1,0) as AI from science_map.dc5_clust_stat_stable) c ON f.cluster_id = c.id
 ) c left join N_for_t ON c.cluster_id = N_for_t.id

