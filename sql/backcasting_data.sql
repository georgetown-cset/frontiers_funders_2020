/* base year 3 years before forecasting year */
WITH
corp_0 as (
select year, merged_id, age, cluster_id from (
select year, merged_id,  ({year} - year) as age from gcp_cset_links_v2.corpus_merged where year <= {year} and
year >= {first_year}
) c inner join (select article_id, cluster_id from
 september_20_clustering_experiments.assignment_{year}_latest)
cl ON c.merged_id = cl.article_id
),
/* Calculate the number of papers in the cluster in forecasting year.   */
N_for_t as
(
select distinct * except(cluster_id) from (
/* current year pubs*/
select count(distinct merged_id) as N_for, cluster_id as id from corp_0 where year = {year} group by cluster_id
) cy inner join (
/* all year pubs */
select count(distinct merged_id) as N_total, cluster_id  from corp_0  group by cluster_id
/* we will only predict clusters with at least 20 papers on the forecasting year */
) ay ON cy.id = ay.cluster_id  and N_for >= 20
) ,
/* Keep only papers with more than 19 in the forecasting year in the corpus */
corp as (select * except(id, N_for) , IF(N_for is null, 0, N_for) as N_for from corp_0 inner join N_for_t
ON corp_0.cluster_id = N_for_t.id
),
/* Growth stage */
pubs_per_year as (
select distinct year, count(distinct merged_id) as N_per_year, cluster_id from corp group by year, cluster_id
),
/* global publications per year */
global_pubs_per_year as (
/* glaobal publications are calculated for all clusters as a based, not just large clusters used in the regression */
select distinct year as y, count(distinct merged_id) as N_G_year from corp_0 group by year
),
global_share_cal as (
/* left join because some clusters are missing pubs in some year, so we need to convert these missing values to zero */
select distinct y as year, 100000*IF(N_per_year is null, 0, N_per_year)/N_G_year as N_share,
IF(N_per_year is null, 0, N_per_year) as N_per_year, N_G_year, cluster_id from global_pubs_per_year left join
 pubs_per_year ON  global_pubs_per_year.y = pubs_per_year.year
),
/* cacluate peak publicaion year for each year */
peak_year_calc as (
select distinct peak_year,  1/({year} - peak_year + 1) as growth_stage, cluster_id as id,  N_peak_share from (
select max(year) as peak_year, cluster_id,  N_peak_share from (
select year, cluster_id, N_share as N_peak_share , ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY  N_share DESC) AS
 cl_year_rank from global_share_cal ) where cl_year_rank = 1 and N_peak_share  > 0
group by cluster_id, N_peak_share
)),
/* forecasting year share calc */
for_year_calc as (
select distinct cluster_id, N_share as N_for_share from global_share_cal  where year = {year}
),
/* future date corpus, keep only the number of papers published in the future year */
corp_fut as (
select distinct m.* from (
select distinct * except(merged_id) from (
select cluster_id, article_id from september_20_clustering_experiments.future_bc_{year}_latest
) f inner join (select year, merged_id from gcp_cset_links_v2.corpus_merged where year = {year+3}) m ON
 f.article_id = m.merged_id
/* keep only papers conencted to the clusters in the corpus defined in corp_0 table (all clusters, not just the large ones )  */
) m inner join corp_0 ON m.cluster_id = corp_0.cluster_id
),
/* future date corpus */
dc5_fut as (
/* number of papers in the future cluster */
select distinct article_id, cluster_id, N_fut, 1 as match from (
select count(distinct article_id) as N_fut, cluster_id as id from corp_fut group by cluster_id) c
inner join corp_fut ON c.id = corp_fut.cluster_id),
/* science growth, future publication share */
dc5_fut_share as (
select distinct cluster_id as id, 100000 * (N_fut/ N_global_fut)  as share_fut  from dc5_fut inner join
(select count(distinct article_id) as N_global_fut, 1 as match from dc5_fut) NG ON dc5_fut.match = NG.match
),
/* merge shares */
shares_total_tab as (
select m.id, N_for_share, peak_year, growth_stage, N_peak_share, IF(share_fut is null,0,share_fut) as share_fut from
(select peak_year_calc.id, IF(N_for_share is null,0,N_for_share) as N_for_share , peak_year, growth_stage, N_peak_share
from  peak_year_calc left join for_year_calc ON peak_year_calc.id = for_year_calc.cluster_id)
m left join dc5_fut_share ON m.id = dc5_fut_share.id
),
growth_tab as (
select *, 1+(share_fut - N_peak_share)/N_peak_share as growth_rate_since_peak,
((share_fut - N_peak_share)/N_peak_share)/({year + 3} - peak_year) as growth_per_year_since_peak,
 1+(share_fut - N_for_share)/N_for_share as growth_rate_3_year,  ({year + 3} - peak_year) as
years_since_peak from shares_total_tab
)
select *, IF(POWER(growth_rate_since_peak, 1/years_since_peak) > 1.08,1,0) as x_growth_peak,
IF(POWER(growth_rate_3_year, 1/3) > 1.08,1,0) as x_growth_3yr from  growth_tab





