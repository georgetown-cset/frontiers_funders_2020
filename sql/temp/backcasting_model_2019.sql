/* base year 3 years before forecasting year */
create or  replace table frontiers_forecasting.backcasting_data_2019 as
WITH
corp_0 as (
select year, merged_id, age, cluster_id from (
select year, merged_id,  (2019 - year) as age from gcp_cset_links_v2.corpus_merged where year <= 2019 and
year >= 2019
) c inner join (select article_id, cluster_id from
science_map.dc5_cluster_assignment_latest)
cl ON c.merged_id = cl.article_id
),
/* Calculate the number of papers in the cluster in forecasting year.   */
N_for_t as
(
select distinct * except(cluster_id) from (
/* current year pubs*/
select count(distinct merged_id) as N_for, cluster_id as id from corp_0 where year = 2019 group by cluster_id
) cy inner join (
/* all year pubs */
select count(distinct merged_id) as N_total, cluster_id  from corp_0  group by cluster_id
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
select distinct peak_year,  1/( (2020 - peak_year + 1)) as growth_stage, cluster_id as id,  N_peak_share from (
select max(year) as peak_year, cluster_id,  N_peak_share from (
select year, cluster_id, N_share as N_peak_share , ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY  N_share DESC) AS
 cl_year_rank from global_share_cal ) where cl_year_rank = 1 and N_peak_share  > 0
group by cluster_id, N_peak_share
))

select * from peak_year_calc


