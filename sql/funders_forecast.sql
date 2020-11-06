/* get all funders*/
WITH funders_tab as (
select unique_funder, merged_id,
IF(unique_funder = 'nsf', 1,0) as nsf,
IF(unique_funder = 'nih', 1,0) as nih,
IF(unique_funder = 'nnsf_china', 1,0) as nnsf_china,
IF(unique_funder = 'erc', 1,0) as erc,
IF(unique_funder = 'jap_sps', 1,0) as jap_sps,
IF(unique_funder = 'ec', 1,0) as ec,
IF(unique_funder = 'eu', 1,0) as eu
from  frontiers_forecasting.full_funder_table_wPaperID
),
/* get authors affiliation. Indicator for at least one authors from one these countries */
chinese as (
select distinct  merged_id  from  (select merged_id, orig_id FROM  frontiers_forecasting.article_links_2020_10_19 ) c inner join
(select  id from  frontiers_forecasting.all_metadata_with_cld2_lid where lower( title_cld2_lid_first_result) = 'chinese' ) l ON
c.orig_id = l.id where merged_id in ( select  merged_id from frontiers_forecasting.corpus_merged where doctype !=  "Patent" or  doctype != "Dataset" or  doctype is  Null)
),
country_tab_0 as (
select distinct merged_id, IF(alpha_3 = 'USA', 1,0) as USA_affiliation,  IF(alpha_3 = 'JPN', 1,0) as Japan_affiliation,
  IF( shared_functions.IsEU(alpha_3), 1,0) as EU_affiliation,   IF(alpha_3 = 'CHN', 1,0) as China_affiliation
from
(
select merged_id, alpha_3 from frontiers_forecasting.paper_affiliations_merged
)
),
country_tab as (
select merged_id, IF(merged_id in (select * from chinese ) and merged_id in
((select merged_id from frontiers_forecasting.article_links_2020_10_19 where orig_id like '%CNKI%') ) ,1,China_affiliation),
EU_affiliation, Japan_affiliation , USA_affiliation from country_tab

)
/* merge clusters and country affiliations */
merge_cl as (
select * except(merged_id) from (
select * except(merged_id) from
/* keep only clusters that are used in the regression more than 20 publications in the forecasting year */
(select cluster_id, article_id from frontiers_forecasting.assignment_{year} where cluster_id in (select cluster_id from frontiers_forecasting.reg_data_{year}) ) a
left join country_tab ON a.article_id = country_tab.merged_id
) a
left join funders_tab ON a.article_id = funders_tab.merged_id
),
/* calculate cluster funding averages */
cluster_average as (
select cluster_id as id, count(*) as NP, avg(IF(nsf is null,0, nsf)) as nsf_share, avg(if(nih is null,0, nih)) as nih_share, avg(if(nnsf_china is null, 0,nnsf_china)) as nnsf_china_share,
avg(if(erc is null,0,erc)) as erc_share, avg(if(jap_sps is null, 0, jap_sps)) as jap_sps_share, avg(if(ec is null, 0, ec)) as ec_share, avg(if(eu is null,0,eu)) as eu_share, avg(USA_affiliation) as USA_affiliation_share, avg(China_affiliation) as China_affiliation_share,
avg(Japan_affiliation) as Japan_affiliation_share, avg(EU_affiliation) as EU_affiliation_share from merge_cl
group by cluster_id
)
/* merge with forecast */
select * except(id) from (select * from frontiers_forecasting.forecast_{year}) f left join (select * from cluster_average) c on f.cluster_id = c.id





