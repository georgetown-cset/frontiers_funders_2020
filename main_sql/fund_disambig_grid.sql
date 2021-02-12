--GRID match
with
nsf_t as (select distinct 'nsf' as funder, related_grid_id from   gcp_cset_grid.grid_relationships where grid_id = 'grid.431093.c' OR related_grid_id = 'grid.431093.c'),
nih_t as (select distinct 'nih' as funder, related_grid_id from   gcp_cset_grid.grid_relationships where grid_id = 'grid.94365.3d' OR related_grid_id = 'grid.94365.3d'),
eu_t as (select distinct  'eu' as funder,related_grid_id from   gcp_cset_grid.grid_relationships where grid_id = 'grid.453396.e' OR related_grid_id = 'grid.453396.e'),
nnsf_china_t as (select distinct  'nnsf_china' as funder, related_grid_id from   gcp_cset_grid.grid_relationships where grid_id = 'grid.419696.5' OR related_grid_id = 'grid.419696.5'),
jap_sps_t as (select distinct  'jap_sps' as funder, related_grid_id from   gcp_cset_grid.grid_relationships where grid_id = 'grid.54432.34' OR related_grid_id = 'grid.54432.34'),
/* merge tables */
all_t1 as (
select * from nsf_t UNION ALL select * from nih_t UNION ALL select * from eu_t UNION ALL select * from nnsf_china_t UNION ALL select * from jap_sps_t
),

grid_match as(
select * except(id) from all_t1 left join (select id, name from gcp_cset_grid.api_grid) n ON all_t1.related_grid_id = n.id
left join gcp_cset_links_v2.paper_fundorg_merged b ON b.grid_id = all_t1.related_grid_id)


select funder as unique_funder, grid_id, merged_id, fund_org, country from grid_match
