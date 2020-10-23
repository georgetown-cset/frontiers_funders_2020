WITH cl as (
/* CHANGE LATER */
select cluster as cluster_id, article_id from frontiers_forecasting.core_2020_latest
),
cl_size as (select cluster_id from (select count(distinct article_id) as N, cluster_id   from cl group by cluster_id) where N < 50)
,
/* CHECK HERE IF WE NEED TO REASSIGN SMALL CLUSTERS */
cl_large as (
select * from cl where cluster_id NOT in (select cluster_id from cl_size)
),
paper_cur as (
select merged_id as id from gcp_cset_links_v2.corpus_merged  /* where year <= 2020*/
),
/* get papers with age */

/* future proof references */
rel_ref as (
select merged_id, ref_id from
(select merged_id, ref_id from (select merged_id, ref_id from gcp_cset_links_v2.paper_references_merged) r
inner join paper_cur ON r.merged_id = paper_cur.id)
r inner join paper_cur ON r.ref_id = paper_cur.id
),
links as (
/* out links */
select  cluster_id, link_id from
(select merged_id, ref_id as link_id from rel_ref) r inner join cl_large ON
 r.merged_id = cl_large.article_id
UNION ALL
/* in links */
select  cluster_id, link_id from
(select ref_id,  merged_id as link_id from rel_ref) r inner join cl_large ON r.ref_id = cl_large.article_id
),
/*Get  unclusters papers */
unclust as (
select merged_id from (select id as merged_id from paper_cur) all_art left join cl_large  ON
 cl_large.article_id = all_art.merged_id
where cluster_id is null
),
/* leave links only unclustered papers */
links_unclust as (
select cluster_id, link_id, count(*) as sum_weight from  (select cluster_id, link_id from links  inner join unclust on
 links.link_id = unclust.merged_id) group by cluster_id, link_id
),
/* rank cluster linked to unclustered papers */
predicted_unclust as (
select distinct cluster_id, link_id as article_id from (
select cluster_id, link_id,  ROW_NUMBER() OVER  (PARTITION BY link_id ORDER BY  sum(sum_weight)  DESC) AS
 cl_rank from links_unclust group by cluster_id, link_id
) where cl_rank  = 1
)
/* add unclustered papers to clustered papers */
select distinct  cluster_id, article_id from (
select cluster_id, article_id  from cl_large UNION ALL select cluster_id, article_id  from predicted_unclust
)