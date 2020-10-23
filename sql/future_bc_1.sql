/* Future data */
WITH cl as (
select cluster_id, article_id from september_20_clustering_experiments.assignment_bc_{for_year}_var{v}_latest
),
paper_cur as (
select merged_id as id, year from gcp_cset_links_v2.corpus_merged where year <= {fut_year}
),
/* get papers with age */
/* future proof references */
rel_ref as (
select merged_id, ref_id from
(select merged_id, ref_id from (select id as merged_id, ref_id from gcp_cset_links_v2.mapped_references) r
inner join paper_cur ON r.merged_id = paper_cur.id)
r inner join paper_cur ON r.ref_id = paper_cur.id
),
links as (
/* out links */
select  cluster_id, link_id from
(select merged_id, ref_id as link_id from rel_ref) r inner join cl ON
 r.merged_id = cl.article_id
UNION ALL
/* in links */
select  cluster_id, link_id from
(select ref_id,  merged_id as link_id from rel_ref) r inner join cl ON r.ref_id = cl.article_id
),
/*Get  unclusters papers */
unclust as (
select id from (select id from paper_cur where year = {for_year} + 1) all_art left join cl  ON
 cl.article_id = all_art.id
where cluster_id is null
),
/* leave links only unclustered papers */
links_unclust as (
select cluster_id, link_id, count(*) as sum_weight from  (select cluster_id, link_id from links  inner join unclust on
links.link_id = unclust.id) group by cluster_id, link_id
),
/* rank cluster linked to unclustered papers */
predicted_unclust as (
select distinct cluster_id, link_id as article_id from (
select cluster_id, link_id,  ROW_NUMBER() OVER  (PARTITION BY link_id ORDER BY  sum(sum_weight)  DESC) AS cl_rank from
 links_unclust group by cluster_id, link_id
) where cl_rank  = 1
)
/* add unclustered papers to clustered papers */
select cluster_id, article_id from (
select distinct  cluster_id, article_id from (
select cluster_id, article_id  from cl UNION ALL select cluster_id, article_id  from predicted_unclust
)
) c left join paper_cur ON c.article_id = paper_cur.id



