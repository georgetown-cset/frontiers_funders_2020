/*
Number of publications and merged */


/* column 1 in row1, 2 */

 select count(*) as N_or, count(distinct merged_id) as N_m from gcp_cset_links_v2.article_links_2020_10_19 where merged_id in ( select  merged_id from gcp_cset_links_v2.corpus_merged where doctype !=  "Patent" or  doctype != "Dataset" or  doctype is not Null
)
/*
row 1
*/
select * from
(select count(*) as N, lang from
(
select  merged_id, lang  from  (select merged_id, orig_id FROM  gcp_cset_links_v2.article_links_2020_10_19 ) c inner join
(select distinct title_cld2_lid_first_result as lang, id from  gcp_cset_links_v2.all_metadata_with_cld2_lid) l ON
c.orig_id = l.id where merged_id in ( select  merged_id from gcp_cset_links_v2.corpus_merged where doctype !=  "Patent" or  doctype != "Dataset" or  doctype is not Null
)
) group by lang) order by N desc



 /* row 2 */
select * from
(select count(*) as N, lang from
(
select distinct merged_id, lang  from  (select merged_id, orig_id FROM  gcp_cset_links_v2.article_links_2020_10_19 ) c inner join
(select distinct title_cld2_lid_first_result as lang, id from  gcp_cset_links_v2.all_metadata_with_cld2_lid) l ON
c.orig_id = l.id where merged_id in ( select  merged_id from gcp_cset_links_v2.corpus_merged where doctype !=  "Patent" or  doctype != "Dataset" or  doctype is not Null
)
) group by lang) order by N desc


/* row 3 core  science */
select * from
(select count(*) as N, lang from
(
select distinct merged_id, lang  from  (select merged_id, orig_id FROM  gcp_cset_links_v2.article_links_2020_10_19 ) c inner join
(select distinct title_cld2_lid_first_result as lang, id from  gcp_cset_links_v2.all_metadata_with_cld2_lid) l ON
c.orig_id = l.id where merged_id in ( select distinct article_id from `202005_p0_clustering`.all_data_20200610_filt_no_ref_from_and_ref_to_or_after_2017_reweighted_clean_clusters_1615e4_lvl2)
) group by lang) order by N desc





/* clustered papers */
select * from
(select count(*) as N, lang from
(
select distinct merged_id, lang  from  (select merged_id, orig_id FROM  gcp_cset_links_v2.article_links_2020_10_19 ) c inner join
(select distinct title_cld2_lid_first_result as lang, id from  gcp_cset_links_v2.all_metadata_with_cld2_lid) l ON
c.orig_id = l.id where merged_id in (select article_id FROM  science_map.dc5_cluster_assignment_latest)
) group by lang) order by N desc


/* language distribution all  */


 select count(*) as N_or, count(distinct merged_id) as N_m from gcp_cset_links_v2.article_links_2020_10_19
  where merged_id in (select article_id FROM  science_map.dc5_cluster_assignment_latest)



/* language distribution distinct */
select * from
(select count(*) as N, lang from
(
select distinct merged_id, lang  from  (select merged_id, orig_id FROM  gcp_cset_links_v2.article_links_2020_10_19 ) c inner join
(select distinct title_cld2_lid_first_result as lang, id from  gcp_cset_links_v2.all_metadata_with_cld2_lid) l ON
c.orig_id = l.id where merged_id in (select article_id FROM  science_map.dc5_cluster_assignment_latest)
) group by lang) order by N desc


/* language distribution all  */

