/* base year 3 years before forecasting year */
create or replace table frontiers_forecasting.top250J_2019 as
WITH
cnki_j as (
select merged_id, issn, DisplayName from
(
select cnki_document_id as cnki_id, issn, DisplayName from (
select cnki_doi, document_name, issn, COALESCE( periodical_title_en, periodical_title) as DisplayName from
gcp_cset_cnki.cset_cnki_journals_corpus where CAST(year as INT64) >= 2016 and  CAST(year as INT64) < 2019 and
issn is not null and issn != '' and issn != ' ') a
 inner join (select cnki_document_id, cnki_doi, document_name from gcp_cset_cnki.cset_cnki_journals_id_mappings) m on
  a.cnki_doi = m.cnki_doi and a.document_name = m.document_name
) d inner join (
 select merged_id, orig_id from gcp_cset_links_v2.article_links)
 mids ON d.cnki_id = mids.orig_id
),
ds_j as (
select distinct merged_id, issn, DisplayName from (
select ds_id, issn, DisplayName from
(select JournalId_ds, issn, DisplayName from  staging_merged_article_metadata.Journal_ds) j inner join (select
JournalID, ds_id from staging_merged_article_metadata.Papers_ds where year >= 2016 and year < 2019)  a ON
j.JournalId_ds = a.JournalID
) d inner join (select merged_id, orig_id from gcp_cset_links_v2.article_links) m ON d.ds_id = m.orig_id where issn is
not null and issn != ''
),
wos_j as (
select distinct merged_id, issn, DisplayName  from (
select wos_id, issn, DisplayName from
(select wos_id, issn, DisplayName from  staging_merged_article_metadata.Journal_wos) j inner join (select JournalID,
wos_id  as id from staging_merged_article_metadata.PapersWithAbstracts_wos where CAST(Year as INT64) > 2016 and
 CAST(Year as INT64) < 2019)  a ON j.wos_id = a.id
) d inner join (select merged_id, orig_id from gcp_cset_links_v2.article_links) m ON d.wos_id = m.orig_id where issn is
 not null and issn != ''
),
mag_j as (
select distinct merged_id, issn, DisplayName  from (
select PaperId, issn, DisplayName from
(select Issn, CAST(JournalId as string) as JournalId, DisplayName from  gcp_cset_mag.Journals) j inner join (
select id, PaperId, Year from
(select JournalID as id, CAST(PaperId as string) as PaperId, Year from gcp_cset_mag.PapersWithAbstracts)
 where Year  >= 2016 and Year  < 2019
)  a ON j.JournalId = a.id
) d inner join (select merged_id, orig_id from gcp_cset_links_v2.article_links) m ON d.PaperId = m.orig_id where issn is
 not null and issn != ''
),
all_d as (
select merged_id, issn, DisplayName from ds_j UNION  ALL Select merged_id,  issn, DisplayName from wos_j UNION ALL
Select merged_id, issn, DisplayName  from mag_j  UNION ALL select  merged_id, issn, DisplayName from cnki_j
),
all_m as (
select distinct merged_id, issn from all_d
),
cit_link as (
select distinct count(id) as ncit, ref_id as id, issn from all_m inner join (
select id, ref_id from (
select merged_id as id, ref_id from gcp_cset_links_v2.paper_references_merged
) m inner join (
select merged_id from gcp_cset_links_v2.article_merged_meta where year = 2019
) y  ON y.merged_id = m.id
) c ON all_m.merged_id = c.ref_id group by ref_id, issn
),
/* aggregated */
cit_sum as (
select sum(ncit) as j_Ncit, issn from cit_link group by issn
),
npubs as (
select count(*) as N, issn as jid from cit_link group by issn
),
top250_issn as (
select distinct jid, cit_index, cit_rank from
(select distinct issn as jid, cit_index,  ROW_NUMBER() OVER (ORDER BY cit_index DESC) AS cit_rank from
(
select distinct issn, sum(j_Ncit/N) as cit_index from cit_sum  inner join npubs ON cit_sum.issn = npubs.jid where
 N > 100 group by issn
)) where cit_rank <= 250
)
select merged_id, issn, DisplayName, cit_index, cit_rank from (
select distinct merged_id, issn, cit_index, cit_rank from all_m inner join top250_issn on all_m.issn = top250_issn.jid
) c inner join (select distinct issn as jid, DisplayName from all_d) n ON c.issn = n.jid