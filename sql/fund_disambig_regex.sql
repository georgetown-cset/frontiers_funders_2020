WITH
--NSF Funders
nsf_t AS (SELECT DISTINCT 'nsf' AS unique_funder, fund_org FROM `gcp-cset-projects.science_map.dc5_fund_orgs_stable`
          WHERE (REGEXP_CONTAINS(fund_org, r'\bNational Science Foundation\b') OR REGEXP_CONTAINS(fund_org, r'(\s+|\(|^)NSF')) AND (REGEXP_CONTAINS(fund_org, r'(\s+|\(|^)U.S.') OR REGEXP_CONTAINS(fund_org, r'\bUS\b') OR REGEXP_CONTAINS(fund_org, r'\bUSA\b'))),

--JSPS Funders
jap_sps_t AS (SELECT DISTINCT 'jap_sps' AS unique_funder, fund_org FROM `gcp-cset-projects.science_map.dc5_fund_orgs_stable`
              WHERE REGEXP_CONTAINS(fund_org, r'(\s+|\(|^)JSPS($|\s+|\))') OR REGEXP_CONTAINS(fund_org, r'\bJapan Society for the Promotion of Science\b')),

--EC Funders
ec_t AS (SELECT DISTINCT  'ec' AS unique_funder, fund_org FROM `gcp-cset-projects.science_map.dc5_fund_orgs_stable`
          WHERE REGEXP_CONTAINS(fund_org, r'\bEuropean Commission\b') OR REGEXP_CONTAINS(fund_org, r'\bEuropean Comission\b')),

--ERC Funders
erc_t AS (SELECT DISTINCT  'erc' AS unique_funder, fund_org FROM `gcp-cset-projects.science_map.dc5_fund_orgs_stable`
          WHERE REGEXP_CONTAINS(fund_org, r'\bEuropean Research Council\b') OR REGEXP_CONTAINS(fund_org, r'(\s+|\(|^)ERC($|\s+|\))')),

--NNSFC Funders
nnsf_china_t AS (SELECT DISTINCT 'nnsf_china' AS unique_funder, fund_org FROM `gcp-cset-projects.science_map.dc5_fund_orgs_stable`
                 WHERE REGEXP_CONTAINS(fund_org, r'\bNational Natural Science Foundation of China\b') OR REGEXP_CONTAINS(fund_org, r'(\s+|\(|^)NSFC($|\s+|\))')),

--NIH Funders
nih_t AS (SELECT DISTINCT 'nih' as unique_funder, fund_org FROM `gcp-cset-projects.science_map.dc5_fund_orgs_stable`
          WHERE REGEXP_CONTAINS(fund_org, r'\bNational Institute of Health\b') OR REGEXP_CONTAINS(fund_org, r'(\s+|\(|^)NIH($|\s+|\))')),

--merge all tables together
all_t2 AS (
SELECT * FROM nsf_t UNION ALL SELECT * FROM nih_t UNION ALL SELECT * FROM ec_t UNION ALL SELECT * FROM erc_t UNION ALL  SELECT * FROM nnsf_china_t UNION ALL SELECT * FROM jap_sps_t
)

--return full table
SELECT a.unique_funder, a.fund_org, b.merged_id, b.country, b.grid_id FROM all_t2 a
LEFT JOIN gcp_cset_links_v2.paper_fundorg_merged b ON b.fund_org = a.fund_org
