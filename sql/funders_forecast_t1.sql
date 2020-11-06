WITH
perc_tab as ( select * except(percentiles_nnsf,percentiles_nsf,percentiles_nih,percentiles_erc,percentiles_jap_sps, percentiles_ec, percentiles_eu), IF(gr_rank <= 2000, 1,0) as x_growth,
IF(nnsf_china_share >  percentiles_nnsf[offset(50)],1,0) as nnsf_conc,
IF(nsf_share >  percentiles_nsf[offset(50)],1,0) as nsf_conc,
IF(nih_share >  percentiles_nih[offset(50)],1,0) as nih_conc,
IF(erc_share >  percentiles_erc[offset(50)],1,0) as erc_conc,
IF(ec_share >  percentiles_ec[offset(50)],1,0) as ec_conc,
IF(eu_share >  percentiles_eu[offset(50)],1,0) as eu_conc,
IF(jap_sps_share >  percentiles_jap_sps[offset(50)],1,0) as jap_sps_conc,
from 
(
select *  from frontiers_forecasting.funders_forecast_2020 ,
(select APPROX_QUANTILES(nnsf_china_share,100) as percentiles_nnsf from frontiers_forecasting.funders_forecast_2020 where ai_share > 0.5) ,
(select APPROX_QUANTILES(nsf_share,100) as percentiles_nsf from frontiers_forecasting.funders_forecast_2020 where ai_share > 0.5) ,
(select APPROX_QUANTILES(nih_share,100) as percentiles_nih from frontiers_forecasting.funders_forecast_2020 where ai_share > 0.5) ,
(select APPROX_QUANTILES(erc_share,100) as percentiles_erc from frontiers_forecasting.funders_forecast_2020 where ai_share > 0.5) ,
(select APPROX_QUANTILES(ec_share,100) as percentiles_ec from frontiers_forecasting.funders_forecast_2020 where ai_share > 0.5) ,
(select APPROX_QUANTILES(eu_share,100) as percentiles_eu from frontiers_forecasting.funders_forecast_2020 where ai_share > 0.5) ,
(select APPROX_QUANTILES(jap_sps_share,100) as percentiles_jap_sps from frontiers_forecasting.funders_forecast_2020 where ai_share > 0.5) where ai_share > 0.5 
)
),
nnsf_tab as (
select  "China" as country, "NNSF_China" as funder, 
avg(IF(nnsf_conc=1,x_growth,null))  as share_high_g_for_funded_AI,
avg( IF(China_affiliation_share = 0, null, nnsf_china_share / China_affiliation_share )) as share_nat_ai_supported,
avg(IF(x_growth=1,China_affiliation_share,null)) as country_share_high_g,
avg(IF(x_growth=0,China_affiliation_share,null))  as country_share_low_g,
from perc_tab where ai_share > 0.5
),
nsf_tab as (
select  "USA" as country, "NSF" as funder,
avg(IF(nsf_conc=1,x_growth,null))  as share_high_g_for_funded_AI,
avg( IF(USA_affiliation_share  = 0, null, nsf_share / USA_affiliation_share ) )as share_nat_ai_supported,
avg(IF(x_growth=1,USA_affiliation_share,null)) as country_share_high_g,
avg(IF(x_growth=0,USA_affiliation_share,null))  as country_share_low_g,
from perc_tab where ai_share > 0.5
),
nih_tab as (
select  "USA" as country, "NIH" as funder,
avg(IF(nih_conc=1,x_growth,null))  as share_high_g_for_funded_AI,
avg( IF(USA_affiliation_share  = 0, null, nih_share / USA_affiliation_share )) as share_nat_ai_supported,
avg(IF(x_growth=1,USA_affiliation_share,null)) as country_share_high_g,
avg(IF(x_growth=0,USA_affiliation_share,null))  as country_share_low_g,
from perc_tab where ai_share > 0.5
),
erc_tab as (
select  "EU" as country, "ERC" as funder,
avg(IF(erc_conc=1,x_growth,null))  as share_high_g_for_funded_AI,
avg( IF(EU_affiliation_share  = 0, null, erc_share / EU_affiliation_share )) as share_nat_ai_supported,
avg(IF(x_growth=1,EU_affiliation_share,null)) as country_share_high_g,
avg(IF(x_growth=0,EU_affiliation_share,null))  as country_share_low_g,
from perc_tab where ai_share > 0.5
),
ec_tab as (
select  "EU" as country, "EC" as funder,
avg(IF(ec_conc=1,x_growth,null))  as share_high_g_for_funded_AI,
avg( IF(EU_affiliation_share  = 0, null, ec_share / EU_affiliation_share )) as share_nat_ai_supported,
avg(IF(x_growth=1,EU_affiliation_share,null)) as country_share_high_g,
avg(IF(x_growth=0,EU_affiliation_share,null))  as country_share_low_g,
from perc_tab where ai_share > 0.5
),
eu_tab as (
select  "EU" as country, "EU" as funder,
avg(IF(eu_conc=1,x_growth,null))  as share_high_g_for_funded_AI,
avg( IF(EU_affiliation_share  = 0, null, eu_share / EU_affiliation_share )) as share_nat_ai_supported,
avg(IF(x_growth=1,EU_affiliation_share,null)) as country_share_high_g,
avg(IF(x_growth=0,EU_affiliation_share,null))  as country_share_low_g,
from perc_tab where ai_share > 0.5
),
jap_sps_tab as (
select  "Japan" as country, "JAP_SPS" as funder,
avg(IF(jap_sps_conc=1,x_growth,null))  as share_high_g_for_funded_AI,
avg(IF(Japan_affiliation_share  = 0, null, jap_sps_share/ Japan_affiliation_share)) as share_nat_ai_supported,
avg(IF(x_growth=1,Japan_affiliation_share,null)) as country_share_high_g, 
avg(IF(x_growth=0,Japan_affiliation_share,null)) as country_share_low_g, 
from perc_tab where ai_share > 0.5
)
select * from
(
select * from nnsf_tab UNION ALL (select * from nsf_tab) UNION ALL (select * from nih_tab)
UNION ALL (select * from erc_tab) UNION ALL (select * from ec_tab) UNION ALL (select * from eu_tab)   UNION ALL (select * from jap_sps_tab)
) order by funder